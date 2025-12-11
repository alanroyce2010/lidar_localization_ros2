// src/recieve.cpp
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include "lidar_localization_ros2/msg/map_chunk.hpp"
#include "lidar_localization_ros2/srv/request_chunks.hpp"

#include <pcl_conversions/pcl_conversions.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

#include <unordered_map>
#include <vector>
#include <mutex>
#include <chrono>
#include <sstream>
#include <memory>
#include <thread>

using lidar_localization_ros2::msg::MapChunk;
using RequestChunks = lidar_localization_ros2::srv::RequestChunks;

class MapChunkReceiver : public rclcpp::Node
{
public:
MapChunkReceiver()
: Node("map_chunk_receiver"),
  request_timeout_(std::chrono::seconds(2)),
  missing_detection_delay_(rclcpp::Duration::from_seconds(1.5)),
  max_retries_(5),
  retry_backoff_(std::chrono::milliseconds(500))
{
  // publisher for final reconstructed map - make it transient_local so RViz can subscribe late
  final_map_pub_ = this->create_publisher<sensor_msgs::msg::PointCloud2>(
    "full_map",
    rclcpp::QoS(rclcpp::KeepLast(1)).transient_local().reliable());

  // subscription for map chunks
  chunk_sub_ = this->create_subscription<MapChunk>(
    "map_chunks",
    rclcpp::QoS(10).reliable(),
    std::bind(&MapChunkReceiver::chunkCallback, this, std::placeholders::_1));

  // client for requesting retransmits
  request_chunks_client_ = this->create_client<RequestChunks>("request_chunks");

  // periodic timer to sweep maps and request missing chunks (fallback)
  maintenance_timer_ = this->create_wall_timer(
    std::chrono::milliseconds(1000), std::bind(&MapChunkReceiver::maintenanceTimerCb, this));

  RCLCPP_INFO(this->get_logger(), "MapChunkReceiver started");
}

private:
  struct MapAccumulator {
    uint32_t total_chunks = 0;
    std::vector<bool> received;
    std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> clouds;
    std_msgs::msg::Header header;
    rclcpp::Time last_received_time = rclcpp::Time(0ll, RCL_ROS_TIME);
    int retry_count = 0;
  };

  // Callback for incoming chunk messages
  void chunkCallback(const MapChunk::SharedPtr msg)
  {
    const uint32_t map_id = msg->map_id;
    const uint32_t chunk_id_u = msg->chunk_id;
    const uint32_t total_chunks_u = msg->total_chunks;

    RCLCPP_INFO(this->get_logger(), "Received chunk: map_id=%u chunk_id=%u total_chunks=%u",
                map_id, chunk_id_u, total_chunks_u);

    // Validate
    if (total_chunks_u == 0) {
      RCLCPP_WARN(this->get_logger(), "total_chunks==0 for map_id=%u, ignoring", map_id);
      return;
    }
    if (chunk_id_u >= total_chunks_u) {
      RCLCPP_WARN(this->get_logger(), "chunk_id %u >= total_chunks %u for map_id=%u, ignoring",
                  chunk_id_u, total_chunks_u, map_id);
      return;
    }

    pcl::PointCloud<pcl::PointXYZI>::Ptr pc(new pcl::PointCloud<pcl::PointXYZI>);
    try {
      pcl::fromROSMsg(msg->cloud, *pc);
    } catch (const std::exception & e) {
      RCLCPP_ERROR(this->get_logger(), "pcl::fromROSMsg failed for map_id=%u chunk=%u: %s",
                   map_id, chunk_id_u, e.what());
      return;
    }

    {
      std::lock_guard<std::mutex> lock(maps_mutex_);

      auto & acc = maps_[map_id];

      // Initialize accumulator if first time
      if (acc.total_chunks == 0) {
        acc.total_chunks = total_chunks_u;
        acc.received.assign(total_chunks_u, false);
        acc.clouds.resize(total_chunks_u);
        acc.header = msg->header;
        acc.retry_count = 0;
        RCLCPP_INFO(this->get_logger(),
                    "Initialized accumulator for map_id=%u expecting %u chunks",
                    map_id, total_chunks_u);
      } else {
        // sanity check
        if (acc.total_chunks != total_chunks_u) {
          RCLCPP_WARN(this->get_logger(),
                      "Inconsistent total_chunks for map_id=%u: prev=%u now=%u",
                      map_id, acc.total_chunks, total_chunks_u);
        }
      }

      size_t chunk_id = static_cast<size_t>(chunk_id_u);

      // store chunk
      if (!acc.received[chunk_id]) {
        acc.clouds[chunk_id] = pc;
        acc.received[chunk_id] = true;
        acc.last_received_time = this->now();
        RCLCPP_INFO(this->get_logger(),
                    "Stored chunk %zu/%u for map_id=%u (%zu points)",
                    chunk_id + 1, acc.total_chunks, map_id, pc->size());
      } else {
        RCLCPP_WARN(this->get_logger(), "Duplicate chunk %zu for map_id=%u, ignoring", chunk_id, map_id);
        acc.last_received_time = this->now();
      }

      // check completeness
      bool all_received = true;
      for (bool r : acc.received) {
        if (!r) { all_received = false; break; }
      }
      if (all_received) {
        RCLCPP_INFO(this->get_logger(), "All chunks received for map_id=%u — reconstructing", map_id);
        // copy data out and erase while holding lock to avoid races
        auto clouds_copy = acc.clouds; // copy vector<shared_ptr>
        auto header_copy = acc.header;
        maps_.erase(map_id);
        // unlock implicit by leaving scope

        reconstructAndPublish(map_id, clouds_copy, header_copy);
        return;
      }
      // not complete — we'll wait for more chunks or maintenance timer to trigger retransmit
    }
  }

  // Reconstruct full pointcloud and publish
  void reconstructAndPublish(
    uint32_t map_id,
    const std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> &clouds,
    const std_msgs::msg::Header & header)
  {
    pcl::PointCloud<pcl::PointXYZI>::Ptr full(new pcl::PointCloud<pcl::PointXYZI>);
    size_t total_points = 0;
    for (const auto &c : clouds) {
      if (c) {
        *full += *c;
        total_points += c->size();
      }
    }
    RCLCPP_INFO(this->get_logger(), "Reconstructed map_id=%u total_points=%zu", map_id, total_points);

    sensor_msgs::msg::PointCloud2 out;
    pcl::toROSMsg(*full, out);
    out.header = header;
    if (out.header.frame_id.empty()) {
      out.header.frame_id = "map";
    }
    final_map_pub_->publish(out);
    RCLCPP_INFO(this->get_logger(), "Published final map for map_id=%u", map_id);
  }

  // Periodic maintenance: detect stale maps and request missing chunks
  void maintenanceTimerCb()
  {
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> requests; // map_id -> missing list

    {
      std::lock_guard<std::mutex> lock(maps_mutex_);
      auto now = this->now();
      for (auto &kv : maps_) {
        uint32_t map_id = kv.first;
        auto & acc = kv.second;

        // If no chunks arrived yet skip
        if (acc.last_received_time.nanoseconds() == 0) continue;

        // If last_received_time older than detection delay, prepare retransmit request
        if ((now - acc.last_received_time) > missing_detection_delay_) {
          // compute missing indices
          std::vector<uint32_t> missing;
          for (size_t i = 0; i < acc.received.size(); ++i) {
            if (!acc.received[i]) missing.push_back(static_cast<uint32_t>(i));
          }
          if (!missing.empty() && acc.retry_count < max_retries_) {
            acc.retry_count++;
            RCLCPP_INFO(this->get_logger(),
                        "Scheduling retransmit request for map_id=%u (retry %d/%d) missing_count=%zu",
                        map_id, acc.retry_count, max_retries_, missing.size());
            requests.emplace_back(map_id, missing);
            // update last_received_time to avoid duplicated immediate requests
            acc.last_received_time = now;
          } else if (!missing.empty() && acc.retry_count >= max_retries_) {
            RCLCPP_WARN(this->get_logger(),
                        "Max retries reached for map_id=%u; missing %zu chunks. Giving up.",
                        map_id, missing.size());
            // optionally: erase to free memory
            // maps_to_erase.push_back(map_id);
          }
        }
      }
    }

    // Issue requests outside the lock
    for (auto &p : requests) {
      requestMissingChunks(p.first, p.second);
      // small backoff between requests
      std::this_thread::sleep_for(retry_backoff_);
    }
  }

  // Call the request_chunks service to ask for the missing chunk indices
  void requestMissingChunks(uint32_t map_id, const std::vector<uint32_t> & missing_ids)
  {
    if (!request_chunks_client_->wait_for_service(request_timeout_)) {
      RCLCPP_WARN(this->get_logger(), "RequestChunks service not available to request map_id=%u", map_id);
      return;
    }

    auto request = std::make_shared<RequestChunks::Request>();
    request->map_id = map_id;
    request->chunk_ids = missing_ids;

    RCLCPP_INFO(this->get_logger(), "Requesting %zu missing chunks for map_id=%u", missing_ids.size(), map_id);

    // async request with callback
    auto future_result = request_chunks_client_->async_send_request(
      request,
      [this, map_id](rclcpp::Client<RequestChunks>::SharedFuture future) {
        try {
          auto res = future.get();
          if (res->success) {
            RCLCPP_INFO(this->get_logger(), "Retransmit OK for map_id=%u : %s", map_id, res->message.c_str());
          } else {
            RCLCPP_WARN(this->get_logger(), "Retransmit failed for map_id=%u : %s", map_id, res->message.c_str());
          }
        } catch (const std::exception & e) {
          RCLCPP_ERROR(this->get_logger(), "RequestChunks service call exception for map_id=%u: %s", map_id, e.what());
        }
      });
    (void)future_result; // silence unused
  }

  // Members
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr final_map_pub_;
  rclcpp::Subscription<MapChunk>::SharedPtr chunk_sub_;
  rclcpp::Client<RequestChunks>::SharedPtr request_chunks_client_;
  rclcpp::TimerBase::SharedPtr maintenance_timer_;

  std::mutex maps_mutex_;
  std::unordered_map<uint32_t, MapAccumulator> maps_;

  // tuneables
  std::chrono::seconds request_timeout_;
  rclcpp::Duration missing_detection_delay_;
  int max_retries_;
  std::chrono::milliseconds retry_backoff_;
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  auto node = std::make_shared<MapChunkReceiver>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
