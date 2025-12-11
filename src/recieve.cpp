#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl_conversions/pcl_conversions.h>
#include <pcl/common/io.h>
#include "lidar_localization_ros2/msg/map_chunk.hpp"

class MapChunkReceiver : public rclcpp::Node
{
public:
  MapChunkReceiver()
  : Node("map_chunk_receiver")
  {
    final_map_pub_ = create_publisher<sensor_msgs::msg::PointCloud2>(
      "full_map",
      rclcpp::QoS(rclcpp::KeepLast(1)).transient_local().reliable());

    chunk_sub_ = create_subscription<lidar_localization_ros2::msg::MapChunk>(
      "map_chunks",
      rclcpp::QoS(10).reliable(),
      std::bind(&MapChunkReceiver::chunkCallback, this, std::placeholders::_1));
      global_frame_id_ = "map";
  }

private:
  struct MapAccumulator {
    uint32_t total_chunks = 0;
    std::vector<bool> received;
    std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> clouds;
    std_msgs::msg::Header header;
  };

  // at top of file


// inside your class, add:
  // class member
// existing: std::unordered_map<uint32_t, MapAccumulator> maps_;

// robust callback and publish:
void chunkCallback(const lidar_localization_ros2::msg::MapChunk::SharedPtr msg)
{
  const uint32_t map_id = msg->map_id;
  const uint32_t chunk_id_u = msg->chunk_id;
  const uint32_t total_chunks_u = msg->total_chunks;

  RCLCPP_INFO(get_logger(), "Received raw chunk map_id=%u chunk_id=%u total_chunks=%u",
              map_id, chunk_id_u, total_chunks_u);

  // defensive: ensure total_chunks is > 0
  if (total_chunks_u == 0) {
    RCLCPP_WARN(get_logger(), "Received chunk with total_chunks==0 for map_id=%u, ignoring", map_id);
    return;
  }

  // convert to size_t with bounds checking
  const size_t total_chunks = static_cast<size_t>(total_chunks_u);
  if (chunk_id_u >= total_chunks_u) {
    RCLCPP_WARN(get_logger(), "Chunk id %u >= total_chunks %u for map_id=%u, ignoring",
                chunk_id_u, total_chunks_u, map_id);
    return;
  }
  const size_t chunk_id = static_cast<size_t>(chunk_id_u);

  // Convert cloud safely
  pcl::PointCloud<pcl::PointXYZI>::Ptr pc(new pcl::PointCloud<pcl::PointXYZI>);
  try {
    pcl::fromROSMsg(msg->cloud, *pc);
  } catch (const std::exception & e) {
    RCLCPP_ERROR(get_logger(), "pcl::fromROSMsg failed for map_id=%u chunk_id=%zu: %s",
                 map_id, chunk_id, e.what());
    return;
  }

  {
    std::lock_guard<std::mutex> lock(maps_mutex_);

    auto & acc = maps_[map_id];

    // first-time initialization
    if (acc.total_chunks == 0) {
      acc.total_chunks = static_cast<uint32_t>(total_chunks);
      acc.received.assign(total_chunks, false);
      acc.clouds.resize(total_chunks);
      acc.header = msg->header;
      RCLCPP_INFO(get_logger(), "Initialized accumulator for map_id=%u expecting %zu chunks",
                  map_id, total_chunks);
    } else {
      // sanity: if incoming total_chunks differs, warn
      if (acc.total_chunks != total_chunks_u) {
        RCLCPP_WARN(get_logger(),
          "Inconsistent total_chunks for map_id=%u: prev=%u now=%u",
          map_id, acc.total_chunks, total_chunks_u);
      }
    }

    if (acc.received.size() != total_chunks) {
      RCLCPP_ERROR(get_logger(), "acc.received.size() mismatch: %zu vs %zu. Reinitializing.",
                   acc.received.size(), total_chunks);
      acc.received.assign(total_chunks, false);
      acc.clouds.resize(total_chunks);
      acc.total_chunks = static_cast<uint32_t>(total_chunks);
    }

    if (acc.received[chunk_id]) {
      RCLCPP_WARN(get_logger(), "Duplicate chunk %zu for map_id=%u, ignoring", chunk_id, map_id);
      return;
    }

    // store chunk
    acc.clouds[chunk_id] = pc;
    acc.received[chunk_id] = true;
    RCLCPP_INFO(get_logger(),
      "Stored chunk %zu/%zu for map_id=%u (%zu points)",
      chunk_id + 1, acc.total_chunks, map_id, pc->size());

    // check completeness efficiently: count remaining
    size_t missing_count = 0;
    std::ostringstream missing_list_ss;
    for (size_t i = 0; i < acc.received.size(); ++i) {
      if (!acc.received[i]) {
        ++missing_count;
        // only record a few to not spam logs
        if (missing_list_ss.tellp() < std::streampos(1000)) {
          missing_list_ss << i << ",";
        }
      }
    }

    if (missing_count == 0) {
      RCLCPP_INFO(get_logger(), "All %zu chunks received for map_id=%u — reconstructing.",
                  acc.received.size(), map_id);
      // call publish without holding the lock to avoid deadlocks
    } else {
      RCLCPP_INFO(get_logger(), "map_id=%u: %zu/%zu chunks received; missing_count=%zu; missing_indices=%s",
                  map_id, acc.received.size() - missing_count, acc.received.size(), missing_count,
                  missing_list_ss.str().c_str());
      return;
    }
  } // unlock maps_mutex_ here

  // publishFullMap will re-lock to access maps_
  publishFullMap(map_id);
}

void publishFullMap(uint32_t map_id)
{
  // Move construction out of lock to limit critical section
  std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> clouds_copy;
  std_msgs::msg::Header header_copy;
  {
    std::lock_guard<std::mutex> lock(maps_mutex_);
    auto it = maps_.find(map_id);
    if (it == maps_.end()) {
      RCLCPP_WARN(get_logger(), "publishFullMap: map_id %u not found", map_id);
      return;
    }
    auto & acc = it->second;
    // ensure we really have all chunks
    for (size_t i = 0; i < acc.received.size(); ++i) {
      if (!acc.received[i]) {
        RCLCPP_ERROR(get_logger(), "publishFullMap: chunk %zu missing for map_id %u — aborting", i, map_id);
        return;
      }
    }
    clouds_copy = acc.clouds;  // shallow copy of shared_ptrs
    header_copy = acc.header;
    // erase to free memory
    maps_.erase(it);
  } // unlock

  // Concatenate clouds
  pcl::PointCloud<pcl::PointXYZI>::Ptr full(new pcl::PointCloud<pcl::PointXYZI>);
  size_t total_points = 0;
  for (auto &c : clouds_copy) {
    if (c) {
      *full += *c;
      total_points += c->size();
    }
  }
  RCLCPP_INFO(get_logger(), "publishFullMap: reconstructed map_id=%u total_points=%zu", map_id, total_points);

  // publish
  sensor_msgs::msg::PointCloud2 out;
  pcl::toROSMsg(*full, out);
  out.header = header_copy;
  // ensure header has frame_id (fallback)
  if (out.header.frame_id.empty()) {
    out.header.frame_id = global_frame_id_;
  }
  final_map_pub_->publish(out);
  RCLCPP_INFO(get_logger(), "publishFullMap: published reconstructed map_id=%u", map_id);
}


  rclcpp::Subscription<lidar_localization_ros2::msg::MapChunk>::SharedPtr chunk_sub_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr final_map_pub_;
  std::mutex maps_mutex_;
  std::unordered_map<uint32_t, MapAccumulator> maps_;
  std::string global_frame_id_;
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  auto node = std::make_shared<MapChunkReceiver>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}