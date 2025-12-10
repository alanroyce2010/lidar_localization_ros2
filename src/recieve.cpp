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
  }

private:
  struct MapAccumulator {
    uint32_t total_chunks = 0;
    std::vector<bool> received;
    std::vector<pcl::PointCloud<pcl::PointXYZI>::Ptr> clouds;
    std_msgs::msg::Header header;
  };

  void chunkCallback(const lidar_localization_ros2::msg::MapChunk::SharedPtr msg)
  {
    auto map_id = msg->map_id;
    auto chunk_id = msg->chunk_id;
    auto total_chunks = msg->total_chunks;

    auto & acc = maps_[map_id];

    if (acc.total_chunks == 0) {
      acc.total_chunks = total_chunks;
      acc.received.assign(total_chunks, false);
      acc.clouds.resize(total_chunks);
      acc.header = msg->header;
      RCLCPP_INFO(
        get_logger(), "Receiving map_id=%u with %u chunks",
        map_id, total_chunks);
    }

    if (chunk_id >= acc.total_chunks) {
      RCLCPP_WARN(
        get_logger(),
        "Received invalid chunk_id %u (total_chunks=%u) for map_id %u",
        chunk_id, acc.total_chunks, map_id);
      return;
    }

    if (acc.received[chunk_id]) {
      RCLCPP_WARN(
        get_logger(),
        "Duplicate chunk %u for map_id %u, ignoring", chunk_id, map_id);
      return;
    }

    pcl::PointCloud<pcl::PointXYZI>::Ptr pc(new pcl::PointCloud<pcl::PointXYZI>);
    pcl::fromROSMsg(msg->cloud, *pc);
    acc.clouds[chunk_id] = pc;
    acc.received[chunk_id] = true;

    RCLCPP_INFO(
      get_logger(),
      "Received chunk %u/%u for map_id %u (%zu points)",
      chunk_id + 1, acc.total_chunks, map_id, pc->size());

    bool all_received = true;
    for (bool r : acc.received) {
      if (!r) { all_received = false; break; }
    }

    if (all_received) {
      RCLCPP_INFO(get_logger(), "All chunks received for map_id %u, reconstructing...", map_id);
      publishFullMap(map_id);
    }
  }

  void publishFullMap(uint32_t map_id)
  {
    auto it = maps_.find(map_id);
    if (it == maps_.end()) return;

    auto & acc = it->second;

    pcl::PointCloud<pcl::PointXYZI>::Ptr full(new pcl::PointCloud<pcl::PointXYZI>);
    for (auto & chunk : acc.clouds) {
      if (chunk) {
        *full += *chunk;  
      }
    }

    RCLCPP_INFO(
      get_logger(), "Final map has %zu points for map_id %u",
      full->size(), map_id);

    sensor_msgs::msg::PointCloud2 out;
    pcl::toROSMsg(*full, out);
    out.header = acc.header;  

    final_map_pub_->publish(out);
    RCLCPP_INFO(get_logger(), "Published reconstructed initial_map");

    maps_.erase(map_id);
  }

  rclcpp::Subscription<lidar_localization_ros2::msg::MapChunk>::SharedPtr chunk_sub_;
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr final_map_pub_;

  std::unordered_map<uint32_t, MapAccumulator> maps_;
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  auto node = std::make_shared<MapChunkReceiver>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}