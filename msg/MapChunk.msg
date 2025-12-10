# MapChunk.msg
std_msgs/Header header
uint32 map_id         # in case you ever send multiple maps
uint32 chunk_id       # 0,1,...,total_chunks-1
uint32 total_chunks   # total number of chunks
sensor_msgs/PointCloud2 cloud