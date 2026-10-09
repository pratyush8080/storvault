// Minimal S3 client (path-style, SigV4) on libcurl + OpenSSL.
// Works against MinIO, Ceph RGW, AWS S3, moto - anything speaking the S3 API.
#pragma once
#include <map>
#include <string>
#include <vector>
#include "util.hpp"

namespace sv {

struct S3Config {
  std::string endpoint;      // e.g. http://localhost:9000
  std::string access_key;
  std::string secret_key;
  std::string region = "us-east-1";
  long connect_timeout_s = 5;
  long low_speed_limit = 1024;   // bytes/s; below this for low_speed_time => abort (stalled link)
  long low_speed_time_s = 20;
};

struct ObjectInfo {
  std::string key;
  uint64_t size = 0;
};

class S3Client {
 public:
  explicit S3Client(S3Config cfg);
  ~S3Client();

  using Meta = std::map<std::string, std::string>;

  Result put_object(const std::string& bucket, const std::string& key, const std::string& data, const Meta& meta = {});
  Result get_object_to_file(const std::string& bucket, const std::string& key, const std::string& dest_path);
  Result get_object(const std::string& bucket, const std::string& key, std::string& out);
  Result head_object(const std::string& bucket, const std::string& key);   // metadata in headers
  Result delete_object(const std::string& bucket, const std::string& key);
  Result create_bucket(const std::string& bucket);

  // multipart
  Result create_multipart(const std::string& bucket, const std::string& key, const Meta& meta, std::string& upload_id);
  Result upload_part(const std::string& bucket, const std::string& key, const std::string& upload_id,
                     int part_number, const std::string& data, std::string& etag);
  Result complete_multipart(const std::string& bucket, const std::string& key, const std::string& upload_id,
                            const std::vector<std::pair<int, std::string>>& parts);
  Result abort_multipart(const std::string& bucket, const std::string& key, const std::string& upload_id);

  // ListObjectsV2 with pagination handled internally
  Result list_objects(const std::string& bucket, const std::string& prefix, std::vector<ObjectInfo>& out);

 private:
  struct Req {
    std::string method, bucket, key;
    std::map<std::string, std::string> query;
    Meta meta;                       // becomes x-amz-meta-*
    const std::string* body = nullptr;
    std::string* out_body = nullptr;
    std::string out_file;            // stream response body to this file if non-empty
  };
  Result perform(const Req& r);
  S3Config cfg_;
  std::string host_, scheme_;
};

// Extract <Tag>value</Tag> (first occurrence) from a small XML document.
std::string xml_get(const std::string& xml, const std::string& tag, size_t from = 0);

}  // namespace sv
