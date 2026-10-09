#include "s3client.hpp"
#include <curl/curl.h>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>

namespace sv {

namespace {
std::once_flag g_curl_once;
size_t write_str(char* p, size_t s, size_t n, void* u) { static_cast<std::string*>(u)->append(p, s * n); return s * n; }
size_t write_file(char* p, size_t s, size_t n, void* u) {
  auto* f = static_cast<std::ofstream*>(u);
  f->write(p, static_cast<std::streamsize>(s * n));
  return f->good() ? s * n : 0;
}
size_t hdr_cb(char* p, size_t s, size_t n, void* u) { static_cast<std::string*>(u)->append(p, s * n); return s * n; }
struct ReadCtx { const std::string* d; size_t off; };
size_t read_cb(char* p, size_t s, size_t n, void* u) {
  auto* c = static_cast<ReadCtx*>(u);
  size_t left = c->d->size() - c->off, want = s * n, take = left < want ? left : want;
  memcpy(p, c->d->data() + c->off, take);
  c->off += take;
  return take;
}
}  // namespace

std::string xml_get(const std::string& xml, const std::string& tag, size_t from) {
  auto a = xml.find("<" + tag + ">", from);
  if (a == std::string::npos) return "";
  a += tag.size() + 2;
  auto b = xml.find("</" + tag + ">", a);
  return b == std::string::npos ? "" : xml.substr(a, b - a);
}

S3Client::S3Client(S3Config cfg) : cfg_(std::move(cfg)) {
  std::call_once(g_curl_once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
  auto p = cfg_.endpoint.find("://");
  scheme_ = p == std::string::npos ? "http" : cfg_.endpoint.substr(0, p);
  host_ = p == std::string::npos ? cfg_.endpoint : cfg_.endpoint.substr(p + 3);
  while (!host_.empty() && host_.back() == '/') host_.pop_back();
}
S3Client::~S3Client() = default;

Result S3Client::perform(const Req& r) {
  Result res;
  std::string path = "/" + r.bucket;
  if (!r.key.empty()) path += "/" + r.key;
  std::string canon_uri = uri_encode(path, false);

  std::string canon_q;
  for (auto& [k, v] : r.query) {        // std::map => sorted by key, as SigV4 requires
    if (!canon_q.empty()) canon_q += "&";
    canon_q += uri_encode(k, true) + "=" + uri_encode(v, true);
  }

  static const std::string empty;
  const std::string& body = r.body ? *r.body : empty;
  std::string payload_hash = sha256_hex(body);
  std::string date; std::string amz_date = iso8601_now(date);

  std::map<std::string, std::string> hdrs;   // lowercase name -> value (sorted)
  hdrs["host"] = host_;
  hdrs["x-amz-content-sha256"] = payload_hash;
  hdrs["x-amz-date"] = amz_date;
  for (auto& [k, v] : r.meta) hdrs["x-amz-meta-" + k] = v;

  std::string canon_h, signed_h;
  for (auto& [k, v] : hdrs) { canon_h += k + ":" + v + "\n"; signed_h += (signed_h.empty() ? "" : ";") + k; }
  std::string canon_req = r.method + "\n" + canon_uri + "\n" + canon_q + "\n" + canon_h + "\n" + signed_h + "\n" + payload_hash;
  std::string scope = date + "/" + cfg_.region + "/s3/aws4_request";
  std::string sts = "AWS4-HMAC-SHA256\n" + amz_date + "\n" + scope + "\n" + sha256_hex(canon_req);
  std::string k = hmac_sha256("AWS4" + cfg_.secret_key, date);
  k = hmac_sha256(k, cfg_.region); k = hmac_sha256(k, "s3"); k = hmac_sha256(k, "aws4_request");
  std::string sig_raw = hmac_sha256(k, sts);
  std::string sig = hex(reinterpret_cast<const unsigned char*>(sig_raw.data()), sig_raw.size());
  std::string auth = "AWS4-HMAC-SHA256 Credential=" + cfg_.access_key + "/" + scope +
                     ", SignedHeaders=" + signed_h + ", Signature=" + sig;

  std::string url = scheme_ + "://" + host_ + canon_uri + (canon_q.empty() ? "" : "?" + canon_q);

  CURL* c = curl_easy_init();
  if (!c) { res.error = "curl_easy_init failed"; return res; }
  curl_slist* hl = nullptr;
  for (auto& [n, v] : hdrs) if (n != "host") hl = curl_slist_append(hl, (n + ": " + v).c_str());
  hl = curl_slist_append(hl, ("Authorization: " + auth).c_str());
  hl = curl_slist_append(hl, "Expect:");

  std::string resp_body;
  std::ofstream outf;
  if (!r.out_file.empty()) outf.open(r.out_file, std::ios::binary | std::ios::trunc);   // truncate on every attempt

  curl_easy_setopt(c, CURLOPT_URL, url.c_str());
  curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, r.method.c_str());
  curl_easy_setopt(c, CURLOPT_HTTPHEADER, hl);
  curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, cfg_.connect_timeout_s);
  curl_easy_setopt(c, CURLOPT_LOW_SPEED_LIMIT, cfg_.low_speed_limit);
  curl_easy_setopt(c, CURLOPT_LOW_SPEED_TIME, cfg_.low_speed_time_s);
  curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, hdr_cb);
  curl_easy_setopt(c, CURLOPT_HEADERDATA, &res.headers);
  if (!r.out_file.empty()) {
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_file);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &outf);
  } else {
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_str);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp_body);
  }
  ReadCtx rc{&body, 0};
  if (r.method == "HEAD") curl_easy_setopt(c, CURLOPT_NOBODY, 1L);
  if (r.method == "PUT" || r.method == "POST") {
    curl_easy_setopt(c, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, r.method.c_str());
    curl_easy_setopt(c, CURLOPT_READFUNCTION, read_cb);
    curl_easy_setopt(c, CURLOPT_READDATA, &rc);
    curl_easy_setopt(c, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(body.size()));
  }

  CURLcode cc = curl_easy_perform(c);
  long code = 0;
  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
  curl_slist_free_all(hl);
  curl_easy_cleanup(c);
  if (outf.is_open()) outf.close();

  res.body = resp_body;
  if (cc != CURLE_OK) { res.http = 0; res.error = curl_easy_strerror(cc); return res; }
  res.http = static_cast<int>(code);
  res.ok = code >= 200 && code < 300;
  if (!res.ok) {
    std::string msg = xml_get(res.body, "Code");
    res.error = "HTTP " + std::to_string(code) + (msg.empty() ? "" : " " + msg);
  }
  if (r.out_body) *r.out_body = res.body;
  return res;
}

Result S3Client::put_object(const std::string& b, const std::string& k, const std::string& data, const Meta& meta) {
  Req r; r.method = "PUT"; r.bucket = b; r.key = k; r.body = &data; r.meta = meta;
  return perform(r);
}
Result S3Client::get_object_to_file(const std::string& b, const std::string& k, const std::string& dest) {
  Req r; r.method = "GET"; r.bucket = b; r.key = k; r.out_file = dest;
  return perform(r);
}
Result S3Client::get_object(const std::string& b, const std::string& k, std::string& out) {
  Req r; r.method = "GET"; r.bucket = b; r.key = k; r.out_body = &out;
  return perform(r);
}
Result S3Client::head_object(const std::string& b, const std::string& k) {
  Req r; r.method = "HEAD"; r.bucket = b; r.key = k;
  return perform(r);
}
Result S3Client::delete_object(const std::string& b, const std::string& k) {
  Req r; r.method = "DELETE"; r.bucket = b; r.key = k;
  return perform(r);
}
Result S3Client::create_bucket(const std::string& b) {
  Req r; r.method = "PUT"; r.bucket = b;
  return perform(r);
}

Result S3Client::create_multipart(const std::string& b, const std::string& k, const Meta& meta, std::string& id) {
  Req r; r.method = "POST"; r.bucket = b; r.key = k; r.query["uploads"] = ""; r.meta = meta;
  Result res = perform(r);
  if (res.ok) id = xml_get(res.body, "UploadId");
  if (res.ok && id.empty()) { res.ok = false; res.error = "no UploadId in response"; }
  return res;
}
Result S3Client::upload_part(const std::string& b, const std::string& k, const std::string& id,
                             int n, const std::string& data, std::string& etag) {
  Req r; r.method = "PUT"; r.bucket = b; r.key = k; r.body = &data;
  r.query["partNumber"] = std::to_string(n); r.query["uploadId"] = id;
  Result res = perform(r);
  if (res.ok) {
    etag = res.header("etag");
    if (etag.empty()) { res.ok = false; res.error = "no ETag in part response"; }
  }
  return res;
}
Result S3Client::complete_multipart(const std::string& b, const std::string& k, const std::string& id,
                                    const std::vector<std::pair<int, std::string>>& parts) {
  std::string xml = "<CompleteMultipartUpload>";
  for (auto& [n, e] : parts) xml += "<Part><PartNumber>" + std::to_string(n) + "</PartNumber><ETag>" + e + "</ETag></Part>";
  xml += "</CompleteMultipartUpload>";
  Req r; r.method = "POST"; r.bucket = b; r.key = k; r.body = &xml; r.query["uploadId"] = id;
  Result res = perform(r);
  // S3 may return 200 with an <Error> body for complete-multipart
  if (res.ok && res.body.find("<Error>") != std::string::npos) {
    res.ok = false; res.http = 500; res.error = "CompleteMultipartUpload error: " + xml_get(res.body, "Code");
  }
  return res;
}
Result S3Client::abort_multipart(const std::string& b, const std::string& k, const std::string& id) {
  Req r; r.method = "DELETE"; r.bucket = b; r.key = k; r.query["uploadId"] = id;
  return perform(r);
}

Result S3Client::list_objects(const std::string& b, const std::string& prefix, std::vector<ObjectInfo>& out) {
  std::string token;
  Result last;
  do {
    Req r; r.method = "GET"; r.bucket = b; r.query["list-type"] = "2";
    if (!prefix.empty()) r.query["prefix"] = prefix;
    if (!token.empty()) r.query["continuation-token"] = token;
    last = perform(r);
    if (!last.ok) return last;
    size_t pos = 0;
    while ((pos = last.body.find("<Contents>", pos)) != std::string::npos) {
      auto end = last.body.find("</Contents>", pos);
      std::string blk = last.body.substr(pos, end - pos);
      ObjectInfo oi; oi.key = xml_get(blk, "Key");
      std::string sz = xml_get(blk, "Size"); oi.size = sz.empty() ? 0 : std::stoull(sz);
      out.push_back(oi);
      pos = end;
    }
    token = xml_get(last.body, "IsTruncated") == "true" ? xml_get(last.body, "NextContinuationToken") : "";
  } while (!token.empty());
  return last;
}

}  // namespace sv
