#include "util.hpp"
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <thread>

namespace sv {

std::string hex(const unsigned char* d, size_t n) {
  static const char* k = "0123456789abcdef";
  std::string s; s.reserve(n * 2);
  for (size_t i = 0; i < n; ++i) { s += k[d[i] >> 4]; s += k[d[i] & 15]; }
  return s;
}

std::string sha256_hex(const std::string& data) {
  unsigned char md[EVP_MAX_MD_SIZE]; unsigned int len = 0;
  EVP_Digest(data.data(), data.size(), md, &len, EVP_sha256(), nullptr);
  return hex(md, len);
}

std::string sha256_file(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f) return "";
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
  std::vector<char> buf(1 << 20);   // 1 MiB window: memory independent of file size
  while (f) {
    f.read(buf.data(), static_cast<std::streamsize>(buf.size()));
    if (f.gcount() > 0) EVP_DigestUpdate(ctx, buf.data(), static_cast<size_t>(f.gcount()));
  }
  unsigned char md[EVP_MAX_MD_SIZE]; unsigned int len = 0;
  EVP_DigestFinal_ex(ctx, md, &len);
  EVP_MD_CTX_free(ctx);
  return hex(md, len);
}

std::string hmac_sha256(const std::string& key, const std::string& msg) {
  unsigned char out[EVP_MAX_MD_SIZE]; unsigned int len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(msg.data()), msg.size(), out, &len);
  return std::string(reinterpret_cast<char*>(out), len);
}

std::string uri_encode(const std::string& s, bool encode_slash) {
  static const char* k = "0123456789ABCDEF";
  std::string o;
  for (unsigned char c : s) {
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') o += static_cast<char>(c);
    else if (c == '/' && !encode_slash) o += '/';
    else { o += '%'; o += k[c >> 4]; o += k[c & 15]; }
  }
  return o;
}

std::string json_escape(const std::string& s) {
  std::string o;
  for (unsigned char c : s) {
    switch (c) {
      case '"': o += "\\\""; break;
      case '\\': o += "\\\\"; break;
      case '\n': o += "\\n"; break;
      case '\r': o += "\\r"; break;
      case '\t': o += "\\t"; break;
      default:
        if (c < 0x20) { char b[8]; snprintf(b, sizeof b, "\\u%04x", c); o += b; }
        else o += static_cast<char>(c);
    }
  }
  return o;
}

std::string iso8601_now(std::string& date_stamp) {
  std::time_t t = std::time(nullptr);
  std::tm tm{}; gmtime_r(&t, &tm);
  char a[32], b[16];
  strftime(a, sizeof a, "%Y%m%dT%H%M%SZ", &tm);
  strftime(b, sizeof b, "%Y%m%d", &tm);
  date_stamp = b;
  return a;
}

std::string Result::header(const std::string& name) const {
  // headers are stored as "key: value\r\n" lines; case-insensitive search
  std::string lower_h = headers, lower_n = name;
  for (auto& c : lower_h) c = static_cast<char>(tolower(c));
  for (auto& c : lower_n) c = static_cast<char>(tolower(c));
  auto pos = lower_h.find("\n" + lower_n + ":");
  if (pos == std::string::npos) return "";
  pos += lower_n.size() + 2;
  auto end = headers.find('\r', pos);
  std::string v = headers.substr(pos, end == std::string::npos ? std::string::npos : end - pos);
  size_t s = v.find_first_not_of(" \t");
  return s == std::string::npos ? "" : v.substr(s);
}

Result with_retry(const RetryPolicy& p, const std::function<Result()>& fn, int* retries_out) {
  thread_local std::mt19937_64 rng(std::random_device{}());
  Result r;
  for (int attempt = 0; attempt < p.max_attempts; ++attempt) {
    r = fn();
    if (r.ok || !r.retryable()) return r;   // success or fatal -> stop immediately
    if (attempt + 1 < p.max_attempts) {
      if (retries_out) ++*retries_out;
      std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(backoff_ms(p, attempt, rng)));
    }
  }
  return r;
}

}  // namespace sv
