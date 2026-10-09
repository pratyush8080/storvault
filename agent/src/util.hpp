// Utilities: streaming SHA-256, HMAC, URI-encoding, retry with exponential backoff + jitter.
#pragma once
#include <algorithm>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace sv {

std::string hex(const unsigned char* d, size_t n);
std::string sha256_hex(const std::string& data);
std::string sha256_file(const std::string& path);                          // streaming, O(1) memory
std::string hmac_sha256(const std::string& key, const std::string& msg);   // raw bytes
std::string uri_encode(const std::string& s, bool encode_slash);
std::string json_escape(const std::string& s);
std::string iso8601_now(std::string& date_stamp);                          // YYYYMMDDTHHMMSSZ + YYYYMMDD

struct RetryPolicy {
  int max_attempts = 5;
  double base_ms = 200;
  double cap_ms = 10000;
};

// "Full jitter": delay = uniform(0, min(cap, base * 2^attempt)).
// Jitter prevents many clients from retrying in lockstep (thundering herd).
inline double backoff_ms(const RetryPolicy& p, int attempt, std::mt19937_64& rng) {
  double ceiling = std::min(p.cap_ms, p.base_ms * static_cast<double>(1ULL << std::min(attempt, 30)));
  std::uniform_real_distribution<double> d(0.0, ceiling);
  return d(rng);
}

struct Result {
  bool ok = false;
  int http = 0;          // HTTP status; 0 = transport-level error
  std::string body;
  std::string headers;   // raw response headers (lowercased keys searched by header())
  std::string error;
  // Retryable: transport errors, 408, 429, 5xx.  Fatal: 4xx (403 bad creds, 404 missing...).
  bool retryable() const {
    if (ok) return false;
    if (http == 0) return true;
    return http == 408 || http == 429 || http >= 500;
  }
  std::string header(const std::string& name) const;
};

Result with_retry(const RetryPolicy& p, const std::function<Result()>& fn, int* retries_out = nullptr);

}  // namespace sv
