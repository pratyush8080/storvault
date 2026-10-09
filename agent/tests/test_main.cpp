// Dependency-free unit tests (swap for GoogleTest later if you like).
#include <atomic>
#include <cassert>
#include <cstdio>
#include <fstream>
#include "metrics.hpp"
#include "s3client.hpp"
#include "threadpool.hpp"
#include "util.hpp"

static int fails = 0;
#define CHECK(c) do { if (!(c)) { std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); ++fails; } } while (0)

int main() {
  using namespace sv;
  // SHA-256 known-answer tests
  CHECK(sha256_hex("") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  CHECK(sha256_hex("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  { std::ofstream("/tmp/sv_t.bin", std::ios::binary) << "abc"; }
  CHECK(sha256_file("/tmp/sv_t.bin") == sha256_hex("abc"));
  { // multi-MiB file streams correctly across the 1 MiB read window
    std::string big(3 * 1024 * 1024 + 17, 'x'); std::ofstream("/tmp/sv_big.bin", std::ios::binary) << big;
    CHECK(sha256_file("/tmp/sv_big.bin") == sha256_hex(big)); }
  // HMAC known answer (RFC 4231 test case 2)
  std::string h = hmac_sha256("Jefe", "what do ya want for nothing?");
  CHECK(hex(reinterpret_cast<const unsigned char*>(h.data()), h.size()) == "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
  // URI encoding per SigV4
  CHECK(uri_encode("a b/c~d", false) == "a%20b/c~d");
  CHECK(uri_encode("a/b", true) == "a%2Fb");
  // Backoff: bounded by cap, ceiling doubles
  RetryPolicy p; p.base_ms = 100; p.cap_ms = 1000; std::mt19937_64 rng(1);
  for (int i = 0; i < 1000; ++i) { CHECK(backoff_ms(p, 0, rng) <= 100.0); CHECK(backoff_ms(p, 20, rng) <= 1000.0); }
  // Retry classification
  { Result r; r.http = 503; CHECK(r.retryable()); r.http = 403; CHECK(!r.retryable()); r.http = 404; CHECK(!r.retryable()); r.http = 0; CHECK(r.retryable()); }
  // with_retry: retries transient, stops on fatal
  { RetryPolicy q; q.base_ms = 1; q.cap_ms = 2; q.max_attempts = 5; int calls = 0, rt = 0;
    Result r = with_retry(q, [&] { Result x; ++calls; if (calls < 3) { x.http = 500; } else { x.ok = true; x.http = 200; } return x; }, &rt);
    CHECK(r.ok && calls == 3 && rt == 2);
    calls = 0; r = with_retry(q, [&] { Result x; ++calls; x.http = 403; return x; }, &rt);
    CHECK(!r.ok && calls == 1); }
  // Thread pool: no lost tasks under contention
  { std::atomic<int> n{0}; { ThreadPool tp(8); for (int i = 0; i < 10000; ++i) tp.submit([&] { ++n; }); tp.wait_idle(); } CHECK(n == 10000); }
  // XML + header helpers
  CHECK(xml_get("<A><UploadId>xyz</UploadId></A>", "UploadId") == "xyz");
  { Result r; r.headers = "HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nContent-Length: 5\r\n\r\n"; CHECK(r.header("etag") == "\"abc\""); CHECK(r.header("content-length") == "5"); }
  // Histogram buckets
  { Metrics m; m.observe_latency_ms(3); m.observe_latency_ms(70); m.observe_latency_ms(99999); auto b = m.buckets(); CHECK(b[0] == 1); CHECK(b[4] == 1); CHECK(b.back() == 1); }
  std::printf(fails ? "%d FAILURES\n" : "all tests passed\n", fails);
  return fails ? 1 : 0;
}
