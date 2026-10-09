// Lock-free counters + mutex-protected latency histogram; renders Prometheus text format.
#pragma once
#include <atomic>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace sv {

inline const std::vector<double>& latency_bounds_ms() {
  static const std::vector<double> b = {5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000};
  return b;
}

struct Metrics {
  std::atomic<uint64_t> bytes_uploaded{0}, bytes_downloaded{0};
  std::atomic<uint64_t> files_ok{0}, files_unchanged{0}, files_failed{0};
  std::atomic<uint64_t> retries{0};
  std::atomic<uint64_t> err_retryable{0}, err_fatal{0}, err_checksum{0};

  void observe_latency_ms(double ms) {
    std::lock_guard<std::mutex> l(mu_);
    const auto& b = latency_bounds_ms();
    size_t i = 0;
    while (i < b.size() && ms > b[i]) ++i;      // i == b.size() => +Inf bucket
    if (counts_.empty()) counts_.assign(b.size() + 1, 0);
    ++counts_[i]; sum_ms_ += ms; ++n_;
  }
  std::vector<uint64_t> buckets() {
    std::lock_guard<std::mutex> l(mu_);
    auto c = counts_; if (c.empty()) c.assign(latency_bounds_ms().size() + 1, 0);
    return c;
  }
  std::string prometheus(const std::string& job, const std::string& cmd, double duration_s) {
    std::ostringstream o;
    std::string lb = "{job=\"" + job + "\",command=\"" + cmd + "\"}";
    o << "# TYPE storvault_agent_bytes_total counter\n";
    o << "storvault_agent_bytes_total{job=\"" << job << "\",command=\"" << cmd << "\",direction=\"up\"} " << bytes_uploaded << "\n";
    o << "storvault_agent_bytes_total{job=\"" << job << "\",command=\"" << cmd << "\",direction=\"down\"} " << bytes_downloaded << "\n";
    o << "# TYPE storvault_agent_files_total counter\n";
    o << "storvault_agent_files_total{job=\"" << job << "\",command=\"" << cmd << "\",result=\"ok\"} " << files_ok << "\n";
    o << "storvault_agent_files_total{job=\"" << job << "\",command=\"" << cmd << "\",result=\"unchanged\"} " << files_unchanged << "\n";
    o << "storvault_agent_files_total{job=\"" << job << "\",command=\"" << cmd << "\",result=\"failed\"} " << files_failed << "\n";
    o << "# TYPE storvault_agent_retries_total counter\nstorvault_agent_retries_total" << lb << " " << retries << "\n";
    o << "# TYPE storvault_agent_errors_total counter\n";
    o << "storvault_agent_errors_total{job=\"" << job << "\",type=\"retryable\"} " << err_retryable << "\n";
    o << "storvault_agent_errors_total{job=\"" << job << "\",type=\"fatal\"} " << err_fatal << "\n";
    o << "storvault_agent_errors_total{job=\"" << job << "\",type=\"checksum\"} " << err_checksum << "\n";
    o << "# TYPE storvault_agent_run_duration_seconds gauge\nstorvault_agent_run_duration_seconds" << lb << " " << duration_s << "\n";
    auto c = buckets();
    o << "# TYPE storvault_agent_object_latency_ms histogram\n";
    uint64_t cum = 0; const auto& b = latency_bounds_ms();
    for (size_t i = 0; i < b.size(); ++i) {
      cum += c[i];
      o << "storvault_agent_object_latency_ms_bucket{job=\"" << job << "\",le=\"" << b[i] << "\"} " << cum << "\n";
    }
    cum += c[b.size()];
    o << "storvault_agent_object_latency_ms_bucket{job=\"" << job << "\",le=\"+Inf\"} " << cum << "\n";
    { std::lock_guard<std::mutex> l(mu_);
      o << "storvault_agent_object_latency_ms_sum{job=\"" << job << "\"} " << sum_ms_ << "\n";
      o << "storvault_agent_object_latency_ms_count{job=\"" << job << "\"} " << n_ << "\n"; }
    return o.str();
  }
 private:
  std::mutex mu_;
  std::vector<uint64_t> counts_;
  double sum_ms_ = 0; uint64_t n_ = 0;
};

}  // namespace sv
