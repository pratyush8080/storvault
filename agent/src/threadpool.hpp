// Fixed-size thread pool: mutex-protected queue + condition variables.
// Data-race freedom: the queue and the "active" counter are only touched under mu_.
#pragma once
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace sv {

class ThreadPool {
 public:
  explicit ThreadPool(size_t n) {
    for (size_t i = 0; i < n; ++i) workers_.emplace_back([this] { loop(); });
  }
  ~ThreadPool() {
    { std::lock_guard<std::mutex> l(mu_); stop_ = true; }
    cv_.notify_all();
    for (auto& t : workers_) t.join();
  }
  void submit(std::function<void()> fn) {
    { std::lock_guard<std::mutex> l(mu_); q_.push(std::move(fn)); }
    cv_.notify_one();
  }
  // Block until the queue is empty AND no task is running.
  void wait_idle() {
    std::unique_lock<std::mutex> l(mu_);
    idle_cv_.wait(l, [this] { return q_.empty() && active_ == 0; });
  }

 private:
  void loop() {
    for (;;) {
      std::function<void()> fn;
      {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [this] { return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) return;
        fn = std::move(q_.front()); q_.pop();
        ++active_;
      }
      try { fn(); } catch (...) { /* tasks report their own errors */ }
      {
        std::lock_guard<std::mutex> l(mu_);
        --active_;
        if (q_.empty() && active_ == 0) idle_cv_.notify_all();
      }
    }
  }
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> q_;
  std::mutex mu_;
  std::condition_variable cv_, idle_cv_;
  size_t active_ = 0;
  bool stop_ = false;
};

}  // namespace sv
