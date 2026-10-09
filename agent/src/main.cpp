// storvault-agent: multithreaded backup / verify / restore agent for S3-compatible storage.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <sys/stat.h>
#include <thread>
#include "metrics.hpp"
#include "s3client.hpp"
#include "threadpool.hpp"

namespace fs = std::filesystem;
using namespace sv;
using Clock = std::chrono::steady_clock;

struct Args {
  std::string cmd, endpoint, bucket, access_key, secret_key, region = "us-east-1";
  std::string src, dest, prefix, manifest_key, base_manifest, metrics_file, job = "manual";
  int threads = 4, part_threads = 4, max_retries = 5;
  uint64_t part_mb = 8, multipart_threshold_mb = 16;
  bool deep = false, create_bucket = false;
};

struct Entry { std::string path, key; uint64_t size = 0; std::string sha, mtime; };

static std::string env_or(const char* k, const std::string& d = "") { const char* v = getenv(k); return v ? v : d; }

static bool parse(int argc, char** argv, Args& a) {
  if (argc < 2) return false;
  a.cmd = argv[1];
  a.endpoint = env_or("STORVAULT_ENDPOINT"); a.access_key = env_or("STORVAULT_ACCESS_KEY");
  a.secret_key = env_or("STORVAULT_SECRET_KEY"); a.bucket = env_or("STORVAULT_BUCKET");
  for (int i = 2; i < argc; ++i) {
    std::string f = argv[i];
    auto val = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (f == "--endpoint") a.endpoint = val();
    else if (f == "--bucket") a.bucket = val();
    else if (f == "--access-key") a.access_key = val();
    else if (f == "--secret-key") a.secret_key = val();
    else if (f == "--region") a.region = val();
    else if (f == "--src") a.src = val();
    else if (f == "--dest") a.dest = val();
    else if (f == "--prefix") a.prefix = val();
    else if (f == "--manifest-key") a.manifest_key = val();
    else if (f == "--base-manifest") a.base_manifest = val();
    else if (f == "--metrics-file") a.metrics_file = val();
    else if (f == "--job") a.job = val();
    else if (f == "--threads") a.threads = std::max(1, atoi(val().c_str()));
    else if (f == "--part-threads") a.part_threads = std::max(1, atoi(val().c_str()));
    else if (f == "--max-retries") a.max_retries = std::max(1, atoi(val().c_str()));
    else if (f == "--part-size-mb") a.part_mb = std::max<uint64_t>(5, strtoull(val().c_str(), nullptr, 10));  // S3 min part = 5 MiB
    else if (f == "--multipart-threshold-mb") a.multipart_threshold_mb = strtoull(val().c_str(), nullptr, 10);
    else if (f == "--deep") a.deep = true;
    else if (f == "--create-bucket") a.create_bucket = true;
    else { std::cerr << "unknown flag: " << f << "\n"; return false; }
  }
  return !a.endpoint.empty() && !a.bucket.empty() && !a.access_key.empty() && !a.secret_key.empty();
}

static void usage() {
  std::cerr <<
    "usage: storvault-agent <backup|verify|restore|list> [options]\n"
    "  common:  --endpoint URL --bucket B --access-key K --secret-key S (or STORVAULT_* env)\n"
    "           --threads N --max-retries N --metrics-file F --job NAME\n"
    "  backup:  --src DIR --prefix snap-ID/ [--base-manifest F] [--part-size-mb 8] [--multipart-threshold-mb 16]\n"
    "           [--part-threads 4] [--create-bucket]\n"
    "  verify:  --manifest-key snap-ID/manifest.tsv [--deep]\n"
    "  restore: --manifest-key snap-ID/manifest.tsv --dest DIR\n"
    "  list:    --prefix P\n";
}

// ---- manifest (TSV, percent-encoded path) ----
static std::string uri_decode(const std::string& s) {
  std::string o;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%' && i + 2 < s.size() + 0 && isxdigit(s[i + 1]) && isxdigit(s[i + 2])) {
      o += static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16)); i += 2;
    } else o += s[i];
  }
  return o;
}
static std::string entry_line(const Entry& e) {
  return uri_encode(e.path, false) + "\t" + e.key + "\t" + std::to_string(e.size) + "\t" + e.sha + "\t" + e.mtime + "\n";
}
static std::vector<Entry> parse_manifest(const std::string& text) {
  std::vector<Entry> v; std::istringstream in(text); std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::vector<std::string> f; std::stringstream ss(line); std::string t;
    while (std::getline(ss, t, '\t')) f.push_back(t);
    if (f.size() < 5) continue;
    Entry e; e.path = uri_decode(f[0]); e.key = f[1]; e.size = std::stoull(f[2]); e.sha = f[3]; e.mtime = f[4];
    v.push_back(e);
  }
  return v;
}

struct Summary {
  std::mutex mu; std::vector<std::pair<std::string, std::string>> errors;
  void err(const std::string& p, const std::string& m) { std::lock_guard<std::mutex> l(mu); if (errors.size() < 200) errors.emplace_back(p, m); }
};

static double ms_since(Clock::time_point t) { return std::chrono::duration<double, std::milli>(Clock::now() - t).count(); }

static void classify(Metrics& m, const Result& r) { if (r.retryable()) ++m.err_retryable; else ++m.err_fatal; }

static std::string read_range(const std::string& path, uint64_t off, uint64_t len) {
  std::ifstream f(path, std::ios::binary);
  std::string buf(len, '\0');
  f.seekg(static_cast<std::streamoff>(off));
  f.read(&buf[0], static_cast<std::streamsize>(len));
  buf.resize(static_cast<size_t>(f.gcount()));
  return buf;
}

static std::string mtime_str(const fs::path& p) {
  struct stat st{}; if (stat(p.c_str(), &st) != 0) return "0";
  return std::to_string(static_cast<long long>(st.st_mtime));
}

// Upload one file. Small -> single PUT. Large -> multipart with parallel parts.
static bool upload_file(S3Client& s3, const Args& a, Metrics& m, const RetryPolicy& rp,
                        const std::string& key, const fs::path& p, uint64_t size,
                        std::string& sha_out, std::string& err) {
  const uint64_t thresh = a.multipart_threshold_mb << 20, part_sz = a.part_mb << 20;
  std::string mt = mtime_str(p);
  int retries = 0;
  if (size < thresh) {
    std::string data = read_range(p.string(), 0, size);            // read once => hash == bytes sent
    sha_out = sha256_hex(data);
    S3Client::Meta meta{{"sha256", sha_out}, {"mtime", mt}, {"size", std::to_string(size)}};
    Result r = with_retry(rp, [&] { return s3.put_object(a.bucket, key, data, meta); }, &retries);
    m.retries += retries;
    if (!r.ok) { classify(m, r); err = r.error; return false; }
    m.bytes_uploaded += size;
    return true;
  }
  sha_out = sha256_file(p.string());                                // streaming hash, O(1) memory
  S3Client::Meta meta{{"sha256", sha_out}, {"mtime", mt}, {"size", std::to_string(size)}};
  std::string upload_id;
  Result r = with_retry(rp, [&] { return s3.create_multipart(a.bucket, key, meta, upload_id); }, &retries);
  if (!r.ok) { m.retries += retries; classify(m, r); err = "create_multipart: " + r.error; return false; }

  uint64_t nparts = (size + part_sz - 1) / part_sz;
  std::vector<std::string> etags(nparts);
  std::atomic<uint64_t> next{0}; std::atomic<bool> failed{false};
  std::mutex emu; std::string first_err; std::atomic<int> part_retries{0};
  auto worker = [&] {
    for (;;) {
      uint64_t i = next.fetch_add(1);
      if (i >= nparts || failed) return;
      uint64_t off = i * part_sz, len = std::min(part_sz, size - off);
      std::string data = read_range(p.string(), off, len);
      int pr = 0; std::string etag;
      Result pr_r = with_retry(rp, [&] { return s3.upload_part(a.bucket, key, upload_id, static_cast<int>(i + 1), data, etag); }, &pr);
      part_retries += pr;
      if (!pr_r.ok) { failed = true; classify(m, pr_r); std::lock_guard<std::mutex> l(emu); if (first_err.empty()) first_err = "part " + std::to_string(i + 1) + ": " + pr_r.error; return; }
      etags[i] = etag; m.bytes_uploaded += len;     // each index written by exactly one thread
    }
  };
  std::vector<std::thread> ts;
  size_t nt = std::min<uint64_t>(static_cast<uint64_t>(a.part_threads), nparts);
  for (size_t i = 0; i < nt; ++i) ts.emplace_back(worker);
  for (auto& t : ts) t.join();
  m.retries += part_retries;
  if (failed) { s3.abort_multipart(a.bucket, key, upload_id); err = first_err; return false; }
  std::vector<std::pair<int, std::string>> parts;
  for (uint64_t i = 0; i < nparts; ++i) parts.emplace_back(static_cast<int>(i + 1), etags[i]);
  retries = 0;
  r = with_retry(rp, [&] { return s3.complete_multipart(a.bucket, key, upload_id, parts); }, &retries);
  m.retries += retries;
  if (!r.ok) { s3.abort_multipart(a.bucket, key, upload_id); classify(m, r); err = "complete_multipart: " + r.error; return false; }
  if (mtime_str(p) != mt || fs::file_size(p) != size) { err = "file changed during backup"; ++m.err_checksum; return false; }
  return true;
}

static void emit_json(const Args& a, Metrics& m, Summary& s, double dur, uint64_t total, const std::string& extra, int rc) {
  auto b = m.buckets();
  std::ostringstream o;
  o << "{\"command\":\"" << a.cmd << "\",\"status\":\"" << (rc == 0 ? "success" : "failed") << "\""
    << ",\"files_total\":" << total << ",\"files_ok\":" << m.files_ok << ",\"files_unchanged\":" << m.files_unchanged
    << ",\"files_failed\":" << m.files_failed << ",\"bytes_uploaded\":" << m.bytes_uploaded
    << ",\"bytes_downloaded\":" << m.bytes_downloaded << ",\"retries\":" << m.retries
    << ",\"errors_retryable\":" << m.err_retryable << ",\"errors_fatal\":" << m.err_fatal << ",\"errors_checksum\":" << m.err_checksum
    << ",\"duration_s\":" << dur << ",\"threads\":" << a.threads;
  double mb = static_cast<double>(m.bytes_uploaded + m.bytes_downloaded) / 1048576.0;
  o << ",\"throughput_mb_s\":" << (dur > 0 ? mb / dur : 0.0);
  if (!extra.empty()) o << "," << extra;
  o << ",\"latency_bounds_ms\":[";
  const auto& lb = latency_bounds_ms();
  for (size_t i = 0; i < lb.size(); ++i) o << (i ? "," : "") << lb[i];
  o << "],\"latency_buckets\":[";
  for (size_t i = 0; i < b.size(); ++i) o << (i ? "," : "") << b[i];
  o << "],\"errors\":[";
  for (size_t i = 0; i < s.errors.size(); ++i)
    o << (i ? "," : "") << "{\"path\":\"" << json_escape(s.errors[i].first) << "\",\"error\":\"" << json_escape(s.errors[i].second) << "\"}";
  o << "]}";
  std::cout << o.str() << std::endl;
  if (!a.metrics_file.empty()) { std::ofstream f(a.metrics_file); f << m.prometheus(a.job, a.cmd, dur); }
}

static int cmd_backup(const Args& a, S3Client& s3, RetryPolicy rp) {
  Metrics m; Summary s; auto t0 = Clock::now();
  if (!fs::is_directory(a.src)) { std::cerr << "--src is not a directory\n"; return 2; }
  if (a.create_bucket) s3.create_bucket(a.bucket);   // ignore "already exists"

  std::map<std::string, Entry> base;
  if (!a.base_manifest.empty()) {
    std::ifstream f(a.base_manifest); std::stringstream ss; ss << f.rdbuf();
    for (auto& e : parse_manifest(ss.str())) base[e.path] = e;
  }
  std::vector<fs::path> files;
  for (auto it = fs::recursive_directory_iterator(a.src, fs::directory_options::skip_permission_denied); it != fs::recursive_directory_iterator(); ++it)
    if (it->is_regular_file() && !it->is_symlink()) files.push_back(it->path());

  std::vector<Entry> entries(files.size()); std::vector<char> okv(files.size(), 0);
  {
    ThreadPool pool(static_cast<size_t>(a.threads));
    for (size_t i = 0; i < files.size(); ++i) {
      pool.submit([&, i] {
        const fs::path& p = files[i];
        std::string rel = fs::relative(p, a.src).generic_string();
        Entry e; e.path = rel; e.key = a.prefix + rel;
        std::error_code ec;
        e.size = fs::file_size(p, ec); e.mtime = mtime_str(p);
        if (ec) { ++m.files_failed; s.err(rel, "stat failed: " + ec.message()); return; }
        auto b = base.find(rel);
        if (b != base.end() && b->second.size == e.size && b->second.mtime == e.mtime) {   // incremental: unchanged
          e.key = b->second.key; e.sha = b->second.sha;
          entries[i] = e; okv[i] = 1; ++m.files_unchanged; return;
        }
        auto t = Clock::now(); std::string err;
        if (upload_file(s3, a, m, rp, e.key, p, e.size, e.sha, err)) {
          m.observe_latency_ms(ms_since(t)); ++m.files_ok; entries[i] = e; okv[i] = 1;
        } else { ++m.files_failed; s.err(rel, err); }
      });
    }
    pool.wait_idle();
  }
  // manifest: only entries that are safely stored
  std::string mf;
  for (size_t i = 0; i < files.size(); ++i) if (okv[i]) mf += entry_line(entries[i]);
  std::string mkey = a.prefix + "manifest.tsv";
  int rt = 0;
  Result r = with_retry(rp, [&] { return s3.put_object(a.bucket, mkey, mf, {{"sha256", sha256_hex(mf)}}); }, &rt);
  m.retries += rt;
  if (!r.ok) s.err("manifest.tsv", r.error);
  int rc = (m.files_failed == 0 && r.ok) ? 0 : 1;
  emit_json(a, m, s, ms_since(t0) / 1000.0, files.size(),
            "\"prefix\":\"" + json_escape(a.prefix) + "\",\"manifest_key\":\"" + json_escape(mkey) + "\"", rc);
  return rc;
}

static bool load_manifest(S3Client& s3, const Args& a, RetryPolicy rp, std::vector<Entry>& out, std::string& err) {
  std::string text;
  Result r = with_retry(rp, [&] { text.clear(); return s3.get_object(a.bucket, a.manifest_key, text); });
  if (!r.ok) { err = "cannot fetch manifest: " + r.error; return false; }
  out = parse_manifest(text); return true;
}

static int cmd_verify(const Args& a, S3Client& s3, RetryPolicy rp) {
  Metrics m; Summary s; auto t0 = Clock::now(); std::string e0; std::vector<Entry> es;
  if (!load_manifest(s3, a, rp, es, e0)) { s.err("manifest", e0); emit_json(a, m, s, 0, 0, "", 1); return 1; }
  {
    ThreadPool pool(static_cast<size_t>(a.threads));
    for (auto& e : es) pool.submit([&, e] {
      auto t = Clock::now(); int rt = 0;
      if (!a.deep) {   // cheap check: HEAD -> size + stored checksum metadata
        Result r = with_retry(rp, [&] { return s3.head_object(a.bucket, e.key); }, &rt);
        m.retries += rt;
        if (!r.ok) { classify(m, r); ++m.files_failed; s.err(e.path, r.error); return; }
        std::string cl = r.header("content-length"), sh = r.header("x-amz-meta-sha256");
        if (cl != std::to_string(e.size)) { ++m.err_checksum; ++m.files_failed; s.err(e.path, "size mismatch: " + cl + " != " + std::to_string(e.size)); return; }
        if (!sh.empty() && sh != e.sha) { ++m.err_checksum; ++m.files_failed; s.err(e.path, "checksum metadata mismatch"); return; }
      } else {         // deep check: download + rehash (catches silent corruption of object bytes)
        std::string tmp = (fs::temp_directory_path() / ("sv-verify-" + sha256_hex(e.key).substr(0, 16))).string();
        Result r = with_retry(rp, [&] { return s3.get_object_to_file(a.bucket, e.key, tmp); }, &rt);
        m.retries += rt;
        if (!r.ok) { classify(m, r); ++m.files_failed; s.err(e.path, r.error); fs::remove(tmp); return; }
        std::string got = sha256_file(tmp); std::error_code ec; uint64_t sz = fs::file_size(tmp, ec); fs::remove(tmp);
        m.bytes_downloaded += sz;
        if (got != e.sha) { ++m.err_checksum; ++m.files_failed; s.err(e.path, "CHECKSUM MISMATCH expected " + e.sha + " got " + got); return; }
      }
      m.observe_latency_ms(ms_since(t)); ++m.files_ok;
    });
    pool.wait_idle();
  }
  int rc = m.files_failed == 0 ? 0 : 1;
  emit_json(a, m, s, ms_since(t0) / 1000.0, es.size(), std::string("\"deep\":") + (a.deep ? "true" : "false"), rc);
  return rc;
}

static int cmd_restore(const Args& a, S3Client& s3, RetryPolicy rp) {
  Metrics m; Summary s; auto t0 = Clock::now(); std::string e0; std::vector<Entry> es;
  if (a.dest.empty()) { std::cerr << "--dest required\n"; return 2; }
  if (!load_manifest(s3, a, rp, es, e0)) { s.err("manifest", e0); emit_json(a, m, s, 0, 0, "", 1); return 1; }
  {
    ThreadPool pool(static_cast<size_t>(a.threads));
    for (auto& e : es) pool.submit([&, e] {
      auto t = Clock::now();
      fs::path rel = fs::path(e.path).lexically_normal();
      if (rel.is_absolute() || rel.string().rfind("..", 0) == 0) { ++m.files_failed; s.err(e.path, "unsafe path in manifest"); return; }  // path traversal guard
      fs::path dst = fs::path(a.dest) / rel, tmp = dst; tmp += ".sv-part";
      std::error_code ec; fs::create_directories(dst.parent_path(), ec);
      int rt = 0;
      Result r = with_retry(rp, [&] { return s3.get_object_to_file(a.bucket, e.key, tmp.string()); }, &rt);
      m.retries += rt;
      if (!r.ok) { classify(m, r); ++m.files_failed; s.err(e.path, r.error); fs::remove(tmp, ec); return; }
      std::string got = sha256_file(tmp.string());
      if (got != e.sha) { ++m.err_checksum; ++m.files_failed; s.err(e.path, "CHECKSUM MISMATCH after download"); fs::remove(tmp, ec); return; }
      fs::rename(tmp, dst, ec);     // atomic publish: never leave a half-written/corrupt file in place
      if (ec) { ++m.files_failed; s.err(e.path, "rename: " + ec.message()); return; }
      m.bytes_downloaded += e.size; m.observe_latency_ms(ms_since(t)); ++m.files_ok;
    });
    pool.wait_idle();
  }
  int rc = m.files_failed == 0 ? 0 : 1;
  emit_json(a, m, s, ms_since(t0) / 1000.0, es.size(), "", rc);
  return rc;
}

static int cmd_list(const Args& a, S3Client& s3, RetryPolicy rp) {
  std::vector<ObjectInfo> v;
  Result r = with_retry(rp, [&] { v.clear(); return s3.list_objects(a.bucket, a.prefix, v); });
  if (!r.ok) { std::cerr << r.error << "\n"; return 1; }
  uint64_t total = 0;
  std::cout << "{\"objects\":[";
  for (size_t i = 0; i < v.size(); ++i) { total += v[i].size; std::cout << (i ? "," : "") << "{\"key\":\"" << json_escape(v[i].key) << "\",\"size\":" << v[i].size << "}"; }
  std::cout << "],\"count\":" << v.size() << ",\"bytes\":" << total << "}" << std::endl;
  return 0;
}

int main(int argc, char** argv) {
  Args a;
  if (!parse(argc, argv, a)) { usage(); return 2; }
  S3Config cfg; cfg.endpoint = a.endpoint; cfg.access_key = a.access_key; cfg.secret_key = a.secret_key; cfg.region = a.region;
  S3Client s3(cfg);
  RetryPolicy rp; rp.max_attempts = a.max_retries;
  if (a.cmd == "backup") return cmd_backup(a, s3, rp);
  if (a.cmd == "verify") return cmd_verify(a, s3, rp);
  if (a.cmd == "restore") return cmd_restore(a, s3, rp);
  if (a.cmd == "list") return cmd_list(a, s3, rp);
  usage(); return 2;
}
