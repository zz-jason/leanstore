#include "benchmarks/ycsb/ycsb.hpp"
#include "leanstore-c/perf_counters.h"
#include "leanstore-c/store_option.h"
#include "leanstore/btree/basic_kv.hpp"
#include "leanstore/btree/transaction_kv.hpp"
#include "leanstore/concurrency/cr_manager.hpp"
#include "leanstore/concurrency/worker_context.hpp"
#include "leanstore/kv_interface.hpp"
#include "leanstore/lean_store.hpp"
#include "leanstore/utils/log.hpp"
#include "leanstore/utils/random_generator.hpp"
#include "leanstore/utils/scrambled_zipf_generator.hpp"
#include "leanstore/utils/user_thread.hpp"
#include "utils/small_vector.hpp"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <memory>
#include <string>

#include <sys/types.h>

namespace leanstore::ycsb {

class YcsbLeanStoreClient : public utils::UserThread {
public:
  static constexpr auto kThreadNamePattern = "ycsb_cli_{}";
  inline static std::atomic<uint64_t> s_client_id_counter = 0;

  static std::unique_ptr<YcsbLeanStoreClient> New(LeanStore* store, KVInterface* btree,
                                                  Workload workload_type) {
    auto* client = new YcsbLeanStoreClient(s_client_id_counter++, store, btree, workload_type);
    return std::unique_ptr<YcsbLeanStoreClient>(client);
  }

  ~YcsbLeanStoreClient() override = default;

protected:
  YcsbLeanStoreClient(uint64_t client_id, LeanStore* store, KVInterface* btree,
                      Workload workload_type)
      : utils::UserThread(nullptr, std::format(kThreadNamePattern, client_id)),
        client_id_(client_id),
        store_(store),
        btree_(btree),
        workload_type_(workload_type) {

    update_desc_ = UpdateDesc::CreateFrom(update_desc_buffer_.Data());
    update_desc_->num_slots_ = 1;
    update_desc_->update_slots_[0].offset_ = 0;
    update_desc_->update_slots_[0].size_ = FLAGS_ycsb_val_size;
  }

  void RunImpl() override {
    auto workload = GetWorkloadSpec(workload_type_);
    while (keep_running_) {
      switch (workload_type_) {
      case Workload::kA:
      case Workload::kB:
      case Workload::kC: {
        auto read_probability = utils::RandomGenerator::Rand(0, 100);
        if (read_probability <= workload.read_proportion_ * 100) {
          SubmitJobSync(NewLookupJob());
        } else {
          SubmitJobSync(NewUpdateJob());
        }
        break;
      }
      default: {
        Log::Fatal("Unknown workload type: {}", static_cast<uint8_t>(workload_type_));
      }
      }
    }
  }

  void SubmitJobSync(std::function<void()>&& job) {
#ifdef ENABLE_COROUTINE
    store_->Submit(std::move(job), client_id_ % store_->store_option_->worker_threads_)->Wait();
#else
    store_->ExecSync(client_id_ % store_->store_option_->worker_threads_, std::move(job));
#endif
  }

  std::function<void()> NewLookupJob() {
    return [this]() {
      SmallBuffer<1024> key_buffer(FLAGS_ycsb_key_size);
      uint8_t* key = key_buffer.Data();
      GenYcsbKey(zipf_random_, key);
      btree_->Lookup(Slice(key, FLAGS_ycsb_key_size), copy_value_);

      GetTlsPerfCounters()->tx_committed_++;
    };
  }

  std::function<void()> NewUpdateJob() {
    return [this]() {
      SmallBuffer<1024> key_buffer(FLAGS_ycsb_key_size);
      uint8_t* key = key_buffer.Data();
      GenYcsbKey(zipf_random_, key);
      btree_->UpdatePartial(Slice(key, FLAGS_ycsb_key_size), update_callback_, *update_desc_);
      GetTlsPerfCounters()->tx_committed_++;
    };
  }

  void GenYcsbKey(utils::ScrambledZipfGenerator& zipf_random, uint8_t* key_buf) {
    GenKey(zipf_random.rand(), key_buf);
  }

  void GenKey(uint64_t key, uint8_t* key_buf) {
    auto key_str = std::to_string(key);
    auto prefix_size =
        FLAGS_ycsb_key_size - key_str.size() > 0 ? FLAGS_ycsb_key_size - key_str.size() : 0;
    std::memset(key_buf, 'k', prefix_size);
    std::memcpy(key_buf + prefix_size, key_str.data(), key_str.size());
  }

  uint64_t client_id_;

  LeanStore* store_;

  KVInterface* btree_;

  Workload workload_type_ = Workload::kA;

  utils::ScrambledZipfGenerator zipf_random_{0, FLAGS_ycsb_record_count, FLAGS_ycsb_zipf_factor};

  std::string val_read_;

  std::function<void(Slice val)> copy_value_ = [&](Slice val) { val.CopyTo(val_read_); };

  SmallBuffer<1024> update_desc_buffer_{UpdateDesc::Size(1)};
  UpdateDesc* update_desc_ = nullptr;
  std::string val_buf_;
  std::function<void(MutableSlice to_update)> update_callback_ = [&](MutableSlice to_update) {
    auto new_val_size = update_desc_->update_slots_[0].size_;
    utils::RandomGenerator::RandAlphString(new_val_size, val_buf_);
    std::memcpy(to_update.Data(), val_buf_.data(), val_buf_.size());
  };
};

} // namespace leanstore::ycsb
