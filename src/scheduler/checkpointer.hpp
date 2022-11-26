#include "types/types.h"
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "storage/storage.h"

namespace checkpoint {

struct checkpoint_times {
  int partition_id;
  int count;
  long start_time;
  long end_time;
  long time_taken;
  long size;
  size_t log_size;
  size_t num_keys;
};

template <typename T> class Checkpointer {
public:
  Checkpointer(std::shared_ptr<kvstorage::Storage> storage, int id, int n_partitions_for_ckp)
      : storage_{storage}, id_{id}, running_{true},
        checkpoint_counter_{0}, cv_size_{n_partitions_for_ckp} {

    worker_thread_ = std::thread(&Checkpointer::thread_loop, this);
  }
  ~Checkpointer() {}

  void make_checkpoint(std::shared_ptr<kvstorage::Storage> storage,
                       std::unordered_set<int> &partition_used_keys) {
    checkpoint_counter_++;
    auto start_time = std::chrono::system_clock::now();
    int ckp_size;
    try {
      ckp_size = serialize_storage_to_file(storage, partition_used_keys);
    } catch (std::exception e) {
      std::cerr << "Failed to write checkpoint! " << e.what() << std::endl;
      exit(1);
    }

    auto end_time = std::chrono::system_clock::now();
    auto checkpointing_elapsed_time = (end_time - start_time).count();

    checkpoint_times_.emplace_back(checkpoint_times{
        id_, checkpoint_counter_, start_time.time_since_epoch().count(),
        end_time.time_since_epoch().count(), checkpointing_elapsed_time,
        ckp_size, log_.size(), partition_used_keys.size()});
    clear_request_log();
  }

  void clear_request_log() {
    std::queue<struct client_message> new_log;
    log_.swap(new_log);
  }

  std::vector<checkpoint_times> &get_checkpoint_times() {
    return checkpoint_times_;
  }

  void save_request_to_log(struct client_message request) {
    log_.push(request);
  }

  void time_for_checkpoint(const std::unordered_set<int> &partition_used_keys,
                           std::condition_variable *cv) {
    std::unique_lock lk(cv_mutex_);
    notify_count_ += 1;
    keys_for_checkpoint_.insert(partition_used_keys.begin(),
                                partition_used_keys.end());
    waiting_partitions_.push_back(cv);
    cv_.notify_all();
  }

  void thread_loop() {
    while (running_) {
      {
        std::unique_lock lk(cv_mutex_);
        cv_.wait(lk, [this] {
          bool ready = (notify_count_ >= cv_size_);
          if (ready)
            notify_count_ = 0;
          return ready;
        });

        make_checkpoint(storage_, keys_for_checkpoint_);
        keys_for_checkpoint_.clear();
        for (auto &cv : waiting_partitions_) {
          cv->notify_all();
        }
      }
    }
  }

private:
  long serialize_storage_to_file(std::shared_ptr<kvstorage::Storage> storage,
                                 std::unordered_set<int> &partition_used_keys) {
    std::string filename = "partition_" + std::to_string(id_) + "_.ckp";
    std::filesystem::remove(filename);
    std::ofstream file(filename, std::ios::out | std::ios::binary);
    if (!file) {
      std::cerr << "Cannot open checkpoint file to write" << std::endl;
      exit(1);
    }
    long ckp_size = 0;
    for (auto &key : partition_used_keys) {
      storage_row row;
      row.key = key;
      row.str_val = storage->read(key);
      file.write(reinterpret_cast<char *>(&row), sizeof(row));
      ckp_size += sizeof(row);
    }
    file.flush();
    file.close();
    return ckp_size;
  }

  // used to serialize to file
  struct storage_row {
    int key;
    std::string str_val;
  };

  std::shared_ptr<kvstorage::Storage> storage_;

  int id_;
  std::vector<checkpoint_times> checkpoint_times_;
  int checkpoint_counter_;
  std::queue<struct client_message> log_;

  bool running_;

  std::condition_variable cv_;
  std::mutex cv_mutex_;
  int notify_count_;
  int cv_size_;
  std::vector<std::condition_variable *> waiting_partitions_;

  std::thread worker_thread_;

  std::unordered_set<int> keys_for_checkpoint_;
};

} // namespace checkpoint
