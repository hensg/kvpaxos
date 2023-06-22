#include "types/types.h"
#include <chrono>
#include <semaphore.h>
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
  std::chrono::time_point<std::chrono::system_clock> start_time;
  std::chrono::time_point<std::chrono::system_clock> end_time;
  long time_taken;
  long size;
  size_t log_size;
  size_t num_keys;
};

template <typename T> class Checkpointer {
public:
  Checkpointer(std::shared_ptr<kvstorage::Storage> storage, int id,
               int n_partitions_for_ckp, pthread_barrier_t* scheduler_barrier)
      : storage_{storage}, id_{id}, running_{true}, notify_count_{0},
        checkpoint_counter_{0}, cv_size_{n_partitions_for_ckp} {

    worker_thread_ = std::thread(&Checkpointer::thread_loop, this);
    scheduler_barrier_ = scheduler_barrier;
  }
  ~Checkpointer() {
      running_ = false;
      if (worker_thread_.joinable()) {
          cv_.notify_all();
          worker_thread_.join();
      }
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

  void time_for_checkpoint(std::vector<int> &partition_used_keys,
                           std::shared_ptr<std::condition_variable> cv) {
    //std::cout << id_ << ": Time for checkpoint" << std::endl;
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
        try {
          // crap code
          cv_.wait(lk, [this] {
            if (!running_) {
              return true;
            }

            bool ready = (notify_count_ >= cv_size_);
            if (ready) {
              notify_count_ = 0;
            }
            return ready;
          });
          if (!running_) {
            return;
          }

          const auto start_time = std::chrono::system_clock::now();
          // notify scheduler that checkpoint has started
          pthread_barrier_wait(scheduler_barrier_);

          make_checkpoint(storage_, keys_for_checkpoint_, start_time);
          keys_for_checkpoint_.clear();
          for (auto &cv : waiting_partitions_) {
            cv->notify_all();
            // std::cout << id_ << ": Waiting partitions" << std::endl;
          }
        } catch (const std::exception &) {
          exit(1);
        }
      }
    }
  }

private:
  void make_checkpoint(std::shared_ptr<kvstorage::Storage> storage,
                       std::unordered_set<int> partition_used_keys,
                       std::chrono::system_clock::time_point start_time) {
    checkpoint_counter_++;
    int ckp_size;
    try {
      ckp_size = serialize_storage_to_file(storage, partition_used_keys);
    } catch (std::exception e) {
      std::cerr << "Failed to write checkpoint! " << e.what() << std::endl;
      exit(1);
    }

    const auto end_time = std::chrono::system_clock::now();
    const auto checkpointing_elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    checkpoint_times_.emplace_back(checkpoint_times{
        id_, checkpoint_counter_, start_time,
        end_time, checkpointing_elapsed_time,
        ckp_size, log_.size(), partition_used_keys.size()});
    clear_request_log();
  }

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

      // Write key
      file.write(reinterpret_cast<char *>(&row.key), sizeof(row.key));
      ckp_size += sizeof(row.key);

      // Write string length
      size_t str_length = row.str_val.size();
      file.write(reinterpret_cast<char *>(&str_length), sizeof(str_length));
      ckp_size += sizeof(str_length);

      // Write string data
      file.write(row.str_val.c_str(), str_length);
      ckp_size += str_length;
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
  std::vector<std::shared_ptr<std::condition_variable>> waiting_partitions_;

  std::thread worker_thread_;

  std::unordered_set<int> keys_for_checkpoint_;
  pthread_barrier_t* scheduler_barrier_;
};

} // namespace checkpoint
