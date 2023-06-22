#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <condition_variable>
#include <memory>
#include <mutex>
#include <netinet/tcp.h>
#include <ostream>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string.h>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "graph/graph.hpp"
#include "graph/partitioning.h"
#include "partition.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Scheduler {
public:

    Scheduler(int n_requests,
                int repartition_interval,
                int n_partitions,
                int n_checkpointers,
                model::CutMethod repartition_method,
                std::shared_ptr<kvstorage::Storage> storage,
                int sliding_window_time,
                std::chrono::time_point<std::chrono::system_clock>* start_time
    ) : storage_{storage},
        n_partitions_{n_partitions},
        repartition_interval_{repartition_interval},
        repartition_method_{repartition_method},
        n_checkpointers_{n_checkpointers},
        sliding_window_time_{sliding_window_time},
        n_requests_{n_requests}
    {
        running_ = true;
        partition_count_ = 0;
        start_time_ = start_time;
        checkpointers_ = std::vector<checkpoint::Checkpointer<T>*>();

        scheduler_ckp_barrier_ = new pthread_barrier_t();
        pthread_barrier_init(scheduler_ckp_barrier_, NULL, n_checkpointers_+1);

        for (auto i = 0; i < n_checkpointers; i++) {
            auto* ckp = new checkpoint::Checkpointer<T>(storage, i, n_partitions_ / n_checkpointers_, scheduler_ckp_barrier_);
            checkpointers_.push_back(ckp);
        }
        for (auto i = 0; i < n_partitions_; i++) {
            Partition<T>* partition;
            if (n_checkpointers == 0) {
                partition = new Partition<T>(storage, i);
            } else {
                partition = new Partition<T>(storage, i, checkpointers_[i % n_checkpointers_]);
            }
            partitions_.emplace(i, partition);
            partitions_to_checkpoint_.emplace(i);
        }
        crossborder_requests_ = std::vector<int>(n_partitions+1, 0);
        requests_per_thread_ = std::vector<int>(n_partitions, 0);
        for (auto i = 0; i < n_partitions_; i++) {
          for (auto j = 0; j < n_partitions_; j++) {
            conflict_matrix_[i][j] = 0;
          }
        }

        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();
        partition_to_data_ = new std::unordered_map<Partition<T>*, std::vector<int>>();
        for (int i = 0; i < partitions_.size(); i++) {
            std::vector<int> new_vec;
           partition_to_data_->emplace(partitions_.at(i), new_vec);
        }

        if (repartition_method_ != model::ROUND_ROBIN) {
            sem_init(&graph_requests_semaphore_, 0, 0);
            pthread_barrier_init(&repartition_barrier_, NULL, 2);
            graph_thread_ = std::thread(&Scheduler<T>::update_graph_loop, this);
        }
        //graph_queue_size_printer_ = std::thread(&Scheduler<T>::graph_loop_size_printer, this);
        sem_init(&update_req_sem_, 0, 0);
        sem_init(&window_cleaner_sem_, 0, 0);
        executed_requests_thread_ = std::thread(&Scheduler<T>::update_executed_requests, this);
        // slide_window_cleaner_thread_ = std::thread(&Scheduler<T>::slide_window_cleaner, this);
    }

    ~Scheduler() {
        running_ = false;

        std::cout << "Joining req threads" << std::endl;
        std::flush(std::cout);
        if (executed_requests_thread_.joinable()) {
            sem_post(&update_req_sem_);
            executed_requests_thread_.join();
        }
        std::cout << "Joining slide threads" << std::endl;
        if (slide_window_cleaner_thread_.joinable()) {
            sem_post(&window_cleaner_sem_);
            slide_window_cleaner_thread_.join();
        }

        std::cout << "Joining graph thread" << std::endl;
        std::flush(std::cout);
        if (graph_thread_.joinable()) {
            sem_post(&graph_requests_semaphore_);
            graph_thread_.join();
        }

        std::cout << "Closing checkpointers" << std::endl;
        std::flush(std::cout);
        for (auto ckp: checkpointers_) {
          delete ckp;
        }

        std::cout << "Closing partitions" << std::endl;
        std::flush(std::cout);
        for (auto partition: partitions_) {
            delete partition.second;
        }

        std::cout << "Deleting start time" << std::endl;
        std::flush(std::cout);
        delete start_time_;

        std::cout << "Deleting partition mapping" << std::endl;
        std::flush(std::cout);
        delete data_to_partition_;
        delete partition_to_data_;

        pthread_barrier_destroy(scheduler_ckp_barrier_);
        delete scheduler_ckp_barrier_;

        sem_destroy(&update_req_sem_);
        sem_destroy(&window_cleaner_sem_);
        sem_destroy(&graph_requests_semaphore_);
    }

    void process_populate_requests(const int n_initial_keys) {
	    for (auto key = 0; key <= n_initial_keys; key++) {
            if (!mapped(key)) {
                add_key(key);
                storage_->write(key, "");
            }
        }
        if (repartition_method_ != model::ROUND_ROBIN) {
          std::unique_lock lk(mutex_);
          graph_queue_empty_.wait(lk, [this] {
              return graph_requests_queue_.size() == 0;
          });
        }
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second->start_worker_thread();
        }
        sem_post(&update_req_sem_);
        sem_post(&window_cleaner_sem_);
    }

    int n_executed_requests() {
        auto n_executed_requests = 0;
        for (auto& kv: partitions_) {
            auto* partition = kv.second;
            n_executed_requests += partition->n_executed_requests();
        }
        return n_executed_requests;
    }

    void wait_requests_execution() {
        executed_requests_thread_.join();
    }

    std::unordered_map<long, int> get_executed_requests() {
        return executed_requests_;
    }

    const std::vector<std::tuple<time_point, time_point>>& repartition_timestamps() const {
        return repartition_timestamps_;
    }

    std::unordered_map<int, Partition<T>*>& get_partitions() {
        return partitions_;
    }

    std::vector<int>& get_crossborder_requests() {
        return crossborder_requests_;
    }

    std::vector<int>& get_requests_per_thread() {
        return requests_per_thread_;
    }

    int count = 0;

    void schedule_and_answer(struct client_message& request) {
        auto type = static_cast<request_type>(request.type);
        if (type == SYNC) {
            return;
        }

        if (type == WRITE) {
            if (not mapped(request.key)) {
                add_key(request.key);
            }
        }

        auto partitions = std::move(involved_partitions(request));
        crossborder_requests_[partitions.size()]++;
        if (partitions.empty()) {
            std::stringstream ss;
            ss << "No partitions for this request! request: "
                << "key:" << request.key 
                << ", type:" << request.type
                << ", args:" << request.args 
                << std::endl;
            throw std::invalid_argument(ss.str());
        }

        auto arbitrary_partition = *begin(partitions);
        requests_per_thread_[data_to_partition_->at(request.key)->id()]++;

        if (partitions.size() > 1) {
            sync_partitions(partitions);
            arbitrary_partition->push_request(request);
            sync_partitions(partitions);
        } else {
            arbitrary_partition->push_request(request);
        }

        n_dispatched_requests_++;
        bool time_for_checkpoint = n_dispatched_requests_ % repartition_interval_ == 0;
        if (time_for_checkpoint && n_checkpointers_ > 0) {
            sync_all_partitions();
            send_checkpoint_requests();

            // wait ckp start
            pthread_barrier_wait(scheduler_ckp_barrier_);
        }

        if (repartition_method_ != model::ROUND_ROBIN) {
            graph_requests_mutex_.lock();
                graph_requests_queue_.push(request);
            graph_requests_mutex_.unlock();
            sem_post(&graph_requests_semaphore_);

            if (
                n_dispatched_requests_ % repartition_interval_ == 0
                && n_dispatched_requests_ > 0
            ) {
                struct client_message sync_message;
                sync_message.type = SYNC;

                graph_requests_mutex_.lock();
                    graph_requests_queue_.push(sync_message);
                graph_requests_mutex_.unlock();
                sem_post(&graph_requests_semaphore_);

                // wait for the graph queue thread to be full consumed
                pthread_barrier_wait(&repartition_barrier_);

                // repartition
                repartition_data();
            }
        }
        if (time_for_checkpoint && n_checkpointers_ > 0) {
            sync_all_partitions();
        }
    }

    std::vector<std::unordered_set<Partition<T>*>>& get_ckp_turns() {
        return ckp_turns;
    }

    std::vector<checkpoint::Checkpointer<T>*> get_checkpointers() {
        return checkpointers_;
    }

private:

    void send_checkpoint_requests() {
        // std::cout << "SENDING CHECKPOINT" << std::endl;
        for (int i = 0; i < partitions_.size(); i++) {
            Partition<T>* p = partitions_.at(i);
            struct client_message checkpoint_message; 
            checkpoint_message.key = 0;
            checkpoint_message.type = CHECKPOINT;
            checkpoint_message.keys = partition_to_data_->at(p);
            //std::stringstream ss;
            //ss << p << " -> ";
            //for (auto k: checkpoint_message.keys) {
            //    ss << k << ",";
            //}
            //ss << std::endl;
            //std::cout << ss.str();
            p->push_request(checkpoint_message);
        }
    }

    void graph_loop_size_printer() {
        using namespace std::chrono_literals;
        while (true) {
            std::stringstream ss;
            ss << "Graph queue has: " << graph_requests_queue_.size() << std::endl;
            std::cout << ss.str();
            std::this_thread::sleep_for(1s);
        }
    }

    std::unordered_set<Partition<T>*> involved_partitions(
        const struct client_message& request)
    {
        std::unordered_set<Partition<T>*> partitions;
        auto type = static_cast<request_type>(request.type);

        auto range = 1;
        if (type == SCAN) {
            range = std::stoi(request.args);
        }

        for (auto i = 0; i < range; i++) {
            if (not mapped(request.key + i)) {
                return std::unordered_set<Partition<T>*>();
            }

            partitions.insert(data_to_partition_->at(request.key + i));
        }

        return partitions;
    }

    struct client_message create_sync_request(int n_partitions) {
        struct client_message sync_message;
        sync_message.id = sync_counter_;
        sync_message.type = SYNC;

        // this is a gross workaround to send the barrier to the partitions.
        // a more elegant approach would be appreciated.
        auto* barrier = new pthread_barrier_t();
        pthread_barrier_init(barrier, NULL, n_partitions);
        sync_message.s_addr = (unsigned long) barrier;

        return sync_message;
    }

    void sync_partitions(const std::unordered_set<Partition<T>*>& partitions) {
        auto sync_message = std::move(
            create_sync_request(partitions.size())
        );
        for (auto partition : partitions) {
            partition->push_request(sync_message);
        }
    }

    void sync_all_partitions() {
        std::unordered_set<Partition<T>*> partitions;
        for (auto i = 0; i < partitions_.size(); i++) {
            partitions.insert(partitions_.at(i));
        }
        sync_partitions(partitions);
    }

    void add_key(T key) {
        auto partition_id = round_robin_counter_;
        Partition<T>* p = partitions_.at(partition_id);
        data_to_partition_->emplace(key, p);
        partition_to_data_->at(p).push_back(key);

        round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;

        if (repartition_method_ != model::ROUND_ROBIN) {
            struct client_message write_message;
            write_message.type = WRITE;
            write_message.key = key;
            write_message.s_addr = (unsigned long) partitions_.at(partition_id);
            write_message.sin_port = 1;

            graph_requests_mutex_.lock();
                graph_requests_queue_.push(write_message);
            graph_requests_mutex_.unlock();
            sem_post(&graph_requests_semaphore_);
        }
    }

    bool mapped(T key) const {
        return data_to_partition_->find(key) != data_to_partition_->end();
    }

    void update_executed_requests() {
        sem_wait(&update_req_sem_);

        std::flush(std::cout);
        using namespace std::literals;

        int already_counted_throughput = 0;

        while (already_counted_throughput < n_requests_) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          const auto throughput = n_executed_requests() - already_counted_throughput;
          const auto now = std::chrono::system_clock::now();
          const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - *start_time_).count();
          std::cout << "now_seconds: " << now_seconds << ", throughput: " << throughput << std::endl;

          executed_requests_[now_seconds] = throughput;

          already_counted_throughput += throughput;
        }
    }

    void slide_window_cleaner() {
      sem_wait(&window_cleaner_sem_);

      while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto now_second = std::chrono::duration_cast<std::chrono::seconds>(
                  std::chrono::system_clock::now().time_since_epoch())
            .count();
        now_second = std::round(now_second / 10.0);
        now_second = now_second - 1;

        std::cout << "Running slide cleaner for: "
          << std::chrono::duration_cast<std::chrono::seconds>((std::chrono::system_clock::now() - *start_time_)).count() - sliding_window_time_
          << " now_second: " << now_second
          << std::endl;
        for (auto& v: workload_graph_.vertex()) {
          workload_graph_.clear_timed_vertex(v.first, std::vector<long>(now_second));
          //workload_graph_.clear_timed_edge(v.first, std::vector<long>(now_second));
        }
      }
    }

    void update_graph_loop() {
        while(running_) {
            sem_wait(&graph_requests_semaphore_);
            graph_requests_mutex_.lock();
                if (graph_requests_queue_.size() == 0) {
                  graph_requests_mutex_.unlock();
                  continue;
                }
                auto request = std::move(graph_requests_queue_.front());
                graph_requests_queue_.pop();
            graph_requests_mutex_.unlock();

            if (request.type == SYNC) {
                //std::cout << "wait inside graph loop" << std::endl;
                pthread_barrier_wait(&repartition_barrier_);
            } else {
                if (request.type == WRITE and request.sin_port == 1) {
                    auto partition = (Partition<T>*) request.s_addr;
                    //data_to_partition_copy_.emplace(request.key, partition);
                    partition->insert_data(request.key);
                }
                update_graph(request);
            }

            if (graph_requests_queue_.size() == 0)
                graph_queue_empty_.notify_one();
        }
    }

    void update_graph(const client_message& message) {
        std::vector<int> data{message.key};
        if (message.type == SCAN) {
            for (auto i = 1; i < std::stoi(message.args); i++) {
                data.emplace_back(message.key+i);
            }
        }

        for (auto i = 0; i < data.size(); i++) {
            if (not workload_graph_.vertice_exists(data[i])) {
                workload_graph_.add_vertice(data[i]);
            }

            workload_graph_.increase_vertice_weight(data[i]);
            //data_to_partition_copy_.at(data[i])->increase_weight(data[i]);
            for (auto j = i+1; j < data.size(); j++) {
                if (not workload_graph_.vertice_exists(data[j])) {
                    workload_graph_.add_vertice(data[j]);
                }
                if (not workload_graph_.are_connected(data[i], data[j])) {
                    workload_graph_.add_edge(data[i], data[j]);
                }

                workload_graph_.increase_edge_weight(data[i], data[j]);
            }
        }
    }

    void repartition_data() {
        std::cout << "Repartitioning..." << std::endl;
        auto start_timestamp = std::chrono::system_clock::now();

        auto partition_scheme = model::cut_graph(
          workload_graph_,
          partitions_,
          repartition_method_,
          *data_to_partition_,
          first_repartition,
          sliding_window_time_
        );
        delete data_to_partition_;
        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();

        delete partition_to_data_;
        partition_to_data_ = new std::unordered_map<Partition<T>*, std::vector<int>>();
        //std::cout << "Repartitioning..." << std::endl;
        for (int i = 0; i < partitions_.size(); i++) {
            Partition<T>* p = partitions_.at(i);
            std::vector<int> new_vec;
            partition_to_data_->emplace(p, new_vec);
        }
        auto sorted_vertex = workload_graph_.sorted_vertex();
        for (auto i = 0; i < partition_scheme.size(); i++) {
            auto partition = partition_scheme[i];
            if (partition >= n_partitions_) {
                printf("ERROR: partition was %d!\n", partition);
                fflush(stdout);
            }
            auto data = sorted_vertex[i];
            data_to_partition_->emplace(data, partitions_.at(partition));
            partition_to_data_->at(partitions_.at(partition)).push_back(data);
        }

        //data_to_partition_copy_ = *data_to_partition_;
        if (first_repartition) {
            first_repartition = false;
        }
        repartition_timestamps_.emplace_back(std::tuple(start_timestamp, std::chrono::system_clock::now()));
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    long n_dispatched_requests_ = 0;
    std::unordered_map<int, Partition<T>*> partitions_;
    std::unordered_map<T, Partition<T>*>* data_to_partition_;
    std::unordered_map<Partition<T>*, std::vector<int>>* partition_to_data_;

    int conflict_matrix_[32][32];
    std::unordered_set<int> partitions_to_checkpoint_;

    std::thread graph_thread_;
    std::thread graph_queue_size_printer_;
    std::queue<struct client_message> graph_requests_queue_;
    sem_t graph_requests_semaphore_;
    std::mutex graph_requests_mutex_;

    std::vector<std::tuple<time_point, time_point>> repartition_timestamps_;
    model::Graph<T> workload_graph_;
    model::CutMethod repartition_method_;
    long repartition_interval_;
    bool first_repartition = true;
    pthread_barrier_t repartition_barrier_;

    std::condition_variable graph_queue_empty_;

    std::vector<int> crossborder_requests_;
    std::vector<int> requests_per_thread_;
    int partition_count_;
    std::vector<std::unordered_set<Partition<T>*>> ckp_turns;

    int n_checkpointers_;
    std::vector<checkpoint::Checkpointer<T>*> checkpointers_;

    std::shared_ptr<kvstorage::Storage> storage_;
    std::unordered_map<long, int> executed_requests_;
    std::mutex mutex_;
    int sliding_window_time_ = 999999;
    bool running_ = true;
    std::chrono::time_point<std::chrono::system_clock>* start_time_;
    std::thread executed_requests_thread_;
    std::thread slide_window_cleaner_thread_;
    int n_requests_;
    sem_t update_req_sem_;
    sem_t window_cleaner_sem_;
    pthread_barrier_t* scheduler_ckp_barrier_;
};


};


#endif
