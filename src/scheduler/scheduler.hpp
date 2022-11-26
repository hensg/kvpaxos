#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <condition_variable>
#include <memory>
#include <netinet/tcp.h>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string.h>
#include <thread>
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
                std::shared_ptr<kvstorage::Storage> storage
    ) : storage_{storage},
        n_partitions_{n_partitions},
        repartition_interval_{repartition_interval},
        repartition_method_{repartition_method},
        n_checkpointers_{n_checkpointers}
    {
        partition_count_ = 0;
        checkpointers_ = std::vector<checkpoint::Checkpointer<T>*>();
        for (auto i = 0; i < n_checkpointers; i++) {
            auto* ckp = new checkpoint::Checkpointer<T>(storage, i, n_partitions_ / n_checkpointers_);
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
        crossborder_requests_ = std::vector<int>(n_partitions, 0);
        requests_per_thread_ = std::vector<int>(n_partitions, 0);
        for (auto i = 0; i < n_partitions_; i++) {
          for (auto j = 0; j < n_partitions_; j++) {
            conflict_matrix_[i][j] = 0;
          }
        }
      
        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();

        sem_init(&graph_requests_semaphore_, 0, 0);
        pthread_barrier_init(&repartition_barrier_, NULL, 2);
        graph_thread_ = std::thread(&Scheduler<T>::update_graph_loop, this);
    }

    ~Scheduler() {
        for (auto partition: partitions_) {
            delete partition;
        }
        delete data_to_partition_;
    }

    void process_populate_requests(const std::vector<workload::Request>& requests) {
        for (auto& request : requests) {
            add_key(request.key());
            if (request.type() == WRITE) {
                continue;
            }
            storage_->write(request.key(), request.args());
        }
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second->start_worker_thread();
        }
    }

    int n_executed_requests() {
        auto n_executed_requests = 0;
        for (auto& kv: partitions_) {
            auto* partition = kv.second;
            n_executed_requests += partition->n_executed_requests();
        }
        return n_executed_requests;
    }

    const std::vector<time_point>& repartition_timestamps() const {
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
                << "id:" << request.id 
                << ", type:" << request.type
                << ", args:" << request.args 
                << std::endl;
            throw std::invalid_argument(ss.str());
        }

        auto arbitrary_partition = *begin(partitions);
        requests_per_thread_[arbitrary_partition->id()]++;

        if (partitions.size() > 1) {
            sync_partitions(partitions);
            arbitrary_partition->push_request(request);
            sync_partitions(partitions);
        } else {
            arbitrary_partition->push_request(request);
        }

        n_dispatched_requests_++;

        if (
            n_dispatched_requests_ % repartition_interval_ == 0 
            && n_dispatched_requests_ > 0 && n_checkpointers_ > 0
        ) {
            send_checkpoint_requests();
        }

        if (repartition_method_ != model::ROUND_ROBIN) {
            graph_requests_mutex_.lock();
                graph_requests_queue_.push(request);
            graph_requests_mutex_.unlock();
            sem_post(&graph_requests_semaphore_);

            if (
                n_dispatched_requests_ % repartition_interval_ == 0
            ) {
                struct client_message sync_message;
                sync_message.type = SYNC;

                graph_requests_mutex_.lock();
                    graph_requests_queue_.push(sync_message);
                graph_requests_mutex_.unlock();
                sem_post(&graph_requests_semaphore_);

                pthread_barrier_wait(&repartition_barrier_);
                repartition_data();
                sync_all_partitions();
            }
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
        struct client_message checkpoint_message; 
        checkpoint_message.key = 0;
        checkpoint_message.type = CHECKPOINT;
        checkpoint_message.args[0] = '\0';
        for (auto& p: partitions_) {
            p.second->push_request(checkpoint_message);
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
                throw std::invalid_argument("missing key partition, key: " + std::to_string(request.key));
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
        data_to_partition_->emplace(key, partitions_.at(partition_id));

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

    void update_graph_loop() {
        while(true) {
            sem_wait(&graph_requests_semaphore_);
            graph_requests_mutex_.lock();
                auto request = std::move(graph_requests_queue_.front());
                graph_requests_queue_.pop();
            graph_requests_mutex_.unlock();

            if (request.type == SYNC) {
                pthread_barrier_wait(&repartition_barrier_);
            } else {
                if (request.type == WRITE and request.sin_port == 1) {
                    auto partition = (Partition<T>*) request.s_addr;
		            data_to_partition_copy_.emplace(request.key, partition);
		            partition->insert_data(request.key);
                }
                update_graph(request);
            }
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
            data_to_partition_copy_.at(data[i])->increase_weight(data[i]);
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
        auto start_timestamp = std::chrono::system_clock::now();
        repartition_timestamps_.emplace_back(start_timestamp);

        auto partition_scheme = std::move(
            model::cut_graph(
                workload_graph_,
                partitions_,
                repartition_method_,
                *data_to_partition_,
                first_repartition
            )
        );
        delete data_to_partition_;
        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();
        auto sorted_vertex = std::move(workload_graph_.sorted_vertex());
        for (auto i = 0; i < partition_scheme.size(); i++) {
            auto partition = partition_scheme[i];
            if (partition >= n_partitions_) {
                printf("ERROR: partition was %d!\n", partition);
                fflush(stdout);
            }
            auto data = sorted_vertex[i];
            data_to_partition_->emplace(data, partitions_.at(partition));
        }

        data_to_partition_copy_ = *data_to_partition_;
        if (first_repartition) {
            first_repartition = false;
        }
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    long n_dispatched_requests_ = 0;
    std::unordered_map<int, Partition<T>*> partitions_;
    std::unordered_map<T, Partition<T>*>* data_to_partition_;
    std::unordered_map<T, Partition<T>*> data_to_partition_copy_;

    int conflict_matrix_[32][32];
    std::unordered_set<int> partitions_to_checkpoint_;

    std::thread graph_thread_;
    std::queue<struct client_message> graph_requests_queue_;
    sem_t graph_requests_semaphore_;
    std::mutex graph_requests_mutex_;

    std::vector<time_point> repartition_timestamps_;
    model::Graph<T> workload_graph_;
    model::CutMethod repartition_method_;
    long repartition_interval_;
    bool first_repartition = true;
    pthread_barrier_t repartition_barrier_;

    std::vector<int> crossborder_requests_;
    std::vector<int> requests_per_thread_;
    int partition_count_;
    std::vector<std::unordered_set<Partition<T>*>> ckp_turns;

    int n_checkpointers_;
    std::vector<checkpoint::Checkpointer<T>*> checkpointers_;

    std::shared_ptr<kvstorage::Storage> storage_;
    std::mutex mutex_;
};


};


#endif
