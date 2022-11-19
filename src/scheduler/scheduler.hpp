#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <condition_variable>
#include <memory>
#include <netinet/tcp.h>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <shared_mutex>
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
                model::CutMethod repartition_method
    ) : n_partitions_{n_partitions},
        repartition_interval_{repartition_interval},
        repartition_method_{repartition_method},
        n_checkpointers_{n_checkpointers}
    {
        partition_count_ = 0;
        for (auto i = 0; i < n_partitions_; i++) {
            auto* partition = new Partition<T>(i);
            partitions_.emplace(i, partition);
            partitions_to_checkpoint_.emplace(i);
            crossborder_requests_.push_back(0);
            requests_per_thread_.push_back(0);
        }
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
        }
        Partition<T>::populate_storage(requests);
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
            request.type = ERROR;
            return partitions_.at(0)->push_request(request);
        }

        auto arbitrary_partition = *begin(partitions);
        if (partitions.size() > 1) {
            sync_partitions(partitions);
            arbitrary_partition->push_request(request);
            sync_partitions(partitions);

            for (auto& p: partitions) {
                for (auto& p2: partitions) {
                    conflict_matrix_[p->id()][p2->id()] = 1;
                }
            }
        } else {
            conflict_matrix_[arbitrary_partition->id()][arbitrary_partition->id()] = 1;
            arbitrary_partition->push_request(request);
        }

        requests_per_thread_[arbitrary_partition->id()]++;
        n_dispatched_requests_++;

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

        if (
            n_dispatched_requests_ % repartition_interval_ == 0
        ) {
            //std::cout << "Partitions Before: [";
            //for (auto& p : partitions_to_checkpoint_) {
            //    std::cout << p << ",";
            //}
            //std::cout << "]" << std::endl;
            struct client_message checkpoint_message; 
            checkpoint_message.type = CHECKPOINT;
            const int partition_id = *partitions_to_checkpoint_.begin();

            std::unordered_set<Partition<T>*> partitions_turn;
            for (int i = 0; i < n_partitions_; i++) {
                if (conflict_matrix_[partition_id][i] == 1) {
                    partitions_turn.emplace(partitions_.at(i));
                    partitions_to_checkpoint_.erase(i);
                    conflict_matrix_[partition_id][i] = 0;
                    for (int k = 0; k < n_partitions_; k++) {
                        if (k != i && conflict_matrix_[i][k] == 1) {
                            partitions_turn.emplace(partitions_.at(i));
                            conflict_matrix_[i][k] = 0;
                        }
                    }
                }
            }
            sync_partitions(partitions_turn);
            for (auto& p: partitions_turn) {
                p->push_request(checkpoint_message);
            }
            ckp_turns.push_back(partitions_turn);
            sync_partitions(partitions_turn);
            if (partitions_to_checkpoint_.size() == 0) {
                //std::cout << "Rebuind partitions for checkpointing\n";
                for (int i = 0; i < n_partitions_; i++) {
                    partitions_to_checkpoint_.emplace(i);
                    for (auto j = 0; j < n_partitions_; j++) {
                      conflict_matrix_[i][j] = 0;
                    }
                }
            }
            //std::cout << "Partitions now: [";
            //for (auto& p : partitions_to_checkpoint_) {
            //    std::cout << p << ",";
            //}
            //std::cout << "]" << std::endl;
        }
    }

    std::vector<std::unordered_set<Partition<T>*>>& get_ckp_turns() {
        return ckp_turns;
    }

private:
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
    kvstorage::Storage storage_;
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
};

};


#endif
