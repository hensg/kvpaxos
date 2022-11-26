#ifndef KVPAXOS_PARTITION_H
#define KVPAXOS_PARTITION_H


#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <ostream>
#include <pthread.h>
#include <queue>
#include <iterator>
#include <mutex>
#include <numeric>
#include <semaphore.h>
#include <sstream>
#include <shared_mutex>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <fstream>

#include "graph/graph.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"
#include "checkpointer.hpp"


namespace kvpaxos {

template <typename T>
class Partition {
public:
    Partition(std::shared_ptr<kvstorage::Storage> storage, int id, checkpoint::Checkpointer<T>* checkpointer) {
        checkpointer_ = checkpointer;
        storage_ = storage;
        id_ = id;
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        n_executed_requests_ = 0;
        executing_ = true;
    }

    Partition(std::shared_ptr<kvstorage::Storage> storage, int id) {
        storage_ = storage;
        id_ = id;
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        n_executed_requests_ = 0;
        executing_ = true;
    }

    ~Partition() {
        executing_ = false;
        if (worker_thread_.joinable()) {
            sem_post(&semaphore_);
            worker_thread_.join();
        }
        delete checkpointer_;
    }

    void start_worker_thread() {
        sem_init(&semaphore_, 0, 0);
        worker_thread_ = std::thread(&Partition<T>::thread_loop, this);
    }

    void push_request(struct client_message request) {
        queue_mutex_.lock();
            requests_queue_.push(std::move(request));
        queue_mutex_.unlock();
        sem_post(&semaphore_);
    }

    void insert_data(const T& data, int weight = 0) {
        weight_[data] = weight;
        total_weight_ += weight;
    }

    void remove_data(const T& data) {
        total_weight_ -= weight_.at(data);
        weight_.erase(data);
    }

    void increase_weight(const T& data, int weight = 1) {
        weight_[data] += weight;
        total_weight_ += weight;
    }

    int weight() const {
        return total_weight_;
    }

    int id() const {
        return id_;
    }

    int n_executed_requests() const {
        return n_executed_requests_;
    }

private:
    /*
    void on_event(struct bufferevent* bev, short ev, void *arg)
    {
        if (ev & BEV_EVENT_EOF || ev & BEV_EVENT_ERROR) {
            bufferevent_free(bev);
        }
    }

    struct sockaddr_in get_client_addr(unsigned long ip, unsigned short port)
    {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ip;
        addr.sin_port = port;
        return addr;
    }

    void answer_client(const char* answer, size_t length,
        client_message& message)
    {
        auto client_addr = get_client_addr(message.s_addr, message.sin_port);
        auto bytes_written = sendto(
            socket_fd_, answer, length, 0,
            (const struct sockaddr *) &client_addr, sizeof(client_addr)
        );
        if (bytes_written < 0) {
            printf("Failed to send answer\n");
        }
    }
    */

    void thread_loop() {
        while (executing_) {
            sem_wait(&semaphore_);
            if (not executing_) {
                return;
            }

            queue_mutex_.lock();
                auto request = std::move(requests_queue_.front());
                requests_queue_.pop();
                if (request.type == WRITE || request.type == READ) {
                    partition_used_keys_.emplace(request.key); 
                }
            queue_mutex_.unlock();

            auto key = request.key;
            auto type = static_cast<request_type>(request.type);
            auto request_args = std::string(request.args);

            std::string answer;
            switch (type)
            {
            case READ:
            {
                answer = std::move(storage_->read(key));
                break;
            }

            case WRITE:
            {
                storage_->write(key, request_args);
                answer = request_args;
                break;
            }

            case SCAN:
            {
                auto length = std::stoi(request_args);
                auto values = std::move(storage_->scan(key, length));
                for (auto i = 0; i < length; i++)
                  partition_used_keys_.emplace(request.key + i); 

                std::ostringstream oss;
                std::copy(values.begin(), values.end(), std::ostream_iterator<std::string>(oss, ","));
                answer = std::string(oss.str());

                std::vector<T> keys(length);
                std::iota(keys.begin(), keys.end(), 1);
                break;
            }

            case SYNC:
            {
                auto barrier = (pthread_barrier_t*) request.s_addr;
                auto coordinator = pthread_barrier_wait(barrier);
                if (coordinator) {
                    pthread_barrier_destroy(barrier);
                    delete barrier;
                }
                break;
            }

            case CHECKPOINT:
            {
                std::unique_lock lk(queue_mutex_);
                std::condition_variable* cv = new std::condition_variable();
                checkpointer_->time_for_checkpoint(partition_used_keys_, cv);
                cv->wait(lk);
                delete cv;
                partition_used_keys_.clear();
                break;
            }

            case ERROR:
            {
                answer = "ERROR";
                break;
            }
            default:
                break;
            }

            if (type == SYNC) {
                continue;
            }

            n_executed_requests_++;
        }
    }

    int id_, socket_fd_, n_executed_requests_;

    std::shared_ptr<kvstorage::Storage> storage_;

    bool executing_;
    std::thread worker_thread_;
    sem_t semaphore_;
    std::queue<struct client_message> requests_queue_;
    std::mutex queue_mutex_;

    checkpoint::Checkpointer<T>* checkpointer_;

    int total_weight_ = 0;
    std::unordered_map<T, int> weight_;

    std::unordered_set<int> partition_used_keys_;
};

}

#endif
