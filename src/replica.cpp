/*
 * Copyright (c) 2014-2015, University of Lugano
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holders nor the names of it
 *       contributors may be used to endorse or promote products derived from
 *       this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <iterator>
#include <memory>
#include <mutex>
#include <netinet/tcp.h>
#include <signal.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "graph/graph.hpp"
#include "request/request_generation.h"
#include "scheduler/scheduler.hpp"
#include "types/types.h"

using toml_config =
    toml::basic_value<toml::discard_comments, std::unordered_map, std::vector>;

static int verbose = 0;
static int SLEEP = 1;
static bool RUNNING = true;

void metrics_loop(int sleep_duration, int n_requests,
                  kvpaxos::Scheduler<int> *scheduler) {
  auto already_counted_throughput = 0;
  auto counter = 0;
  std::ofstream thr("results/throughput.csv");
  thr << "time,requests" << std::endl;
  while (RUNNING) {
    std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
    auto executed_requests = scheduler->n_executed_requests();
    auto throughput = executed_requests - already_counted_throughput;
    thr << counter << ",";
    thr << throughput << "\n";
    already_counted_throughput += throughput;
    counter++;

    if (executed_requests >= n_requests) {
      break;
    }
  }
  thr.close();
}

static kvpaxos::Scheduler<int> *
initialize_scheduler(std::vector<workload::Request> &requests,
                     const toml_config &config,
                     std::shared_ptr<kvstorage::Storage> storage,
                     long sliding_window) {
  auto n_partitions = toml::find<int>(config, "n_partitions");
  auto repartition_method_s =
      toml::find<std::string>(config, "repartition_method");
  auto repartition_method =
      model::string_to_cut_method.at(repartition_method_s);
  auto repartition_interval = toml::find<int>(config, "repartition_interval");
  auto n_checkpointers = toml::find<int>(config, "n_checkpointers");
  auto *scheduler = new kvpaxos::Scheduler<int>(
      requests.size(), repartition_interval, n_partitions, n_checkpointers,
      repartition_method, storage, sliding_window);

  auto n_initial_keys = toml::find<int>(config, "n_initial_keys");

  scheduler->process_populate_requests(n_initial_keys);

  scheduler->run();
  return scheduler;
}

std::vector<struct client_message>
to_client_messages(std::vector<workload::Request> &requests) {
  std::vector<struct client_message> client_messages;
  auto counter = 0;
  for (auto i = 0; i < requests.size(); i++) {
    auto &request = requests[i];
    struct client_message client_message;
    client_message.sin_port = htons(0);
    client_message.id = i;
    client_message.type = request.type();
    client_message.key = request.key();
    for (auto i = 0; i < request.args().size(); i++) {
      client_message.args[i] = request.args()[i];
    }
    client_message.args[request.args().size()] = 0;
    client_message.size = request.args().size();

    client_messages.emplace_back(client_message);
  }

  return client_messages;
}

void execute_requests(kvpaxos::Scheduler<int> &scheduler,
                      std::vector<struct client_message> &requests,
                      int print_percentage) {
  for (auto &request : requests) {
    scheduler.schedule_and_answer(request);
  }
}

std::unordered_map<int, time_point>
join_maps(std::vector<std::unordered_map<int, time_point>> maps) {
  std::unordered_map<int, time_point> joined_map;
  for (auto &map : maps) {
    joined_map.insert(map.begin(), map.end());
  }
  return joined_map;
}

static void run(const toml_config &config, const std::string conf_name) {
  using namespace std::literals;
  const auto results_dir = std::string("results/") + conf_name;
  std::filesystem::create_directory("results");
  std::filesystem::create_directory(results_dir);

  auto requests_path = toml::find<std::string>(config, "requests_path");
  auto requests = std::move(workload::import_cs_requests(requests_path));

  const auto storage = std::make_shared<kvstorage::Storage>();
  //std::cout << "Initializing scheduler" << std::endl;
  const long sliding_window = 999;
  auto *scheduler = initialize_scheduler(requests, config, storage, sliding_window);

  auto print_percentage = toml::find<int>(config, "print_percentage");
  auto client_messages = to_client_messages(requests);

  auto throughput_thread = std::thread(metrics_loop, SLEEP, requests.size(), scheduler);
  
  const auto start_execution_timestamp = std::chrono::system_clock::now();
  //std::cout << "Executing requests" << std::endl;
  execute_requests(*scheduler, client_messages, print_percentage);

  throughput_thread.join();
  auto end_execution_timestamp = std::chrono::system_clock::now();

  auto makespan = std::chrono::duration_cast<std::chrono::milliseconds>(end_execution_timestamp - start_execution_timestamp);

  std::ofstream msmetric(results_dir + std::string("/makespan.csv"));
  msmetric << "makespan(ms)" << std::endl;
  msmetric << makespan.count() << std::endl;
  msmetric.close();

  auto &partitions = scheduler->get_partitions();

  auto checkpointers = scheduler->get_checkpointers();
  auto checkpoint_times = std::vector<checkpoint::checkpoint_times>();
  for (auto &ckp : checkpointers) {
    for (auto &t : ckp->get_checkpoint_times()) {
      checkpoint_times.push_back(t);
    }
  }
  auto checkpoint_avg_elapsed_time = std::unordered_map<int, long>();
  auto checkpoint_avg_size = std::unordered_map<int, long>();
  auto num_of_checkpoints = 0;

  std::ofstream ckp_metrics(results_dir + std::string("/checkpoint_raw_metrics.csv"));
  ckp_metrics << "count,partition_id,start_time,end_time,duration,size(M),num_keys" << std::endl;
  for (auto& t : checkpoint_times) {
      if (t.count > num_of_checkpoints)
        num_of_checkpoints = t.count;

      ckp_metrics << t.count 
                << "," << t.partition_id 
                << ","
                << std::chrono::duration_cast<std::chrono::milliseconds>(t.start_time - start_execution_timestamp).count()
                << ","
                << std::chrono::duration_cast<std::chrono::milliseconds>(t.end_time - start_execution_timestamp).count()
                << "," << t.time_taken 
                << "," << t.size/1024/1024 // MB
                << "," << t.num_keys
                << std::endl;

      if (checkpoint_avg_elapsed_time.find(t.partition_id) == checkpoint_avg_elapsed_time.end()) {
        checkpoint_avg_elapsed_time[t.partition_id] = t.time_taken;
      } else {
        checkpoint_avg_elapsed_time[t.partition_id] = checkpoint_avg_elapsed_time.at(t.partition_id) + t.time_taken;
      }

      if (checkpoint_avg_size.find(t.partition_id) == checkpoint_avg_size.end()) {
        checkpoint_avg_size[t.partition_id] = t.size;
      } else {
        checkpoint_avg_size[t.partition_id] = checkpoint_avg_size.at(t.partition_id) + t.size;
      }
  }
  ckp_metrics.close();

  std::ofstream ckp_times(results_dir + std::string("/checkpoint_times.csv"));
  ckp_times << "partition,avg-elapsed-time" << std::endl;
  for (auto& elapsed_time: checkpoint_avg_elapsed_time) {
    ckp_times << elapsed_time.first << "," << elapsed_time.second/num_of_checkpoints << std::endl;
  }
  ckp_times.close();

  std::ofstream ckp_sizes(results_dir + std::string("/checkpoint_sizes.csv"));
  ckp_sizes << "partition,avg-size" << std::endl;
  for (auto& size: checkpoint_avg_size) {
    ckp_sizes << size.first << "," << size.second/num_of_checkpoints << std::endl;
  }
  ckp_sizes.close();

  auto &csb_requests = scheduler->get_crossborder_requests();
  std::ofstream ckp_cross(results_dir + std::string("/crossborder_req.csv"));
  ckp_cross << "num_partitions,num_requests" << std::endl;
  for (int i = 1; i < csb_requests.size(); i++) {
    ckp_cross << i << "," << csb_requests.at(i) << std::endl;
  }
  ckp_cross.close();

  auto &reqs_by_thread = scheduler->get_requests_per_thread();
  std::ofstream req_per_thread(results_dir + std::string("/request_per_thread.csv"));
  req_per_thread << "thread,num_requests" << std::endl;
  for (int i = 0; i < reqs_by_thread.size(); i++) {
    req_per_thread << i << "," << reqs_by_thread.at(i) << std::endl;
  }
  req_per_thread.close();

  std::ofstream rep(results_dir + std::string("/repartition_metrics.csv"));
  rep << "start,end,timetaken" << std::endl;
  for (auto &repartition_times : scheduler->repartition_timestamps()) {
    rep << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<0>(repartition_times) - start_execution_timestamp).count()
      << "," 
      << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - start_execution_timestamp).count()
      << ","
      << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - std::get<0>(repartition_times)).count()
      << std::endl;
  }
  rep.close();

}

static void usage(std::string prog) {
  std::cout << "Usage: " << prog << " config\n";
}

int main(int argc, char const *argv[]) {
  if (argc < 2) {
    usage(std::string(argv[0]));
    exit(1);
  }

  // workload::generate_workload_a(argv[1], std::stoi(argv[2]));
  auto filename = argv[1];
  const auto config = toml::parse(filename);

  std::string ss(filename);
  auto dot_pos = ss.find(".");
  run(config, ss.substr(0, dot_pos));

  return 0;
}
