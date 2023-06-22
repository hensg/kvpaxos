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
static bool RUNNING = false;

void metrics_loop(int sleep_duration, int n_requests,
                  kvpaxos::Scheduler<int> *scheduler,
                  std::string results_dir,
                  std::chrono::time_point<std::chrono::system_clock> start_time) {
  auto already_counted_throughput = 0;
  std::ofstream thr(results_dir + std::string("/throughput.csv"));
  thr << "time,requests" << std::endl;
  while (RUNNING) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto now_second = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now() - start_time
    ).count();

    auto executed_requests = scheduler->n_executed_requests();
    auto throughput = executed_requests - already_counted_throughput;

    thr << now_second << "," << throughput << std::endl;

    already_counted_throughput += throughput;
    if (executed_requests >= n_requests) {
      break;
    }
  }
  thr.flush();
  thr.close();
}

static kvpaxos::Scheduler<int> *
initialize_scheduler(long n_requests,
                     std::shared_ptr<kvstorage::Storage> storage,
                     int n_partitions, int repartition_interval,
                      int n_checkpointers, model::CutMethod repartition_method,
                     int sliding_window,
                     int max_key,
                     std::chrono::system_clock::time_point* start_time
                     ) {
  auto *scheduler = new kvpaxos::Scheduler<int>(
      n_requests, repartition_interval, n_partitions, n_checkpointers,
      repartition_method, storage, sliding_window, start_time);

  std::cout << "Populating keys..." << std::endl;
  scheduler->process_populate_requests(max_key);
  *start_time = std::chrono::system_clock::now();
  std::cout << "Running..." << std::endl;
  scheduler->run();
  return scheduler;
}

std::vector<struct client_message>
to_client_messages(std::vector<workload::Request> &requests) {
  std::vector<struct client_message> client_messages;
  auto counter = 0;
  for (auto i = 0; i < requests.size(); i++) {
    auto& request = requests[i];
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
                      std::vector<struct client_message> &client_messages,
                      int print_percentage) {
  RUNNING = true;
  for (auto &request : client_messages) {
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

static kvpaxos::Scheduler<int>* run_scheduler(std::vector<client_message>& client_messages,
                                              long n_requests,
                                              std::string results_dir,
                                              int n_partitions,
                                              int repartition_interval,
                                              int n_checkpointers,
                                              model::CutMethod repartition_method,
                                              int sliding_window,
                                              std::chrono::time_point<std::chrono::system_clock>* start_time,
                                              int max_key) {
  
  const auto storage = std::make_shared<kvstorage::Storage>();
  auto *scheduler = initialize_scheduler(n_requests, storage, n_partitions,
                                         repartition_interval, n_checkpointers,
                                         repartition_method, sliding_window, max_key, start_time);


  //auto throughput_thread = std::thread(metrics_loop, SLEEP, n_requests, scheduler, results_dir, start_time);
  execute_requests(*scheduler, client_messages, 0);
  //throughput_thread.join();
  return scheduler;
};

static void run_workload(std::string requests_path, int repartition_interval, std::string results_dir) {
  auto n_partitions = 8;
  auto n_checkpointers = 8;

  int* max_key = new int(0);
  
  auto requests = workload::import_cs_requests_csv(requests_path, max_key);
  auto n_requests = requests.size();
  auto client_messages = to_client_messages(requests);

  std::chrono::time_point<std::chrono::system_clock>* start_time = new std::chrono::time_point<std::chrono::system_clock>(std::chrono::system_clock::now());
 
  // RR
  std::cout << "Running RR" << std::endl;
  auto repartition_method = model::string_to_cut_method.at("ROUND_ROBIN");
  auto sliding_window = 999999;
  auto scheduler = run_scheduler(
    client_messages,
    n_requests,
    results_dir,
    n_partitions,
    repartition_interval,
    n_checkpointers,
    repartition_method,
    sliding_window,
    start_time,
    *max_key);
  scheduler->wait_requests_execution();
  std::unordered_map<long, int> rr_thr = scheduler->get_executed_requests();
  auto end_execution_timestamp = std::chrono::system_clock::now();
  auto rr_makespan = std::chrono::duration_cast<std::chrono::milliseconds>(end_execution_timestamp - *start_time);

  auto checkpointers = scheduler->get_checkpointers();
  auto n_checkpoints = checkpointers[0]->get_checkpoint_times().size();
  std::vector<std::tuple<long, long, long>> rr_ckp_window;
  long rr_ckp_time = 0;
  for (int i = 0; i < n_checkpoints; i++) {
    std::chrono::system_clock::time_point earliest_start = checkpointers.at(0)->get_checkpoint_times().at(i).start_time;
    std::chrono::system_clock::time_point latest_end = checkpointers.at(0)->get_checkpoint_times().at(i).end_time;
    for (int j = 0; j < checkpointers.size(); j++) {
      auto t = checkpointers.at(j)->get_checkpoint_times().at(i);
      rr_ckp_time += t.time_taken;
      if (earliest_start > t.start_time) {
        earliest_start = t.start_time;
      }
      if (latest_end < t.end_time) {
        latest_end = t.end_time;
      }
    }
    rr_ckp_window.push_back(std::tuple(
      std::chrono::duration_cast<std::chrono::milliseconds>(earliest_start - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - earliest_start).count()
    ));
  }

  auto rr_cb_requests = scheduler->get_crossborder_requests();

  std::cout << "Deleting scheduler" << std::endl;
  delete scheduler;

  // METIS
  std::cout << "Running METIS" << std::endl;

  start_time = new std::chrono::time_point<std::chrono::system_clock>(std::chrono::system_clock::now());
  repartition_method = model::string_to_cut_method.at("METIS");
  sliding_window = 999999;
  scheduler = run_scheduler(
    client_messages,
    n_requests,
    results_dir,
    n_partitions,
    repartition_interval,
    n_checkpointers,
    repartition_method,
    sliding_window,
    start_time,
    *max_key
  );
  std::cout << "Waiting for requests to finish" << std::endl;
  scheduler->wait_requests_execution();
  std::unordered_map<long, int> metis_thr = scheduler->get_executed_requests();
  end_execution_timestamp = std::chrono::system_clock::now();
  auto metis_makespan = std::chrono::duration_cast<std::chrono::milliseconds>(end_execution_timestamp - *start_time);

  checkpointers = scheduler->get_checkpointers();
  n_checkpoints = checkpointers[0]->get_checkpoint_times().size();
  std::vector<std::tuple<long, long, long>> metis_ckp_window;
  long metis_ckp_time = 0;
  for (int i = 0; i < n_checkpoints; i++) {
    std::chrono::system_clock::time_point earliest_start = checkpointers.at(0)->get_checkpoint_times().at(i).start_time;
    std::chrono::system_clock::time_point latest_end = checkpointers.at(0)->get_checkpoint_times().at(i).end_time;
    for (int j = 0; j < checkpointers.size(); j++) {
      auto t = checkpointers.at(j)->get_checkpoint_times().at(i);
      metis_ckp_time += t.time_taken;
      if (earliest_start > t.start_time) {
        earliest_start = t.start_time;
      }
      if (latest_end < t.end_time) {
        latest_end = t.end_time;
      }
    }
    metis_ckp_window.push_back(std::tuple(
      std::chrono::duration_cast<std::chrono::milliseconds>(earliest_start - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - earliest_start).count()
    ));
  }

  std::vector<std::tuple<long,long,long>> metis_repartition_times;
  for (auto &repartition_times : scheduler->repartition_timestamps()) {
    metis_repartition_times.push_back(std::tuple(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<0>(repartition_times) - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - std::get<0>(repartition_times)).count()
    ));
  }

  auto metis_cb_requests = scheduler->get_crossborder_requests();

  std::cout << "Deleting scheduler" << std::endl;
  delete scheduler;

  // // SLIDING WINDOW
  std::cout << "Running - METIS - sliding window-30" << std::endl;
  start_time = new std::chrono::time_point<std::chrono::system_clock>(std::chrono::system_clock::now());
  repartition_method = model::string_to_cut_method.at("METIS");
  sliding_window = 30;
  scheduler = run_scheduler(
    client_messages,
    n_requests,
    results_dir,
    n_partitions,
    repartition_interval,
    n_checkpointers,
    repartition_method,
    sliding_window,
    start_time,
    *max_key
  );
  scheduler->wait_requests_execution();
  std::unordered_map<long, int> sliding_thr30 = scheduler->get_executed_requests();
  end_execution_timestamp = std::chrono::system_clock::now();
  auto sliding_makespan30 = std::chrono::duration_cast<std::chrono::milliseconds>(end_execution_timestamp - *start_time);

  checkpointers = scheduler->get_checkpointers();
  n_checkpoints = checkpointers[0]->get_checkpoint_times().size();
  std::vector<std::tuple<long, long, long>> slide30_ckp_window;
  long slide30_ckp_time = 0;
  for (int i = 0; i < n_checkpoints; i++) {
    std::chrono::system_clock::time_point earliest_start = checkpointers.at(0)->get_checkpoint_times().at(i).start_time;
    std::chrono::system_clock::time_point latest_end = checkpointers.at(0)->get_checkpoint_times().at(i).end_time;
    for (int j = 0; j < checkpointers.size(); j++) {
      auto t = checkpointers.at(j)->get_checkpoint_times().at(i);
      slide30_ckp_time += t.time_taken;
      if (earliest_start > t.start_time) {
        earliest_start = t.start_time;
      }
      if (latest_end < t.end_time) {
        latest_end = t.end_time;
      }
    }
    slide30_ckp_window.push_back(std::tuple(
      std::chrono::duration_cast<std::chrono::milliseconds>(earliest_start - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(latest_end - earliest_start).count()
    ));
  }

  std::vector<std::tuple<long,long,long>> slide_repartition_times;
  for (auto &repartition_times : scheduler->repartition_timestamps()) {
    slide_repartition_times.push_back(std::tuple(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<0>(repartition_times) - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - *start_time).count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - std::get<0>(repartition_times)).count()
    ));
  }

  auto slide_cb_requests = scheduler->get_crossborder_requests();

  delete scheduler;

  // // MAKESPAN
  std::ofstream mkspan_metric(results_dir + std::string("/makespan.csv"));
  mkspan_metric << "baseline,metis,metis-sliding-30" << std::endl;
  mkspan_metric << rr_makespan.count() << "," << metis_makespan.count() << "," << sliding_makespan30.count() << std::endl;
  mkspan_metric.flush();
  mkspan_metric.close();

  // CHECKPOINT TOTAL TIME
  std::ofstream ckp_times(results_dir + std::string("/ckp_total_times.csv"));
  //ckp_times << "baseline,metis,metis-sliding-30,metis-sliding-60" << std::endl;
  //ckp_times << rr_ckp_time << "," << metis_ckp_time << "," << slide30_ckp_time << "," << slide60_ckp_time << std::endl;
  ckp_times << "baseline,metis,metis-sliding-30" << std::endl;
  ckp_times << rr_ckp_time << "," << metis_ckp_time << "," << slide30_ckp_time << std::endl;
  ckp_times.flush();
  ckp_times.close();

  // CHECKPOINT TIMES
  std::ofstream ckp_start_end_window(results_dir + std::string("/ckp_start_window.csv"));
  ckp_start_end_window << "baseline_start,baseline_end,baseline_elapsed,metis_start,metis_end,metis_elapsed,metis-sliding-30_start,metis-sliding-30_end,metis-sliding-30_elapsed" << std::endl;
  for (int ic = 0; ic < n_checkpoints; ic++){
    ckp_start_end_window << 
      std::get<0>(rr_ckp_window[ic]) << "," <<
      std::get<1>(rr_ckp_window[ic]) << "," <<
      std::get<2>(rr_ckp_window[ic]) << "," <<

      std::get<0>(metis_ckp_window[ic]) << "," <<
      std::get<1>(metis_ckp_window[ic]) << "," <<
      std::get<2>(metis_ckp_window[ic]) << "," <<

      std::get<0>(slide30_ckp_window[ic]) << "," <<
      std::get<1>(slide30_ckp_window[ic]) << "," <<
      std::get<2>(slide30_ckp_window[ic]) << std::endl;
  }
  ckp_start_end_window.flush();
  ckp_start_end_window.close();

  // REPARTITION TIME
  std::ofstream repartition_times(results_dir + std::string("/repartition_times.csv"));
  repartition_times << "metis_start,metis_end,metis_elapsed,metis-sliding-30_start,metis-sliding-30_end,metis-sliding-30_elapsed" << std::endl;
  int n_repartitions = metis_repartition_times.size() > slide_repartition_times.size() ? metis_repartition_times.size() : slide_repartition_times.size();
  for (int ir = 0; ir < n_repartitions; ir++) {
    repartition_times <<
      std::get<0>(metis_repartition_times[ir]) << "," <<
      std::get<1>(metis_repartition_times[ir]) << "," <<
      std::get<2>(metis_repartition_times[ir]) << "," <<
      std::get<0>(slide_repartition_times[ir]) << "," <<
      std::get<1>(slide_repartition_times[ir]) << "," <<
      std::get<2>(slide_repartition_times[ir]) << std::endl;
  }
  repartition_times.flush();
  repartition_times.close();

  std::ofstream cb_req(results_dir + std::string("/crossborder_req.csv"));
  cb_req << "num_partitions,baseline,metis,metis-sliding-30" << std::endl;
  for (int ci = 1; ci < rr_cb_requests.size(); ci++) {
    cb_req << ci << "," << rr_cb_requests[ci] << "," << metis_cb_requests[ci] << "," << slide_cb_requests[ci] << std::endl;
  }
  cb_req.flush();
  cb_req.close();

  // THROUGHPUT
  std::ofstream thr_metric(results_dir + std::string("/throughput.csv"));
  thr_metric << "time,baseline,metis,metis-sliding-30" << std::endl;
  int max_time = 0;
  //std::vector<std::unordered_map<long, int>> thrs = {rr_thr, metis_thr, sliding_thr30, sliding_thr60};
  std::vector<std::unordered_map<long, int>> thrs = {rr_thr, metis_thr, sliding_thr30};
  for (auto& thr: thrs) {
    for (auto& t: thr) {
      if (t.first > max_time)
        max_time = t.first;
    }
  }

  long time_i;
  for (time_i = 0; time_i <= max_time; time_i++) {
    thr_metric << time_i << ",";

    if (rr_thr.find(time_i) != rr_thr.end()) {
      thr_metric << rr_thr[time_i] << ",";
    } else {
      thr_metric << "0,";
    }

    if (metis_thr.find(time_i) != metis_thr.end()) {
      thr_metric << metis_thr[time_i] << ",";
    } else {
      thr_metric << "0,";
    }

    if (sliding_thr30.find(time_i) != sliding_thr30.end()) {
      thr_metric << sliding_thr30[time_i] << std::endl;
    } else {
      thr_metric << "0" << std::endl;
    }

  }
  thr_metric.flush();
  thr_metric.close();
  delete max_key;
}

static void run_all() {
  using namespace std::literals;

  auto begin = std::chrono::system_clock::now();

  auto results_dir = std::string("results/a");
  std::filesystem::create_directory("results");
  std::filesystem::create_directory(results_dir);

  // std::cout << "Running workload A" << std::endl;
  // ///auto requests_path = "workloada_sample.csv";
  // ///auto repartition_interval = 51000;
  // auto requests_path = "workloada.csv";
  // auto repartition_interval = 15000000; 
  // run_workload(requests_path, repartition_interval, results_dir);

  std::cout << "Running workload D" << std::endl;
  results_dir = std::string("results/d");
  std::filesystem::create_directory(results_dir);
  auto requests_path = "workloadd.csv";
  auto repartition_interval = 32000000; 
  // requests_path = "workloadd_sample.csv";
  // repartition_interval = 5000000; 
  run_workload(requests_path, repartition_interval, results_dir);

  std::cout << "Running workload E" << std::endl;
  results_dir = std::string("results/e");
  std::filesystem::create_directory(results_dir);
  requests_path = "workloade.csv";
  repartition_interval = 840000; 
  run_workload(requests_path, repartition_interval, results_dir);

  std::cout << "Took: " << std::chrono::duration_cast<std::chrono::minutes>(std::chrono::system_clock::now() - begin).count() << "m" << std::endl;
}

static void run(const toml_config &config, const std::string conf_name) {
  using namespace std::literals;

  const auto results_dir = std::string("results/") + conf_name;
  std::filesystem::create_directory("results");
  std::filesystem::create_directory(results_dir);

  //std::cout << "Initializing scheduler" << std::endl;
  auto requests_path = toml::find<std::string>(config, "requests_path");
  auto sliding_window = 999999; // millis
  //
  auto n_partitions = toml::find<int>(config, "n_partitions");
  auto repartition_method_s = toml::find<std::string>(config, "repartition_method");
  auto repartition_method = model::string_to_cut_method.at(repartition_method_s);
  auto repartition_interval = toml::find<int>(config, "repartition_interval");
  auto n_checkpointers = toml::find<int>(config, "n_checkpointers");

  const auto storage = std::make_shared<kvstorage::Storage>();
  int* max_key = new int(0);
  auto requests = std::move(workload::import_cs_requests_csv(requests_path, max_key));
  const long n_request = requests.size();

  std::chrono::time_point<std::chrono::system_clock>* start_time = new std::chrono::system_clock::time_point(std::chrono::system_clock::now());
  auto print_percentage = toml::find<int>(config, "print_percentage");
  auto client_messages = to_client_messages(requests);

  auto *scheduler = initialize_scheduler(n_request, storage, n_partitions,
                                         repartition_interval, n_checkpointers,
                                         repartition_method, sliding_window, *max_key,
                                         start_time);


  requests.clear();
  auto throughput_thread = std::thread(metrics_loop, SLEEP, n_request, scheduler, results_dir, *start_time);

  std::cout << "Executing requests" << std::endl;
  execute_requests(*scheduler, client_messages, print_percentage);

  throughput_thread.join();

  auto end_execution_timestamp = std::chrono::system_clock::now();

  auto makespan = std::chrono::duration_cast<std::chrono::milliseconds>(end_execution_timestamp - *start_time);

  std::ofstream msmetric(results_dir + std::string("/makespan.csv"));
  msmetric << "makespan" << std::endl;
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
                << std::chrono::duration_cast<std::chrono::milliseconds>(t.start_time - *start_time).count()
                << ","
                << std::chrono::duration_cast<std::chrono::milliseconds>(t.end_time - *start_time).count()
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
    rep << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<0>(repartition_times) - *start_time).count()
      << "," 
      << std::chrono::duration_cast<std::chrono::milliseconds>(std::get<1>(repartition_times) - *start_time).count()
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
  //if (argc < 2) {
  //  usage(std::string(argv[0]));
  //  exit(1);
  //}

  //// workload::generate_workload_a(argv[1], std::stoi(argv[2]));
  //auto filename = argv[1];
  //const auto config = toml::parse(filename);

  //std::string ss(filename);
  //auto dot_pos = ss.find(".");
  //run(config, ss.substr(0, dot_pos));

  run_all();
  return 0;
}
