#ifndef MODEL_GRAPH_H
#define MODEL_GRAPH_H

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <unordered_map>
#include <vector>

namespace model {

template <typename T> class Graph {
public:
  Graph() = default;

  long get_seconds_since_epoch() {
    auto now_second = std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
    now_second = std::round(now_second / 15.0);
    return now_second;
  }

  void add_vertice(T data, int weight = 0) {
    vertex_weight_[data] = weight;
    const auto now_second = get_seconds_since_epoch();
    timed_vertex_weight_[data] = std::unordered_map<long, int>();
    timed_vertex_weight_[data][now_second] = weight;
    edges_weight_[data] = std::unordered_map<T, int>();
    timed_edges_weight_[data] = std::unordered_map<T, std::unordered_map<long, int>>();
    total_vertex_weight_ += weight;
  }

  void add_edge(T from, T to, int weight = 0) {
    if (edges_weight_[from].find(to) == edges_weight_[from].end()) {
      edges_weight_[from][to] = 0;
      edges_weight_[to][from] = 0;
    }

    edges_weight_[from][to] = weight;
    edges_weight_[to][from] = weight;

    const auto now_second = get_seconds_since_epoch();
    timed_edges_weight_[from][to][now_second] = weight;
    timed_edges_weight_[to][from][now_second] = weight;
    n_edges_++;
    total_edges_weight_ += weight;
  }

  void increase_vertice_weight(T vertice, int value = 1) {
    vertex_weight_[vertice] += value;

    const auto now_second = get_seconds_since_epoch();
    auto& vertice_timed_weights = timed_vertex_weight_[vertice];
    vertice_timed_weights.find(now_second) != vertice_timed_weights.end()
        ? vertice_timed_weights[now_second] += value
        : vertice_timed_weights[now_second] = value;

    total_vertex_weight_ += value;
  }

  void increase_edge_weight(T from, T to, int value = 1) {
    edges_weight_[from][to] += value;
    edges_weight_[to][from] += value;

    const auto now_second = get_seconds_since_epoch();
    timed_edges_weight_[from][to][now_second] += value;
    timed_edges_weight_[to][from][now_second] += value;

    total_edges_weight_ += value;
  }

  bool vertice_exists(T vertice) const {
    return vertex_weight_.find(vertice) != vertex_weight_.end();
  }

  bool are_connected(T vertice_a, T vertice_b) const {
    return edges_weight_.at(vertice_a).find(vertice_b) !=
           edges_weight_.at(vertice_a).end();
  }

  std::vector<T> sorted_vertex() const {
    std::vector<T> sorted_vertex_;
    for (auto &it : vertex_weight_) {
      sorted_vertex_.emplace_back(it.first);
    }
    std::sort(sorted_vertex_.begin(), sorted_vertex_.end());
    return sorted_vertex_;
  }

  std::vector<T> sorted_timed_vertex() const {
    std::vector<T> sorted_vertex_;
    for (auto &it : vertex_weight_) {
      sorted_vertex_.emplace_back(it.first);
    }
    std::sort(sorted_vertex_.begin(), sorted_vertex_.end());
    return sorted_vertex_;
  }

  std::size_t n_vertex() const { return vertex_weight_.size(); }
  std::size_t n_edges() const { return edges_weight_.size(); }
  int total_vertex_weight() const { return total_vertex_weight_; }
  int total_edges_weight() const { return total_edges_weight_; }
  int vertice_weight(T vertice) const { return vertex_weight_.at(vertice); }
  int edge_weight(T from, T to) const { return edges_weight_.at(from).at(to); }
  const std::unordered_map<T, int> &vertice_edges(T vertice) const {
    return edges_weight_.at(vertice);
  }
  const std::unordered_map<T, std::unordered_map<long, int>> &timed_vertice_edges(T vertice) const {
    return timed_edges_weight_.at(vertice);
  }
  const std::unordered_map<T, int> &vertex() const { return vertex_weight_; }
  const std::unordered_map<T, std::unordered_map<long, int>>& timed_vertex() const { return timed_vertex_weight_; }

  void clear_timed_vertex(T vertice, std::vector<long> times_to_remove) {
    if (times_to_remove.empty()) {
      return;
    }

    auto outer_it = timed_vertex_weight_.find(vertice);
    if (outer_it != timed_vertex_weight_.end()) {
      auto& inner_map = outer_it->second;
      for (long& key : times_to_remove) {
        auto inner_it = inner_map.find(key);
        if (inner_it != inner_map.end()) {
          inner_map.erase(inner_it);
        }
      }
    }
  }
  void clear_timed_edge(T vertice, std::vector<long> times_to_remove) {
    auto it = timed_edges_weight_.find(vertice);
    if (it != timed_edges_weight_.end()) {
      for (long& key : times_to_remove) {
        for (auto& nw : it->second) {
          nw.second.erase(key);
        }
      }
    }
  }

private:
  std::unordered_map<T, int> vertex_weight_;
  std::unordered_map<T, std::unordered_map<long, int>> timed_vertex_weight_;

  std::unordered_map<T, std::unordered_map<T, int>> edges_weight_;
  std::unordered_map<T, std::unordered_map<T, std::unordered_map<long, int>>> timed_edges_weight_;
  int n_edges_{0};
  int total_vertex_weight_{0};
  int total_edges_weight_{0};
};

} // namespace model

#endif
