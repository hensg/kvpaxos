#ifndef _EVPAXOS_CLIENT_H_
#define _EVPAXOS_CLIENT_H_


#include <errno.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <evpaxos.h>
#include <iostream>
#include <stdlib.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <time.h>
#include <pthread.h>
#include <string.h>
#include <signal.h>
#include <netinet/tcp.h>
#include <unordered_map>
#include <unordered_set>

#include "types/types.h"


struct client* make_client(
    const char* config, int proposer_id, int outstanding,
	int value_size, bufferevent_event_cb on_connect,
	reply_callback on_reply
);
void listen_server(
	tbb::concurrent_vector<time_point>& latencies,
	tbb::concurrent_unordered_map<int, time_point>& timestamps,
	unsigned short port,
	pthread_barrier_t& start_barrier);
void client_free(struct client* c);


#endif
