#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <iostream>
#include <netinet/tcp.h>

#include <sstream>
#include <string>

#include "evclient/evclient.h"
#include "request/request_generation.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 128;
static unsigned short REPLY_PORT;


static void
send_requests(client* c, const std::vector<workload::Request>& requests)
{
	auto* v = (struct client_message*)c->send_buffer;
    v->sin_port = htons(REPLY_PORT);

    auto counter = 0;
    for (auto& request: requests) {
        v->id = counter;
        v->type = request.type();
        v->key = request.key();
        if (request.args().empty()) {
            memset(v->args, '#', VALUE_SIZE);
            v->size = VALUE_SIZE;
        }
        else {
            for (auto i = 0; i < request.args().size(); i++) {
                v->args[i] = request.args()[i];
            }
            v->args[request.args().size()] = 0;
            v->size = request.args().size();
        }
        auto size = sizeof(struct client_message) + v->size;
        paxos_submit(c->bev, c->send_buffer, size);
        counter++;
    }
}

static void
read_reply(struct bufferevent* bev, void* args)
{
    reply_message reply;
    bufferevent_read(bev, &reply, sizeof(reply_message));
    printf("Result of query %d was %s\n", reply.id, reply.answer);
}

void usage(std::string name) {
    std::cout << "Usage: " << name << " port id client_config request_config\n";
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        usage(std::string(argv[0]));
        exit(1);
    }

    REPLY_PORT = atoi(argv[1]);
    auto client_id = atoi(argv[2]);
    auto client_config = std::string(argv[3]);
    auto requests_path = std::string(argv[4]);

    auto* client = make_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        REPLY_PORT, nullptr, read_reply
    );
	signal(SIGPIPE, SIG_IGN);

    auto requests = workload::import_requests(requests_path);
    send_requests(client, requests);
	event_base_dispatch(client->base);

    client_free(client);

    return 0;
}
