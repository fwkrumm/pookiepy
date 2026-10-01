#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "message.grpc.pb.h"

namespace pookiecpp {

using Message = message::proto::v3::PookieMessage;

struct Peer {
    std::string address;
    std::string session_id;
    std::string client_id;
    std::string name;
};

struct ServerConfig {
    std::size_t max_queue_elements = 0;
    std::chrono::milliseconds shutdown_grace_period{1000};
    std::string schema_version = "pookiepy.schema.v0";
    std::vector<std::pair<std::string, int>> channel_arguments;
    std::shared_ptr<grpc::ServerCredentials> credentials = grpc::InsecureServerCredentials();
};

class BaseServer : private message::proto::v3::Stream::CallbackService {
public:
    explicit BaseServer(int port, std::string name = "server", std::string ip = "[::]",
                        ServerConfig config = {});
    virtual ~BaseServer();

    BaseServer(const BaseServer&) = delete;
    BaseServer& operator=(const BaseServer&) = delete;

    void start();
    void serve_forever();
    void shutdown();
    int bound_port() const noexcept { return bound_port_; }

    virtual void on_init() {}
    virtual void on_shutdown() {}
    virtual bool on_client_connect(const Message& request, grpc::CallbackServerContext& context) {
        return true;
    }
    virtual void on_client_accepted(const Peer& peer, const Message& request) {}
    virtual bool on_receive(const Peer& peer, Message& request) { return true; }
    virtual void on_data_yield(const Peer& peer, const Message& data) {}
    virtual void on_client_disconnect(const Peer& peer) {}

private:
    class Session;
    grpc::ServerBidiReactor<Message, Message>* DataChannel(grpc::CallbackServerContext* context) override;
    bool reserve_client(Session* session);
    void subscribe(Session* session, const std::vector<std::string>& topics);
    void unsubscribe(Session* session);
    void broadcast(const Peer& sender, std::shared_ptr<const Message> message);

    int port_;
    int bound_port_ = 0;
    std::string name_;
    std::string ip_;
    std::string server_id_;
    ServerConfig config_;
    std::mutex lifecycle_mutex_;
    std::unique_ptr<grpc::Server> server_;
    bool stopped_ = false;
    std::shared_mutex routes_mutex_;
    std::unordered_map<std::string, std::unordered_set<Session*>> routes_;
    std::unordered_set<std::string> client_ids_;
};

}  // namespace pookiecpp