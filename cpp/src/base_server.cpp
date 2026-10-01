#include "pookiecpp/base_server.h"

#include <array>
#include <chrono>
#include <deque>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace pookiecpp {
namespace {

std::string new_id() {
    std::array<std::uint8_t, 16> bytes;
    std::random_device random;
    for (auto& byte : bytes) byte = static_cast<std::uint8_t>(random());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    std::ostringstream result;
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) result << '-';
        result << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(bytes[index]);
    }
    return result.str();
}

google::protobuf::Timestamp now_timestamp() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now);
    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(seconds.count());
    timestamp.set_nanos(static_cast<int>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - seconds).count()));
    return timestamp;
}

}  // namespace

class BaseServer::Session final : public grpc::ServerBidiReactor<Message, Message> {
public:
    Session(BaseServer& owner, grpc::CallbackServerContext* context)
        : owner_(owner), context_(context), peer_{context->peer(), new_id(), "", ""} {
        const auto metadata = context->client_metadata().find("x-schema-version");
        const std::string client_schema = metadata == context->client_metadata().end()
                                              ? "" : std::string(metadata->second.data(), metadata->second.size());
        if (client_schema != owner_.config_.schema_version) {
            Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                "Proto schema mismatch: server=" + owner_.config_.schema_version +
                                    ", client=" + client_schema));
            return;
        }
        StartRead(&incoming_);
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            std::lock_guard lock(write_mutex_);
            read_closed_ = true;
            if (!writing_ && !finished_) finish(grpc::Status::OK);
            return;
        }
        try {
            if (peer_.client_id.empty()) {
                accept();
            } else {
                receive();
            }
        } catch (const std::exception& error) {
            std::lock_guard lock(write_mutex_);
            if (!finished_) finish(grpc::Status(grpc::StatusCode::INTERNAL, error.what()));
            return;
        }
        if (!finished_.load()) {
            incoming_.Clear();
            StartRead(&incoming_);
        }
    }

    void OnWriteDone(bool ok) override {
        std::lock_guard lock(write_mutex_);
        outgoing_.pop_front();
        writing_ = false;
        if (!ok) {
            if (!finished_) finish(grpc::Status(grpc::StatusCode::CANCELLED, "write failed"));
        } else if (!finished_ && !outgoing_.empty()) {
            write_next();
        } else if (read_closed_ && !finished_) {
            finish(grpc::Status::OK);
        }
    }

    void OnDone() override {
        owner_.unsubscribe(this);
        try { owner_.on_client_disconnect(peer_); } catch (...) {}
        delete this;
    }

    void enqueue(std::shared_ptr<const Message> message) {
        std::lock_guard lock(write_mutex_);
        if (finished_ || read_closed_ ||
            (owner_.config_.max_queue_elements != 0 &&
             outgoing_.size() >= owner_.config_.max_queue_elements)) return;
        outgoing_.push_back(std::move(message));
        if (!writing_) write_next();
    }

    const Peer& peer() const { return peer_; }
    bool registered() const { return registered_; }
    const std::vector<std::string>& topics() const { return topics_; }

private:
    void accept() {
        const auto& info = incoming_.metainfo().clientinfo();
        if (info.uuid().empty() || !owner_.on_client_connect(incoming_, *context_)) {
            std::lock_guard lock(write_mutex_);
            finish(grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "connection rejected by server"));
            return;
        }
        peer_.client_id = info.uuid();
        peer_.name = info.name();
        if (!owner_.reserve_client(this)) {
            std::lock_guard lock(write_mutex_);
            finish(grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "duplicate client uuid"));
            return;
        }
        registered_ = true;
        owner_.on_client_accepted(peer_, incoming_);

        auto welcome = std::make_shared<Message>();
        auto* metadata = welcome->mutable_metainfo();
        metadata->mutable_serverinfo()->set_serverid(owner_.server_id_);
        metadata->mutable_serverinfo()->set_uuid(peer_.session_id);
        metadata->mutable_serverinfo()->set_name(owner_.name_);
        metadata->set_messageid(new_id());
        *metadata->mutable_timestamp() = now_timestamp();
        enqueue(std::move(welcome));
        topics_.assign(info.requires_().begin(), info.requires_().end());
        owner_.subscribe(this, topics_);
    }

    void receive() {
        if (incoming_.history_size() != 0) {
            auto* point = incoming_.add_history();
            point->set_name("server");
            *point->mutable_receivetimestamp() = now_timestamp();
            point->set_perfcounter(std::chrono::duration<double>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
        }
        if (owner_.on_receive(peer_, incoming_)) {
            owner_.broadcast(peer_, std::make_shared<const Message>(std::move(incoming_)));
        }
    }

    void write_next() {
        writing_ = true;
        if (outgoing_.front()->history_size() != 0) {
            auto stamped = std::make_shared<Message>(*outgoing_.front());
            auto* point = stamped->mutable_history(stamped->history_size() - 1);
            point->set_perfcounter(std::chrono::duration<double>(
                std::chrono::steady_clock::now().time_since_epoch()).count() -
                                   point->perfcounter());
            *point->mutable_sendtimestamp() = now_timestamp();
            outgoing_.front() = std::move(stamped);
        }
        auto& message = *outgoing_.front();
        try { owner_.on_data_yield(peer_, message); } catch (...) {}
        StartWrite(&message);
    }

    void finish(grpc::Status status) {
        finished_ = true;
        Finish(std::move(status));
    }

    BaseServer& owner_;
    grpc::CallbackServerContext* context_;
    Peer peer_;
    std::vector<std::string> topics_;
    Message incoming_;
    std::mutex write_mutex_;
    std::deque<std::shared_ptr<const Message>> outgoing_;
    bool writing_ = false;
    bool read_closed_ = false;
    std::atomic<bool> finished_{false};
    bool registered_ = false;
};

BaseServer::BaseServer(int port, std::string name, std::string ip, ServerConfig config)
    : port_(port), name_(std::move(name)), ip_(std::move(ip)), server_id_(new_id()),
      config_(std::move(config)) {}

BaseServer::~BaseServer() { shutdown(); }

void BaseServer::start() {
    std::lock_guard lock(lifecycle_mutex_);
    if (server_ || stopped_) throw std::logic_error("server cannot be started twice");
    on_init();
    grpc::ServerBuilder builder;
    for (const auto& [key, value] : config_.channel_arguments) builder.AddChannelArgument(key, value);
    builder.AddListeningPort(ip_ + ":" + std::to_string(port_), config_.credentials, &bound_port_);
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    if (!server_) throw std::runtime_error("failed to start gRPC server");
}

void BaseServer::serve_forever() {
    start();
    server_->Wait();
}

void BaseServer::shutdown() {
    std::unique_lock lock(lifecycle_mutex_);
    if (!server_ || stopped_) return;
    stopped_ = true;
    auto* server = server_.get();
    lock.unlock();
    server->Shutdown(std::chrono::system_clock::now() + config_.shutdown_grace_period);
    server->Wait();
    on_shutdown();
}

grpc::ServerBidiReactor<Message, Message>* BaseServer::DataChannel(grpc::CallbackServerContext* context) {
    return new Session(*this, context);
}

bool BaseServer::reserve_client(Session* session) {
    std::lock_guard lock(routes_mutex_);
    return client_ids_.insert(session->peer().client_id).second;
}

void BaseServer::subscribe(Session* session, const std::vector<std::string>& topics) {
    std::lock_guard lock(routes_mutex_);
    for (const auto& topic : topics) routes_[topic].insert(session);
}

void BaseServer::unsubscribe(Session* session) {
    std::lock_guard lock(routes_mutex_);
    if (session->registered()) client_ids_.erase(session->peer().client_id);
    for (const auto& topic : session->topics()) {
        auto route = routes_.find(topic);
        if (route == routes_.end()) continue;
        route->second.erase(session);
        if (route->second.empty()) routes_.erase(route);
    }
}

void BaseServer::broadcast(const Peer& sender, std::shared_ptr<const Message> message) {
    std::shared_lock lock(routes_mutex_);
    auto route = routes_.find(message->metainfo().messagename());
    if (route == routes_.end()) return;
    for (Session* session : route->second) {
        if (session->peer().client_id != sender.client_id) session->enqueue(message);
    }
}

}  // namespace pookiecpp