#include "pookiecpp/base_server.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

using pookiecpp::Message;

class TestServer final : public pookiecpp::BaseServer {
public:
    using BaseServer::BaseServer;

    bool on_client_connect(const Message& request, grpc::CallbackServerContext&) override {
        return request.metainfo().clientinfo().uuid() != "rejected";
    }

    bool on_receive(const pookiecpp::Peer&, Message& request) override {
        ++received;
        request.mutable_metainfo()->set_responsetoid("observed");
        return true;
    }

    std::atomic<int> received{0};
};

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

Message connect_message(const std::string& id, bool subscribe) {
    Message request;
    auto* info = request.mutable_metainfo()->mutable_clientinfo();
    info->set_uuid(id);
    info->set_name(id);
    if (subscribe) info->add_requires_("topic");
    return request;
}

void read_welcome(grpc::ClientReaderWriter<Message, Message>& stream) {
    Message welcome;
    require(stream.Read(&welcome), "welcome missing");
    require(!welcome.metainfo().serverinfo().serverid().empty(), "server ID missing");
    require(!welcome.metainfo().serverinfo().uuid().empty(), "session ID missing");
}

}  // namespace

int main() {
    TestServer server(0, "test", "127.0.0.1");
    server.start();
    require(server.bound_port() > 0, "server failed to bind");
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(server.bound_port()),
                                       grpc::InsecureChannelCredentials());
    auto stub = message::proto::v3::Stream::NewStub(channel);

    grpc::ClientContext mismatch;
    mismatch.AddMetadata("x-schema-version", "wrong");
    mismatch.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    auto bad_stream = stub->DataChannel(&mismatch);
    Message result;
        require(!bad_stream->Read(&result), "schema mismatch was not rejected");
        require(bad_stream->Finish().error_code() == grpc::StatusCode::FAILED_PRECONDITION,
            "schema mismatch returned wrong status");

            grpc::ClientContext denied_context;
            denied_context.AddMetadata("x-schema-version", "pookiepy.schema.v0");
            denied_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            auto denied = stub->DataChannel(&denied_context);
            auto rejected = connect_message("rejected", false);
            denied->Write(rejected);
            require(!denied->Read(&result), "rejected client received welcome");
            require(denied->Finish().error_code() == grpc::StatusCode::PERMISSION_DENIED,
                "rejected client returned wrong status");

    grpc::ClientContext subscriber_context;
    subscriber_context.AddMetadata("x-schema-version", "pookiepy.schema.v0");
    subscriber_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    auto subscriber = stub->DataChannel(&subscriber_context);
    auto registration = connect_message("subscriber", true);
    require(subscriber->Write(registration), "subscriber registration failed");
    read_welcome(*subscriber);

        grpc::ClientContext duplicate_context;
        duplicate_context.AddMetadata("x-schema-version", "pookiepy.schema.v0");
        duplicate_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto duplicate = stub->DataChannel(&duplicate_context);
        duplicate->Write(registration);
        require(!duplicate->Read(&result), "duplicate UUID received welcome");
        require(duplicate->Finish().error_code() == grpc::StatusCode::ALREADY_EXISTS,
            "duplicate UUID returned wrong status");

    grpc::ClientContext sender_context;
    sender_context.AddMetadata("x-schema-version", "pookiepy.schema.v0");
    sender_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    auto sender = stub->DataChannel(&sender_context);
    auto sender_registration = connect_message("sender", true);
    require(sender->Write(sender_registration), "sender registration failed");
    read_welcome(*sender);

    Message payload;
    payload.mutable_metainfo()->set_messagename("topic");
    payload.mutable_payload()->set_bytepayload("test-data");
    payload.add_history()->set_name("sender");
    require(sender->Write(payload), "payload write failed");
    require(subscriber->Read(&result), "subscriber did not receive payload");
    require(result.payload().bytepayload() == "test-data", "payload corrupted");
    require(result.metainfo().responsetoid() == "observed", "receive hook did not run");
    require(server.received == 1, "unexpected receive hook call count");
    require(result.history_size() == 2, "server history point missing");
    require(result.history(1).has_receivetimestamp(), "receive timestamp missing");
    require(result.history(1).has_sendtimestamp(), "send timestamp missing");
    require(result.history(1).perfcounter() >= 0, "negative server processing time");

    sender->WritesDone();
    require(!sender->Read(&result), "sender received its own payload");
    require(sender->Finish().ok(), "sender stream failed");
    subscriber->WritesDone();
    require(!subscriber->Read(&result), "subscriber stream did not close");
    require(subscriber->Finish().ok(), "subscriber stream failed");
    server.shutdown();
}