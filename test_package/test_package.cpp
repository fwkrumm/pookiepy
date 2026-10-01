#include "pookiecpp/base_server.h"

int main() {
    pookiecpp::BaseServer server(0, "package-test", "127.0.0.1");
    server.start();
    const bool bound = server.bound_port() > 0;
    server.shutdown();
    return bound ? 0 : 1;
}