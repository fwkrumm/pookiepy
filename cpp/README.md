# C++ server

`pookiecpp::BaseServer` implements bundled `Stream.DataChannel` wire protocol using
gRPC callback reactors. Subclass it and override public virtual hooks; call
`start()` for nonblocking startup, `serve_forever()` for blocking startup, and
`shutdown()` from a non-callback thread. `on_init()` runs during `start()` (C++
virtual dispatch does not work from base constructors). `on_shutdown()` runs once
after gRPC has stopped. Hooks run on gRPC callback threads: keep them short; avoid
blocking on server shutdown or acquiring application locks in reverse order.
`shutdown()` gives active streams one second to finish by default, then cancels
them; set `ServerConfig::shutdown_grace_period` to change that deadline.

```cpp
class Relay final : public pookiecpp::BaseServer {
public:
    using BaseServer::BaseServer;

    bool on_receive(const pookiecpp::Peer& peer, pookiecpp::Message& message) override {
        return message.metainfo().messagename() != "ignore";
    }
};
```

Default schema version is `pookiepy.schema.v0`, matching bundled Python
interface. Override `ServerConfig::schema_version` for custom versions (empty
only when both peers deliberately disable validation). Initial client message
must include nonempty `metaInfo.clientInfo.uuid`; accepted peers receive welcome
before topic data. Duplicate UUIDs are rejected. Subsequent messages reach
subscribers by `messageName`, excluding sender. One write per stream is in flight;
payload is shared across subscriber queues, and slow consumers with full queues
drop incoming messages. Set `max_queue_elements` to bound per-client memory;
zero means unlimited. Callbacks can edit inbound messages before routing.

Build from repo root, with uv and Conan 2 installed in project venv:

```powershell
uv pip install -r requirements-cpp.txt
uv run --no-sync conan profile detect --exist-ok
uv run --no-sync conan install . --output-folder=build --build=missing -s build_type=Release -s compiler.cppstd=17 -c tools.cmake.cmaketoolchain:generator=Ninja
uv run --no-sync conan build . --output-folder=build -s build_type=Release -s compiler.cppstd=17 -c tools.cmake.cmaketoolchain:generator=Ninja
ctest --test-dir build/build/Release --output-on-failure
```

Requires working C++17 compiler, CMake, and Conan Center access. `conan install`
may compile native dependencies locally if matching binaries are unavailable.
To verify the distributable package and run the same tests as CI, use:

```powershell
uv run --no-sync conan create . --build=missing -s build_type=Release -s compiler.cppstd=17 -c tools.cmake.cmaketoolchain:generator=Ninja
```

`conan create` compiles the library, runs CTest (failure blocks packaging), and
installs the library and generated/public headers into the Conan package. It
then builds and runs an external consumer against the installed package.
This server uses bundled proto; Python custom `ProtoInterface`, static data,
compression configuration, and server-originated broadcasts are not supported.