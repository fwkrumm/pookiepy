const { spawnSync } = require("node:child_process");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const pluginPath = path.join(root, "node_modules", ".bin", "protoc-gen-ts.cmd");
const protocBin = path.join(root, "node_modules", ".bin", "grpc_tools_node_protoc.cmd");

const args = [
  "--js_out=import_style=commonjs,binary:./src/generated",
  "--grpc_out=grpc_js:./src/generated",
  `--plugin=protoc-gen-ts=${pluginPath}`,
  "--ts_out=grpc_js:./src/generated",
  "-I",
  "..",
  "../pookiepy/message.proto"
];

const result = spawnSync(protocBin, args, {
  cwd: root,
  stdio: "inherit",
  shell: true
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
