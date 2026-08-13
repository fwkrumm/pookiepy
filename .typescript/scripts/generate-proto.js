const { spawnSync } = require("node:child_process");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const isWindows = process.platform === "win32";
const pluginPath = path.join(
  root,
  "node_modules",
  ".bin",
  isWindows ? "protoc-gen-ts.cmd" : "protoc-gen-ts"
);
const protocCli = require.resolve("grpc-tools/bin/protoc.js");

const args = [
  protocCli,
  "--js_out=import_style=commonjs,binary:./src/generated",
  "--grpc_out=grpc_js:./src/generated",
  `--plugin=protoc-gen-ts=${pluginPath}`,
  "--ts_out=grpc_js:./src/generated",
  "-I",
  "..",
  "../pookiepy/message.proto"
];

const result = spawnSync(process.execPath, args, {
  cwd: root,
  stdio: "inherit"
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
