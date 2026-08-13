const fs = require("node:fs");
const path = require("node:path");

const generatedDir = path.join(__dirname, "..", "src", "generated");

if (!fs.existsSync(generatedDir)) {
  process.exit(0);
}

for (const entry of fs.readdirSync(generatedDir)) {
  const fullPath = path.join(generatedDir, entry);
  if (fs.statSync(fullPath).isFile()) {
    fs.unlinkSync(fullPath);
  }
}
