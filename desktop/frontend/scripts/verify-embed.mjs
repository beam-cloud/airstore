import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

function fail(message) {
  console.error(`embed verification failed: ${message}`);
  process.exit(1);
}

const distDir = process.argv[2];
if (!distDir) {
  fail("usage: node verify-embed.mjs <dist-dir>");
}

let html = "";
try {
  html = readFileSync(join(distDir, "index.html"), "utf8");
} catch (err) {
  fail(`cannot read index.html (${String(err)})`);
}

const matches = [...html.matchAll(/(?:src|href)=["'](\/assets\/[^"']+)["']/g)];
if (matches.length === 0) {
  fail("index.html references no /assets/* files");
}

for (const match of matches) {
  const urlPath = match[1];
  const relPath = urlPath.replace(/^\//, "");
  const absPath = join(distDir, relPath);
  try {
    const stats = statSync(absPath);
    if (!stats.isFile() || stats.size === 0) {
      fail(`asset exists but is invalid: ${urlPath}`);
    }
  } catch (err) {
    fail(`missing asset referenced by index.html: ${urlPath} (${String(err)})`);
  }
}

console.log(`embed verification passed (${matches.length} asset references)`);
