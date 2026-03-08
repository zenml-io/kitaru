/**
 * Convert extracted Python API JSON to MDX pages for FumaDocs.
 *
 * Reads a JSON file produced by scripts/generate_sdk_docs.py (griffe extraction),
 * then uses fumadocs-python's convert() + write() to generate MDX pages
 * with Python-specific React components.
 *
 * Usage:
 *   node docs/scripts/convert-sdk-docs.mjs [input.json] [output-dir] [--base-url /docs]
 *
 * Defaults:
 *   input:  docs/.generated/sdk-api.json
 *   output: docs/content/docs/reference/python/
 *
 * Must be run from docs/ directory (or with docs/node_modules visible).
 */

import { existsSync } from "node:fs";
import { readFile, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { convert, write } from "fumadocs-python";

const __dirname = dirname(fileURLToPath(import.meta.url));
const docsRoot = resolve(__dirname, "..");

const args = process.argv.slice(2);
const baseUrlIdx = args.indexOf("--base-url");
const baseUrl = baseUrlIdx !== -1 ? args[baseUrlIdx + 1] : "/docs";

// Filter out --base-url and its value from positional args
const positional = args.filter(
  (_, i) => i !== baseUrlIdx && i !== baseUrlIdx + 1,
);

const inputPath = positional[0] || resolve(docsRoot, ".generated/sdk-api.json");
const outputDir =
  positional[1] || resolve(docsRoot, "content/docs/reference/python");

if (!existsSync(inputPath)) {
  console.log(
    `No SDK API JSON found at ${inputPath}. Skipping MDX conversion.`,
  );
  console.log(
    "  Run 'uv run python scripts/generate_sdk_docs.py' first to extract the API.",
  );
  process.exit(0);
}

const raw = await readFile(inputPath, "utf-8");
const mod = JSON.parse(raw);

console.log(`Converting ${mod.name} API to MDX...`);
const files = convert(mod, { baseUrl: `${baseUrl}/reference/python` });
console.log(`Generated ${files.length} MDX file(s)`);

// Flatten singleton module directories into flat files before writing.
// This prevents redundant sidebar nesting (e.g. "artifacts" → "artifacts").
flattenSingletonPaths(files);

// Clean previous output
if (existsSync(outputDir)) {
  await rm(outputDir, { recursive: true });
}
await mkdir(outputDir, { recursive: true });

await write(files, { outDir: outputDir });

// Generate meta.json files for FumaDocs sidebar navigation
await generateMetaFiles(files, outputDir);

console.log(`Wrote ${files.length} MDX files + meta.json to ${outputDir}`);

/**
 * Flatten singleton module directories into flat files.
 *
 * When a module directory contains only its own index.mdx and no other
 * descendants, rewrite its path from `root/module/index.mdx` to
 * `root/module.mdx`. This prevents redundant sidebar nesting where
 * clicking "artifacts" just reveals another "artifacts" entry.
 *
 * Mutates file objects in place.
 */
function flattenSingletonPaths(files) {
  // Group files by their directory (after stripping the root module prefix,
  // matching what write() does with .slice(1))
  const dirContents = new Map();
  for (const f of files) {
    const parts = f.path.split("/").slice(1); // strip root module
    const dir = parts.slice(0, -1).join("/") || ".";
    if (!dirContents.has(dir)) {
      dirContents.set(dir, []);
    }
    dirContents.get(dir).push(f);
  }

  // A directory is flattenable if:
  // 1. It contains exactly one file (its own index.mdx)
  // 2. No other directory is nested under it
  const allDirs = new Set(dirContents.keys());

  for (const [dir, dirFiles] of dirContents) {
    if (dir === ".") continue;
    if (dirFiles.length !== 1) continue;

    const file = dirFiles[0];
    const strippedPath = file.path.split("/").slice(1).join("/");
    if (!strippedPath.endsWith("/index.mdx")) continue;

    // Check no nested subdirectories exist under this dir
    const hasNestedDirs = [...allDirs].some(
      (d) => d !== dir && d.startsWith(dir + "/"),
    );
    if (hasNestedDirs) continue;

    // Flatten: root/module/index.mdx → root/module.mdx
    const rootPrefix = file.path.split("/")[0];
    const moduleName = dir.split("/").pop();
    const parentDir = dir.split("/").slice(0, -1).join("/");
    const newPath = parentDir
      ? `${rootPrefix}/${parentDir}/${moduleName}.mdx`
      : `${rootPrefix}/${moduleName}.mdx`;
    file.path = newPath;
  }
}

/**
 * Generate meta.json files for the reference section sidebar.
 *
 * fumadocs-python's write() strips the root module name from file paths
 * (using .slice(1)), so we must do the same when computing directory
 * structure for meta.json files.
 */
async function generateMetaFiles(files, outDir) {
  // Apply the same path stripping that write() does: remove root module prefix
  const strippedPaths = files.map((f) =>
    f.path.split("/").slice(1).join("/"),
  );

  // Build directory tree from stripped paths
  const dirs = new Map();

  for (const filePath of strippedPaths) {
    const parts = filePath.split("/");
    const fileName = parts.pop().replace(/\.mdx$/, "");
    const dirPath = parts.join("/") || ".";

    if (!dirs.has(dirPath)) {
      dirs.set(dirPath, new Set());
    }
    dirs.get(dirPath).add(fileName);

    // Register intermediate directories
    for (let i = 1; i < parts.length; i++) {
      const parentPath = parts.slice(0, i).join("/") || ".";
      const childName = parts[i];
      if (!dirs.has(parentPath)) {
        dirs.set(parentPath, new Set());
      }
      dirs.get(parentPath).add(childName);
    }

    // Register top-level entries for subdirectories
    if (parts.length > 0) {
      if (!dirs.has(".")) {
        dirs.set(".", new Set());
      }
      dirs.get(".").add(parts[0]);
    }
  }

  // Write meta.json for each subdirectory
  for (const [dirPath, children] of dirs) {
    if (dirPath === ".") continue;

    const dirName = dirPath.split("/").pop();
    const pages = Array.from(children).sort();
    const hasIndex = pages.includes("index");
    const ordered = hasIndex
      ? ["index", ...pages.filter((p) => p !== "index")]
      : pages;

    const meta = { title: dirName, pages: ordered };
    const metaPath = resolve(outDir, dirPath, "meta.json");
    await mkdir(dirname(metaPath), { recursive: true });
    await writeFile(metaPath, JSON.stringify(meta, null, 2) + "\n");
  }

  // Write top-level python/ meta.json
  const topChildren = dirs.get(".") || new Set();
  const topPages = Array.from(topChildren).sort();
  const hasIndex = topPages.includes("index");
  const ordered = hasIndex
    ? ["index", ...topPages.filter((p) => p !== "index")]
    : topPages;

  await writeFile(
    resolve(outDir, "meta.json"),
    JSON.stringify(
      { title: "Python SDK", defaultOpen: true, pages: ordered },
      null,
      2,
    ) + "\n",
  );

  // Note: no reference/meta.json needed — the top-level meta.json
  // references "reference/python" directly to avoid redundant nesting.
}
