/**
 * Convert extracted Python API JSON to MDX pages for FumaDocs.
 *
 * Reads a JSON file produced by scripts/generate_sdk_docs.py (griffe extraction),
 * then uses fumadocs-python's convert() + write() to generate MDX pages
 * with Python-specific React components.
 *
 * Usage:
 *   node docs/scripts/convert-sdk-docs.mjs [input.json] [output-dir] [--base-url URL]
 *
 * Defaults:
 *   input:  docs/.generated/sdk-api.json
 *   output: docs/content/docs/reference/python/
 *   base-url: "" (app-relative; Next.js basePath adds /docs at build time)
 *
 * Must be run from docs/ directory (or with docs/node_modules visible).
 */

import { existsSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { convert, write } from "fumadocs-python";

const __dirname = dirname(fileURLToPath(import.meta.url));
const docsRoot = resolve(__dirname, "..");

const args = process.argv.slice(2);
const baseUrlIdx = args.indexOf("--base-url");
const baseUrl = baseUrlIdx !== -1 ? args[baseUrlIdx + 1] : "";

// Filter out --base-url and its value from positional args
const positional =
  baseUrlIdx === -1
    ? args
    : args.filter((_, i) => i !== baseUrlIdx && i !== baseUrlIdx + 1);

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

const rootModule = mod.name;
if (!rootModule) {
  console.error("Error: input model has no top-level module name.");
  process.exit(1);
}

const normalizedBase = baseUrl.replace(/\/+$/, "");
const apiPrefix = `${normalizedBase}/reference/python`;

console.log(`Converting ${rootModule} API to MDX...`);
const generatedFiles = convert(mod, { baseUrl: apiPrefix, groupBy: "none" });
const files = deduplicateFiles(generatedFiles);
console.log(
  `Generated ${files.length} unique MDX file(s) from ${generatedFiles.length} page entries`,
);
const descriptions = collectPageDescriptions(mod, files);

// Clean previous output
if (existsSync(outputDir)) {
  await rm(outputDir, { recursive: true });
}
await mkdir(outputDir, { recursive: true });

await write(files, outputDir);
await addDescriptionFrontmatter(files, descriptions, outputDir);

// Generate meta.json files for FumaDocs sidebar navigation
await generateMetaFiles(files, outputDir);

console.log(`Wrote ${files.length} MDX files + meta.json to ${outputDir}`);

function deduplicateFiles(files) {
  const byPath = new Map();
  for (const file of files) {
    const existing = byPath.get(file.path);
    if (
      existing &&
      (existing.title !== file.title || existing.content !== file.content)
    ) {
      throw new Error(`Conflicting generated pages for path ${file.path}`);
    }
    byPath.set(file.path, file);
  }
  return [...byPath.values()];
}

function collectPageDescriptions(root, files) {
  const descriptions = new Map();
  const rootPath = root.path;
  const pagePath = (path) =>
    path.slice(rootPath.length + 1).replaceAll(".", "/");
  const describe = (description, sections, fallback) => {
    const text =
      description || sections?.find((item) => item.kind === "text")?.value;
    return (text || fallback).replace(/\s+/g, " ").trim();
  };

  function visit(module) {
    const relativePath = module.path === rootPath ? "" : pagePath(module.path);
    const hasChildren =
      Object.keys(module.classes ?? {}).length > 0 ||
      Object.keys(module.modules ?? {}).length > 0;
    const moduleFile = relativePath
      ? hasChildren
        ? `${relativePath}/index.mdx`
        : `${relativePath}.mdx`
      : "index.mdx";
    descriptions.set(
      moduleFile,
      describe(module.description, module.docstring, module.name),
    );

    for (const cls of Object.values(module.classes ?? {})) {
      descriptions.set(
        `${pagePath(cls.path)}.mdx`,
        describe(cls.description, cls.docstring, cls.name),
      );
    }
    for (const child of Object.values(module.modules ?? {})) visit(child);
  }

  visit(root);
  return new Map(
    files.map((file) => [file.path, descriptions.get(file.path) || file.title]),
  );
}

async function addDescriptionFrontmatter(files, descriptions, outDir) {
  for (const file of files) {
    const path = resolve(outDir, file.path);
    const content = await readFile(path, "utf-8");
    const separator = content.indexOf("\n---\n");
    if (separator < 0) throw new Error(`Missing frontmatter in ${file.path}`);
    const frontmatter = content.slice(0, separator);
    const body = content.slice(separator + 5);
    await writeFile(
      path,
      `${frontmatter}\ndescription: ${JSON.stringify(descriptions.get(file.path))}\n---\n${body}`,
    );
  }
}

/**
 * Generate meta.json files for the reference section sidebar.
 *
 * fumadocs-python 1.x paths are already relative to the generated content
 * directory when groupBy is "none".
 */
async function generateMetaFiles(files, outDir) {
  // Build directory tree from stripped paths
  const dirs = new Map();

  for (const file of files) {
    const filePath = file.path;
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
    const ordered = pages.filter((p) => p !== "index");

    const metaPath = resolve(outDir, dirPath, "meta.json");
    await mkdir(dirname(metaPath), { recursive: true });
    await writeJson(metaPath, { title: dirName, pages: ordered });
  }

  // Write top-level python/ meta.json
  const topChildren = dirs.get(".") || new Set();
  const topPages = Array.from(topChildren).sort();
  const ordered = topPages.filter((p) => p !== "index");

  await writeJson(resolve(outDir, "meta.json"), {
    title: "Python SDK",
    defaultOpen: false,
    pages: ordered,
  });

  // Note: no reference/meta.json needed — the top-level meta.json
  // references "reference/python" directly to avoid redundant nesting.
}

/** Write `value` as pretty-printed JSON with a trailing newline. */
async function writeJson(path, value) {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
