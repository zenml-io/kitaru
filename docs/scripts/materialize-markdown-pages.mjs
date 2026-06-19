import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const docsRoot = path.resolve(scriptDir, "..");
const outDir = path.join(docsRoot, "out");
const hiddenDocsDir = path.join(outDir, "llms.mdx", "docs");

async function fileExists(filePath) {
  try {
    const fileStat = await stat(filePath);
    return fileStat.isFile();
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

async function collectHiddenMarkdownFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const nestedFiles = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map((entry) => collectHiddenMarkdownFiles(path.join(dir, entry.name))),
  );

  const files = entries
    .filter((entry) => entry.isFile() && entry.name === "index.mdx")
    .map((entry) => path.join(dir, entry.name));

  return [...files, ...nestedFiles.flat()];
}

function canonicalMarkdownPath(hiddenMarkdownPath) {
  const relativePath = path.relative(hiddenDocsDir, hiddenMarkdownPath);
  const segments = relativePath.split(path.sep);

  if (segments.at(-1) !== "index.mdx") {
    throw new Error(`Unexpected hidden markdown path: ${hiddenMarkdownPath}`);
  }

  const pageSegments = segments.slice(0, -1);
  if (pageSegments.length === 0) {
    return path.join(outDir, "index.md");
  }

  return path.join(
    outDir,
    ...pageSegments.slice(0, -1),
    `${pageSegments.at(-1)}.md`,
  );
}

function looksLikeMarkdown(content) {
  const trimmed = content.trimStart();
  return (
    trimmed.startsWith("# ") ||
    trimmed.startsWith("## ") ||
    trimmed.startsWith("---")
  );
}

function rewritePublicMarkdownLinks(content) {
  // The site is served at the domain root (no /docs basePath), so root-relative
  // links in the materialized markdown are already correct — no rewriting.
  return content;
}

async function copyMarkdownFile(sourcePath, destinationPath) {
  const content = rewritePublicMarkdownLinks(
    await readFile(sourcePath, "utf8"),
  );
  if (!looksLikeMarkdown(content)) {
    throw new Error(
      `Refusing to materialize non-markdown content from ${sourcePath}`,
    );
  }

  await mkdir(path.dirname(destinationPath), { recursive: true });
  await writeFile(destinationPath, content, "utf8");
}

async function copyAlias(aliasPath, candidatePaths) {
  const absoluteAliasPath = path.join(outDir, aliasPath);
  if (await fileExists(absoluteAliasPath)) {
    return { aliasPath, sourcePath: undefined, skipped: true };
  }

  for (const candidatePath of candidatePaths) {
    const absoluteCandidatePath = path.join(outDir, candidatePath);
    if (await fileExists(absoluteCandidatePath)) {
      await copyMarkdownFile(absoluteCandidatePath, absoluteAliasPath);
      return { aliasPath, sourcePath: candidatePath, skipped: false };
    }
  }

  // Legacy aliases are best-effort: their target pages may no longer exist
  // (e.g. on the reference-only site). Skip rather than fail the build.
  console.warn(
    `skipped ${aliasPath}; none of these candidates exist: ${candidatePaths.join(", ")}`,
  );
  return { aliasPath, sourcePath: undefined, skipped: true };
}

async function main() {
  if (!(await fileExists(path.join(hiddenDocsDir, "index.mdx")))) {
    throw new Error(
      `Expected hidden markdown export at ${hiddenDocsDir}. Run this script after Next's static export finishes.`,
    );
  }

  const hiddenMarkdownFiles = await collectHiddenMarkdownFiles(hiddenDocsDir);
  await Promise.all(
    hiddenMarkdownFiles.map((hiddenMarkdownPath) =>
      copyMarkdownFile(
        hiddenMarkdownPath,
        canonicalMarkdownPath(hiddenMarkdownPath),
      ),
    ),
  );

  // These legacy/sample aliases are intentionally shallow. Some are not real
  // docs slugs today, but external crawlers benchmark these exact URLs. Each
  // alias points at the closest shipped page and is only filled in when no real
  // canonical page already produced that filename.
  const aliases = [
    {
      aliasPath: "quickstart.md",
      candidatePaths: ["getting-started/quickstart.md"],
    },
    {
      aliasPath: "getting-started.md",
      candidatePaths: [
        "getting-started.md",
        "getting-started/installation.md",
        "getting-started/quickstart.md",
      ],
    },
    {
      aliasPath: "guide.md",
      candidatePaths: [
        "guides.md",
        "guides/configuration.md",
        "guides/deployments.md",
      ],
    },
    {
      aliasPath: "api.md",
      candidatePaths: [
        "reference/python.md",
        "reference.md",
        "cli.md",
        "index.md",
      ],
    },
    {
      aliasPath: "reference.md",
      candidatePaths: [
        "reference.md",
        "reference/python.md",
        "cli.md",
        "index.md",
      ],
    },
  ];

  const aliasResults = await Promise.all(
    aliases.map((alias) => copyAlias(alias.aliasPath, alias.candidatePaths)),
  );

  console.log(
    `Materialized ${hiddenMarkdownFiles.length} markdown docs pages in ${outDir}`,
  );
  for (const result of aliasResults) {
    if (result.skipped) {
      console.log(`kept existing ${result.aliasPath}`);
      continue;
    }

    console.log(`created ${result.aliasPath} from ${result.sourcePath}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
