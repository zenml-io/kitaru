import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const converterPath = fileURLToPath(
  new URL("./convert-sdk-docs.mjs", import.meta.url),
);

test("converts 1.x output into stable paths, links, frontmatter, and sidebar", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "kitaru-sdk-docs-"));
  const inputPath = join(tempDir, "api.json");
  const outputDir = join(tempDir, "output");
  const model = {
    name: "kitaru",
    path: "kitaru",
    description: "Kitaru SDK summary.",
    docstring: [],
    modules: {
      client: {
        name: "client",
        path: "kitaru.client",
        description: "Client API summary.",
        docstring: [],
        modules: {},
        attributes: [],
        classes: {
          KitaruClient: {
            name: "KitaruClient",
            path: "kitaru.client.KitaruClient",
            description: "Creates and uses a Kitaru client.",
            docstring: [],
            parameters: [],
            attributes: [],
            functions: {},
            source: "",
            inherited_members: {},
          },
        },
        functions: {},
      },
      single: {
        name: "single",
        path: "kitaru.single",
        description: "A leaf module.",
        docstring: [],
        modules: {},
        attributes: [],
        classes: {},
        functions: {},
      },
    },
    attributes: [],
    classes: {},
    functions: {},
  };

  try {
    await writeFile(inputPath, JSON.stringify(model));
    execFileSync(process.execPath, [converterPath, inputPath, outputDir], {
      stdio: "pipe",
    });

    const rootPage = await readFile(join(outputDir, "index.mdx"), "utf-8");
    const clientPage = await readFile(
      join(outputDir, "client", "index.mdx"),
      "utf-8",
    );
    const clientClassPage = await readFile(
      join(outputDir, "client", "KitaruClient.mdx"),
      "utf-8",
    );
    const leafModulePage = await readFile(
      join(outputDir, "single.mdx"),
      "utf-8",
    );
    const rootMeta = JSON.parse(
      await readFile(join(outputDir, "meta.json"), "utf-8"),
    );
    const clientMeta = JSON.parse(
      await readFile(join(outputDir, "client", "meta.json"), "utf-8"),
    );

    assert.match(rootPage, /description: "Kitaru SDK summary\."/);
    assert.match(rootPage, /href="\/reference\/python\/client"/);
    assert.match(clientPage, /description: "Client API summary\."/);
    assert.match(
      clientPage,
      /href="\/reference\/python\/client\/KitaruClient"/,
    );
    assert.match(
      clientClassPage,
      /description: "Creates and uses a Kitaru client\."/,
    );
    assert.match(leafModulePage, /description: "A leaf module\."/);
    assert.deepEqual(rootMeta.pages, ["client", "single"]);
    assert.deepEqual(clientMeta.pages, ["KitaruClient"]);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});
