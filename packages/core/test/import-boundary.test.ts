import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { describe, expect, it } from "vitest";

import * as rootExports from "../src/index.js";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));

function isTypeOnlyImport(node: ts.ImportDeclaration): boolean {
  const clause = node.importClause;
  if (clause === undefined || clause.name !== undefined) {
    return clause?.isTypeOnly ?? false;
  }
  const bindings = clause.namedBindings;
  return (
    clause.isTypeOnly ||
    (bindings !== undefined &&
      ts.isNamedImports(bindings) &&
      bindings.elements.every((element) => element.isTypeOnly))
  );
}

function isTypeOnlyExport(node: ts.ExportDeclaration): boolean {
  return (
    node.isTypeOnly ||
    (node.exportClause !== undefined &&
      ts.isNamedExports(node.exportClause) &&
      node.exportClause.elements.every((element) => element.isTypeOnly))
  );
}

async function rootRuntimeImports(): Promise<Map<string, string[]>> {
  const imports = new Map<string, string[]>();
  const pending = [join(packageRoot, "src", "index.ts")];

  while (pending.length > 0) {
    const path = pending.pop();
    if (path === undefined || imports.has(path)) {
      continue;
    }
    const source = await readFile(path, "utf8");
    const sourceFile = ts.createSourceFile(
      path,
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TS,
    );
    const specifiers: string[] = [];

    function visit(node: ts.Node): void {
      if (
        ts.isImportDeclaration(node) &&
        !isTypeOnlyImport(node) &&
        ts.isStringLiteral(node.moduleSpecifier)
      ) {
        specifiers.push(node.moduleSpecifier.text);
      } else if (
        ts.isExportDeclaration(node) &&
        !isTypeOnlyExport(node) &&
        node.moduleSpecifier !== undefined &&
        ts.isStringLiteral(node.moduleSpecifier)
      ) {
        specifiers.push(node.moduleSpecifier.text);
      } else if (
        ts.isCallExpression(node) &&
        node.expression.kind === ts.SyntaxKind.ImportKeyword &&
        node.arguments.length === 1 &&
        ts.isStringLiteral(node.arguments[0] as ts.Expression)
      ) {
        specifiers.push((node.arguments[0] as ts.StringLiteral).text);
      }
      ts.forEachChild(node, visit);
    }

    visit(sourceFile);
    imports.set(path, specifiers);
    for (const specifier of specifiers) {
      if (specifier.startsWith(".")) {
        pending.push(join(dirname(path), specifier.replace(/\.js$/, ".ts")));
      }
    }
  }

  return imports;
}

describe("core import boundary", () => {
  it("keeps Node built-ins out of the complete root import graph", async () => {
    const imports = await rootRuntimeImports();
    const nodeImports = [...imports.entries()].flatMap(([path, specifiers]) =>
      specifiers
        .filter((specifier) => specifier.startsWith("node:"))
        .map((specifier) => `${path}: ${specifier}`),
    );

    expect(nodeImports).toEqual([]);
  });

  it("does not publish internal resource constructors from the package root", () => {
    expect(Object.keys(rootExports)).not.toEqual(
      expect.arrayContaining([
        "AccountsResource",
        "AgentsResource",
        "JobsResource",
        "ReplaysResource",
        "SessionsResource",
        "TasksResource",
      ]),
    );
  });

  it("does not depend on agent frameworks or model providers", async () => {
    const sourceRoot = join(packageRoot, "src");
    const sourceFiles = (
      await readdir(sourceRoot, {
        recursive: true,
      })
    ).filter((path) => path.endsWith(".ts"));
    const sources = await Promise.all(
      sourceFiles.map((path) => readFile(join(sourceRoot, path), "utf8")),
    );
    const manifest = JSON.parse(
      await readFile(join(packageRoot, "package.json"), "utf8"),
    ) as Record<string, Record<string, string> | undefined>;
    const dependencies = {
      ...manifest.dependencies,
      ...manifest.optionalDependencies,
      ...manifest.peerDependencies,
    };

    expect(sources.join("\n")).not.toMatch(
      /(?:from|import)\s*\(?["'](?:@mastra\/|@ai-sdk\/|ai(?:\/|["']))/,
    );
    expect(Object.keys(dependencies)).not.toContain("ai");
    expect(Object.keys(dependencies)).not.toEqual(
      expect.arrayContaining([
        expect.stringMatching(/^@mastra\//),
        expect.stringMatching(/^@ai-sdk\//),
      ]),
    );
  });
});
