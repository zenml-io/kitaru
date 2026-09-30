import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open, readdir } from "node:fs/promises";
import { join, posix } from "node:path";
import type { SkillSource, SkillSourceEntry } from "@mastra/core/workspace";
import { MAX_RECORDED_PAYLOAD_CHARS } from "@zenml-io/kitaru/adapter";

export interface SkillsManifest {
  files: { path: string; length: number; sha256: string }[];
  directories: string[];
}

/** Read and pin an artifact's skills. Native tools subsequently read only these bytes. */
export async function loadSkillsWorkspace(
  skillsDirectory: string,
  expectedManifest?: SkillsManifest,
) {
  const { Workspace } = await import("@mastra/core/workspace");
  const files = new Map<string, Buffer>();
  const directories = new Map<string, SkillSourceEntry[]>();
  let size = 0;
  async function visit(relative: string): Promise<void> {
    const path = join(skillsDirectory, relative);
    const info = await lstat(path);
    if (info.isSymbolicLink())
      throw new Error("Unsupported Mastra skills symlink");
    if (info.isDirectory()) {
      const entries = await readdir(path, { withFileTypes: true });
      entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      directories.set(
        relative,
        entries.map((entry) => ({
          name: entry.name,
          type: entry.isDirectory() ? "directory" : "file",
        })),
      );
      for (const entry of entries)
        await visit(posix.join(relative, entry.name));
    } else if (info.isFile()) {
      if (size + info.size > MAX_RECORDED_PAYLOAD_CHARS)
        throw new Error("Skills content exceeds the supported replay limit");
      // O_NOFOLLOW closes the gap between lstat and the read if a file is swapped.
      const handle = await open(
        path,
        constants.O_RDONLY | constants.O_NOFOLLOW,
      );
      let content: Buffer;
      try {
        if (!(await handle.stat()).isFile())
          throw new Error("Unsupported Mastra skills file type");
        content = await handle.readFile();
      } finally {
        await handle.close();
      }
      size += content.length;
      files.set(relative, content);
    } else throw new Error("Unsupported Mastra skills file type");
    if (
      files.size + directories.size > 10_000 ||
      size > MAX_RECORDED_PAYLOAD_CHARS
    )
      throw new Error("Skills content exceeds the supported replay limit");
  }
  await visit(".");
  const manifest: SkillsManifest = {
    files: [...files].map(([path, content]) => ({
      path,
      length: content.length,
      sha256: createHash("sha256").update(content).digest("hex"),
    })),
    directories: [...directories.keys()].sort(),
  };
  if (
    expectedManifest &&
    JSON.stringify(expectedManifest) !== JSON.stringify(manifest)
  )
    throw new Error(
      "Unsupported Mastra memory replay: skills artifact changed.",
    );
  function normalize(path: string): string {
    const normalized = posix.normalize(path.replaceAll("\\", "/"));
    if (
      posix.isAbsolute(normalized) ||
      normalized === ".." ||
      normalized.startsWith("../")
    )
      throw new Error("Skill path is outside the pinned artifact");
    return normalized;
  }
  const source: SkillSource = {
    async exists(path) {
      const key = normalize(path);
      return files.has(key) || directories.has(key);
    },
    async stat(path) {
      const key = normalize(path);
      const directory = directories.has(key);
      const file = files.get(key);
      if (!directory && !file)
        throw new Error("Skill path is absent from pinned artifact");
      return {
        name: posix.basename(key),
        type: directory ? "directory" : "file",
        size: file?.length ?? 0,
        createdAt: new Date(0),
        modifiedAt: new Date(0),
      };
    },
    async readFile(path) {
      const file = files.get(normalize(path));
      if (!file) throw new Error("Skill file is absent from pinned artifact");
      return Buffer.from(file);
    },
    async readdir(path) {
      const entries = directories.get(normalize(path));
      if (!entries)
        throw new Error("Skill directory is absent from pinned artifact");
      return entries.map((entry) => ({ ...entry }));
    },
    async realpath(path) {
      return normalize(path);
    },
  };
  return {
    manifest,
    workspace: new Workspace({ skills: ["."], skillSource: source }),
  };
}
