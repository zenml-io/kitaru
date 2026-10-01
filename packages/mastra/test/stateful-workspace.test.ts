import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { loadSkillsWorkspace } from "../src/stateful-workspace.js";

it("hashes skills by content and refuses changed content or symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "kitaru-memory-skills-"));
  try {
    await mkdir(join(root, "triage"));
    const path = join(root, "triage", "SKILL.md");
    await writeFile(
      path,
      "---\nname: triage\ndescription: Support triage\n---\nHistorical instructions.\n",
    );
    const baseline = await loadSkillsWorkspace(root);
    expect(baseline.manifest.files[0]?.path).toBe("triage/SKILL.md");
    expect(baseline.manifest.directories).toEqual([".", "triage"]);
    expect(await baseline.workspace.skills?.list()).toEqual(
      expect.arrayContaining([expect.objectContaining({ name: "triage" })]),
    );
    expect(
      (await loadSkillsWorkspace(root, baseline.manifest)).manifest,
    ).toEqual(baseline.manifest);
    await mkdir(join(root, "extra"));
    await expect(loadSkillsWorkspace(root, baseline.manifest)).rejects.toThrow(
      /changed/,
    );
    await rm(join(root, "extra"), { recursive: true });
    await writeFile(join(root, "triage", "new.md"), "new instructions");
    await expect(loadSkillsWorkspace(root, baseline.manifest)).rejects.toThrow(
      /changed/,
    );
    await rm(join(root, "triage", "new.md"));
    await writeFile(path, "changed");
    expect(
      JSON.stringify(await baseline.workspace.skills?.get("triage")),
    ).toContain("Historical instructions.");
    await expect(loadSkillsWorkspace(root, baseline.manifest)).rejects.toThrow(
      /changed/,
    );
    await symlink(path, join(root, "linked.md"));
    await expect(loadSkillsWorkspace(root)).rejects.toThrow(/symlink/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
