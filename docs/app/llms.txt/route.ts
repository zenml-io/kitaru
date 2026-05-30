import { getMarkdownUrl, source } from "@/lib/source";

export const revalidate = false;

type DocsPage = ReturnType<typeof source.getPages>[number];
type SectionKey = "gettingStarted" | "guides" | "apiReference" | "optional";

type Section = {
  key: SectionKey;
  title: string;
  description: string;
};

type Link = {
  title: string;
  url: string;
  description: string;
};

type PageEntry = {
  page: DocsPage;
  section: SectionKey;
  sortKey: [number, number, number, string];
};

const PRODUCT_DESCRIPTION =
  "Kitaru is the runtime layer underneath your agent stack: it gives Python AI workflows durable checkpoints, replay, resumable waits, tracked LLM calls, artifact lineage, deployment, and operational tooling.";

const SECTIONS: Section[] = [
  {
    key: "gettingStarted",
    title: "Getting Started",
    description:
      "Install Kitaru, run your first durable flow, and learn the first production path.",
  },
  {
    key: "guides",
    title: "Guides",
    description:
      "Task-focused guides and conceptual explanations for building durable agent workflows.",
  },
  {
    key: "apiReference",
    title: "API Reference",
    description:
      "CLI and Python reference material for exact commands, functions, and types.",
  },
  {
    key: "optional",
    title: "Optional",
    description:
      "Lower-priority project material, exhaustive generated pages, and release history.",
  },
];

const SECTION_ORDER = new Map(
  SECTIONS.map((section, index) => [section.key, index]),
);

const TOP_LEVEL_ORDER = new Map([
  ["", 0],
  ["getting-started", 10],
  ["concepts", 20],
  ["guides", 30],
  ["deploy", 40],
  ["stacks", 50],
  ["agent-native", 60],
  ["cli", 70],
  ["reference", 80],
  ["contributing", 90],
  ["changelog", 100],
]);

const FALLBACK_API_REFERENCE_LINKS: Link[] = [
  {
    title: "API Reference",
    url: "/docs/api.md",
    description: "High-level API reference entry point for Kitaru docs.",
  },
  {
    title: "Reference",
    url: "/docs/reference.md",
    description:
      "Reference entry point for commands, SDK APIs, and generated reference material.",
  },
];

const PAGE_ORDER = new Map([
  ["/", 0],
  ["/getting-started/installation", 10],
  ["/getting-started/quickstart", 20],
  ["/getting-started/deploy", 30],
  ["/getting-started/examples", 40],
  ["/getting-started/troubleshooting", 50],
  ["/concepts", 10],
  ["/concepts/harness-runtime-platform", 20],
  ["/concepts/how-it-works", 30],
  ["/concepts/flows", 40],
  ["/concepts/checkpoints", 50],
  ["/guides/configuration", 10],
  ["/guides/authentication", 20],
  ["/guides/deployments", 30],
  ["/guides/llm-calls", 40],
  ["/guides/artifacts", 50],
  ["/guides/wait-and-resume", 60],
  ["/guides/execution-management", 70],
  ["/cli", 10],
  ["/reference/python", 20],
]);

function sectionForPage(page: DocsPage): SectionKey {
  const [topLevel, secondLevel] = page.slugs;

  if (page.slugs.length === 0 || topLevel === "getting-started") {
    return "gettingStarted";
  }

  if (topLevel === "cli") {
    return "apiReference";
  }

  if (topLevel === "reference") {
    // Keep the main Python API landing page prominent. Deep generated API pages
    // are still useful for agents, but they are better treated as optional
    // exhaustive material rather than the top-level map through the docs.
    return secondLevel === "python" && page.slugs.length <= 2
      ? "apiReference"
      : "optional";
  }

  if (topLevel === "contributing" || topLevel === "changelog") {
    return "optional";
  }

  return "guides";
}

function pageSortKey(
  page: DocsPage,
  section: SectionKey,
): PageEntry["sortKey"] {
  const sectionIndex = SECTION_ORDER.get(section) ?? SECTIONS.length;
  const slugPath = page.url;
  const topLevel = page.slugs[0] ?? "";

  return [
    sectionIndex,
    TOP_LEVEL_ORDER.get(topLevel) ?? 1_000,
    PAGE_ORDER.get(slugPath) ?? 1_000,
    slugPath,
  ];
}

function pageEntry(page: DocsPage): PageEntry {
  const section = sectionForPage(page);
  return {
    page,
    section,
    sortKey: pageSortKey(page, section),
  };
}

function comparePageEntries(left: PageEntry, right: PageEntry) {
  const [leftSection, leftTopLevel, leftPage, leftUrl] = left.sortKey;
  const [rightSection, rightTopLevel, rightPage, rightUrl] = right.sortKey;

  return (
    leftSection - rightSection ||
    leftTopLevel - rightTopLevel ||
    leftPage - rightPage ||
    leftUrl.localeCompare(rightUrl)
  );
}

function pageLink(page: DocsPage): Link {
  return {
    title: page.data.title,
    url: getMarkdownUrl(page),
    description: page.data.description ?? "Kitaru documentation page.",
  };
}

function linkLine(link: Link) {
  return `- [${link.title}](${link.url}): ${link.description}`;
}

function sectionPages(
  pagesBySection: Map<SectionKey, DocsPage[]>,
  section: SectionKey,
) {
  const pages = pagesBySection.get(section);
  if (!pages) {
    throw new Error(`Unknown llms.txt section: ${section}`);
  }

  return pages;
}

export async function GET() {
  const pagesBySection = new Map<SectionKey, DocsPage[]>(
    SECTIONS.map((section) => [section.key, []]),
  );

  for (const { page, section } of source
    .getPages()
    .map(pageEntry)
    .sort(comparePageEntries)) {
    sectionPages(pagesBySection, section).push(page);
  }

  const lines: string[] = [
    "# Kitaru Documentation",
    "",
    `> ${PRODUCT_DESCRIPTION}`,
    "",
  ];

  for (const section of SECTIONS) {
    const pages = sectionPages(pagesBySection, section.key);
    const links = pages.map(pageLink);

    if (section.key === "apiReference" && links.length === 0) {
      // The docs build can materialize shallow API/reference markdown aliases
      // after Next renders this route. Keep the llms.txt map useful even when
      // generated API pages are absent from source.getPages().
      links.push(...FALLBACK_API_REFERENCE_LINKS);
    }

    if (links.length === 0) continue;

    lines.push(`## ${section.title}`);
    lines.push("");
    lines.push(section.description);
    lines.push("");
    lines.push(...links.map(linkLine));
    lines.push("");
  }

  return new Response(`${lines.join("\n").trimEnd()}\n`, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
    },
  });
}
