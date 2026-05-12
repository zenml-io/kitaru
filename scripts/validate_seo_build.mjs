#!/usr/bin/env node

import { readdir, readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const SITE_ORIGIN = 'https://kitaru.ai';
// Keep this list in sync with the permanent redirect sources in site/worker/index.js.
const RETIRED_REDIRECTED_PATHS = new Set([
  '/docs/concepts/memory',
  '/docs/guides/memory',
  '/blog/kitaru-agents-now-have-memory',
]);
const BAD_BUILT_OUTPUT_SNIPPETS = [
  'https://kitaru.ai/docs/docs',
  '/docs/docs/cli',
  'https://kitaru.ai/og/docs',
];
const EXPECTED_ROBOTS_SITEMAPS = [
  'Sitemap: https://kitaru.ai/sitemap-index.xml',
  'Sitemap: https://kitaru.ai/docs/sitemap.xml',
];
const DOCS_ROBOTS_SITEMAP = 'Sitemap: https://kitaru.ai/docs/sitemap.xml';
const REPRESENTATIVE_HTML = [
  ['site root', 'index.html', 'https://kitaru.ai/'],
  ['blog index', 'blog/index.html', 'https://kitaru.ai/blog/'],
  [
    'blog article',
    'blog/no-journal-replay/index.html',
    'https://kitaru.ai/blog/no-journal-replay/',
  ],
  ['compare index', 'compare/index.html', 'https://kitaru.ai/compare/'],
  ['docs root', 'docs/index.html', 'https://kitaru.ai/docs/'],
  [
    'docs CLI page',
    'docs/cli/executions/index.html',
    'https://kitaru.ai/docs/cli/executions/',
  ],
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..');
const siteDist = path.join(repoRoot, 'site', 'dist');
const docsOut = path.join(repoRoot, 'docs', 'out');

const failures = [];
const checked = [];

function fail(message) {
  failures.push(message);
}

function pass(message) {
  checked.push(message);
}

async function assertDirectory(directory, label) {
  try {
    const info = await stat(directory);
    if (!info.isDirectory()) {
      fail(`${label} exists but is not a directory: ${directory}`);
      return false;
    }
  } catch {
    fail(`${label} is missing: ${directory}`);
    return false;
  }

  return true;
}

async function listFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(
    entries.map(async (entry) => {
      const fullPath = path.join(directory, entry.name);
      return entry.isDirectory() ? listFiles(fullPath) : [fullPath];
    }),
  );

  return files.flat();
}

async function readText(filePath) {
  return readFile(filePath, 'utf8');
}

function relativeToRepo(filePath) {
  return path.relative(repoRoot, filePath);
}

function extractXmlLocations(xml) {
  return [...xml.matchAll(/<loc>\s*([^<\s]+)\s*<\/loc>/g)].map(
    (match) => match[1],
  );
}

function hasFileExtension(pathname) {
  const lastSegment = pathname.split('/').filter(Boolean).at(-1) ?? '';
  return /\.[^/]+$/.test(lastSegment);
}

function normalizePathname(pathname) {
  if (pathname !== '/' && pathname.endsWith('/')) {
    return pathname.slice(0, -1);
  }

  return pathname;
}

async function validateSitemaps() {
  const sitemapFiles = (await listFiles(siteDist)).filter((filePath) =>
    path.basename(filePath).startsWith('sitemap') && filePath.endsWith('.xml'),
  );

  if (sitemapFiles.length === 0) {
    fail('No sitemap XML files found under site/dist.');
    return;
  }

  const badDirectoryUrls = [];
  const retiredUrls = [];

  for (const sitemapFile of sitemapFiles) {
    const xml = await readText(sitemapFile);
    for (const loc of extractXmlLocations(xml)) {
      const url = new URL(loc);
      if (url.origin !== SITE_ORIGIN) continue;

      if (
        url.pathname !== '/' &&
        !url.pathname.endsWith('/') &&
        !hasFileExtension(url.pathname)
      ) {
        badDirectoryUrls.push(`${loc} (${relativeToRepo(sitemapFile)})`);
      }

      if (RETIRED_REDIRECTED_PATHS.has(normalizePathname(url.pathname))) {
        retiredUrls.push(`${loc} (${relativeToRepo(sitemapFile)})`);
      }
    }
  }

  if (badDirectoryUrls.length > 0) {
    fail(
      `Sitemap directory-page URLs must end in '/':\n${badDirectoryUrls
        .map((entry) => `  - ${entry}`)
        .join('\n')}`,
    );
  } else {
    pass('sitemap directory-page URLs use trailing slashes');
  }

  if (retiredUrls.length > 0) {
    fail(
      `Retired redirected URLs must not appear in sitemaps:\n${retiredUrls
        .map((entry) => `  - ${entry}`)
        .join('\n')}`,
    );
  } else {
    pass('retired redirected URLs are absent from sitemaps');
  }
}

function extractCanonical(html) {
  return html.match(/<link\b[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["'][^>]*>/i)?.[1];
}

function extractOgUrl(html) {
  return html.match(/<meta\b[^>]*property=["']og:url["'][^>]*content=["']([^"']+)["'][^>]*>/i)?.[1];
}

function extractOgImage(html) {
  return html.match(/<meta\b[^>]*property=["']og:image["'][^>]*content=["']([^"']+)["'][^>]*>/i)?.[1];
}

async function validateRepresentativeHtml() {
  for (const [label, relativePath, expectedUrl] of REPRESENTATIVE_HTML) {
    const filePath = path.join(siteDist, relativePath);
    let html;
    try {
      html = await readText(filePath);
    } catch {
      fail(`Representative HTML is missing for ${label}: ${relativeToRepo(filePath)}`);
      continue;
    }

    const canonical = extractCanonical(html);
    const ogUrl = extractOgUrl(html);
    const ogImage = extractOgImage(html);

    if (!canonical) {
      fail(`${label} is missing a canonical URL in ${relativeToRepo(filePath)}`);
    }
    if (!ogUrl) {
      fail(`${label} is missing og:url in ${relativeToRepo(filePath)}`);
    }
    if (canonical && canonical !== expectedUrl) {
      fail(`${label} canonical mismatch: expected ${expectedUrl}, got ${canonical}`);
    }
    if (ogUrl && ogUrl !== expectedUrl) {
      fail(`${label} og:url mismatch: expected ${expectedUrl}, got ${ogUrl}`);
    }
    if (relativePath.startsWith('docs/') && ogImage && !ogImage.startsWith('https://kitaru.ai/docs/og/docs/')) {
      fail(`${label} docs og:image should live under /docs/og/docs, got ${ogImage}`);
    }
    if (canonical && ogUrl && canonical !== ogUrl) {
      fail(`${label} canonical and og:url disagree: ${canonical} vs ${ogUrl}`);
    }
  }

  pass('representative HTML pages expose matching canonical, og:url, and docs og:image values');
}

async function validateBuiltOutputSnippets() {
  const builtDirectories = [siteDist, docsOut];
  const matchingFiles = [];

  for (const directory of builtDirectories) {
    const files = await listFiles(directory);
    for (const filePath of files) {
      const content = await readText(filePath).catch(() => undefined);
      if (!content) continue;

      const matches = BAD_BUILT_OUTPUT_SNIPPETS.filter((snippet) =>
        content.includes(snippet),
      );
      if (matches.length > 0) {
        matchingFiles.push(`${relativeToRepo(filePath)} (${matches.join(', ')})`);
      }
    }
  }

  if (matchingFiles.length > 0) {
    fail(
      `Built output contains forbidden docs/docs URL shapes:\n${matchingFiles
        .map((entry) => `  - ${entry}`)
        .join('\n')}`,
    );
  } else {
    pass('built output does not contain forbidden docs/docs or root docs OG image URL shapes');
  }
}

async function validateGeneratedCliDocs() {
  const cliDocsDirectories = [
    path.join(repoRoot, 'docs', 'content', 'docs', 'cli'),
    path.join(docsOut, 'cli'),
  ];
  const badLinks = [];

  for (const directory of cliDocsDirectories) {
    const exists = await assertDirectory(directory, `Generated CLI docs directory ${relativeToRepo(directory)}`);
    if (!exists) continue;

    const files = (await listFiles(directory)).filter((filePath) =>
      ['.md', '.mdx'].includes(path.extname(filePath)),
    );
    for (const filePath of files) {
      const content = await readText(filePath);
      const matches = [...content.matchAll(/\]\(\.\/[A-Za-z0-9_-][^)]*\)/g)];
      for (const match of matches) {
        badLinks.push(`${relativeToRepo(filePath)}: ${match[0]}`);
      }
    }
  }

  if (badLinks.length > 0) {
    fail(
      `Generated CLI docs contain fragile ./child links:\n${badLinks
        .map((entry) => `  - ${entry}`)
        .join('\n')}`,
    );
  } else {
    pass('generated CLI docs do not contain fragile ./child links');
  }
}

async function validateRobots() {
  const siteRobotsPaths = [
    path.join(repoRoot, 'site', 'public', 'robots.txt'),
    path.join(siteDist, 'robots.txt'),
  ];

  for (const robotsPath of siteRobotsPaths) {
    const content = await readText(robotsPath).catch(() => undefined);
    if (!content) {
      fail(`Robots file is missing: ${relativeToRepo(robotsPath)}`);
      continue;
    }

    for (const sitemap of EXPECTED_ROBOTS_SITEMAPS) {
      if (!content.includes(sitemap)) {
        fail(`${relativeToRepo(robotsPath)} does not advertise ${sitemap}`);
      }
    }
  }

  const docsRobotsPaths = [
    path.join(repoRoot, 'docs', 'app', 'robots.ts'),
    path.join(docsOut, 'robots.txt'),
    path.join(siteDist, 'docs', 'robots.txt'),
  ];

  for (const robotsPath of docsRobotsPaths) {
    const content = await readText(robotsPath).catch(() => undefined);
    if (!content) {
      fail(`Docs robots file is missing: ${relativeToRepo(robotsPath)}`);
      continue;
    }

    if (!content.includes('https://kitaru.ai/docs/sitemap.xml')) {
      fail(`${relativeToRepo(robotsPath)} does not advertise ${DOCS_ROBOTS_SITEMAP}`);
    }
  }

  pass('robots files advertise expected sitemap locations');
}

function loadWorkerRedirectHarness(workerSource) {
  const testableSource = workerSource
    .replace(
      /^import astroWorker from .*;\n/m,
      `const astroWorker = {
        fetch: async (request) => {
          const url = new URL(request.url);
          if (url.pathname === '/api/waitlist') {
            return Response.redirect(new URL('/api/waitlist/', url).href, 301);
          }

          return new Response(null, {
            status: 599,
            headers: { 'x-astro-pathname': url.pathname },
          });
        },
      };\n`,
    )
    .replace(/export default\s*{/, 'return {');

  return new Function(testableSource)();
}

async function validateWorkerRedirects() {
  const workerPath = path.join(repoRoot, 'site', 'worker', 'index.js');
  const worker = loadWorkerRedirectHarness(await readText(workerPath));
  const env = {
    ASSETS: {
      fetch: async () => new Response('not found', { status: 404 }),
    },
  };

  const redirectCases = [
    {
      label: 'legacy get-started redirect',
      url: 'https://kitaru.ai/get-started',
      accept: 'text/html',
      status: 301,
      location: 'https://kitaru.ai/book-a-demo/',
    },
    {
      label: 'legacy docs memory redirect',
      url: 'https://kitaru.ai/docs/concepts/memory/',
      accept: 'text/html',
      status: 301,
      location: 'https://kitaru.ai/docs/concepts/checkpoints/',
    },
    {
      label: 'legacy blog memory redirect',
      url: 'https://kitaru.ai/blog/kitaru-agents-now-have-memory',
      accept: 'text/html',
      status: 301,
      location: 'https://kitaru.ai/blog/',
    },
    {
      label: 'HTML slash canonical redirect',
      url: 'https://kitaru.ai/docs/cli/executions?from=test',
      accept: 'text/html',
      status: 308,
      location: 'https://kitaru.ai/docs/cli/executions/?from=test',
    },
  ];

  for (const testCase of redirectCases) {
    const response = await worker.fetch(
      new Request(testCase.url, { headers: { accept: testCase.accept } }),
      env,
      {},
    );
    const location = response.headers.get('location');

    if (response.status !== testCase.status || location !== testCase.location) {
      fail(
        `Worker ${testCase.label} mismatch: expected ${testCase.status} ${testCase.location}, got ${response.status} ${location}`,
      );
    }
  }

  const nonRedirectCases = [
    {
      label: 'file-like docs markdown path',
      url: 'https://kitaru.ai/docs/cli/executions.md',
      accept: 'text/html',
    },
    {
      label: 'API path',
      url: 'https://kitaru.ai/api/waitlist',
      accept: 'text/html',
    },
    {
      label: 'Astro image optimizer endpoint',
      url: 'https://kitaru.ai/_image?href=%2Fdashboard.png&w=640&f=webp',
      accept: 'text/html',
    },
    {
      label: 'Astro server island endpoint',
      url: 'https://kitaru.ai/_server-islands/example',
      accept: 'text/html',
    },
    {
      label: 'markdown negotiation path',
      url: 'https://kitaru.ai/docs/cli/executions',
      accept: 'text/markdown',
    },
    {
      label: 'non-HTML Accept header',
      url: 'https://kitaru.ai/docs/cli/executions',
      accept: 'application/json',
    },
    {
      label: 'non-idempotent method',
      url: 'https://kitaru.ai/docs/cli/executions',
      accept: 'text/html',
      method: 'POST',
    },
  ];

  for (const testCase of nonRedirectCases) {
    const response = await worker.fetch(
      new Request(testCase.url, {
        method: testCase.method ?? 'GET',
        headers: { accept: testCase.accept },
      }),
      env,
      {},
    );

    if ([301, 308].includes(response.status)) {
      fail(
        `Worker ${testCase.label} should not redirect, got ${response.status} ${response.headers.get('location')}`,
      );
    }

    if (
      testCase.url === 'https://kitaru.ai/api/waitlist' &&
      response.headers.get('x-astro-pathname') !== '/api/waitlist/'
    ) {
      fail(
        `Worker API path should dispatch internally as /api/waitlist/, got ${response.headers.get('x-astro-pathname')}`,
      );
    }
  }

  pass('Worker redirect behavior uses 301 for permanent redirects and 308 only for eligible HTML slash redirects');
}

async function main() {
  const hasSiteDist = await assertDirectory(siteDist, 'Merged site output');
  const hasDocsOut = await assertDirectory(docsOut, 'Docs static output');
  if (!hasSiteDist || !hasDocsOut) {
    throw new Error('SEO validation requires both site/dist and docs/out. Run just site-build first.');
  }

  await validateSitemaps();
  await validateRepresentativeHtml();
  await validateBuiltOutputSnippets();
  await validateGeneratedCliDocs();
  await validateRobots();
  await validateWorkerRedirects();

  if (failures.length > 0) {
    console.error('SEO build validation failed:\n');
    for (const failure of failures) {
      console.error(`✘ ${failure}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log('SEO build validation passed:');
  for (const message of checked) {
    console.log(`✓ ${message}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
