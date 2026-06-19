import { createMDX } from "fumadocs-mdx/next";

const withMDX = createMDX();

/** @type {import('next').NextConfig} */
const config = {
  // Served at the root of sdkdocs.kitaru.ai (no /docs basePath).
  trailingSlash: true,
  serverExternalPackages: ["@takumi-rs/image-response"],
  output: "export",
  images: { unoptimized: true },
  reactStrictMode: true,
  turbopack: {
    root: ".",
  },
};

export default withMDX(config);
