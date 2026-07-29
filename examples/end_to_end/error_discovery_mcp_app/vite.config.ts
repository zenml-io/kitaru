import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

const input = process.env.INPUT ?? "mcp-app.html";

export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    rollupOptions: { input },
    outDir: "dist",
    emptyOutDir: false,
    cssMinify: true,
    minify: true,
  },
});
