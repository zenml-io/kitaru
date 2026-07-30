import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The dashboard always talks to the kitaru server through this dev proxy so
// the browser sees a single origin (the server has no CORS middleware).
const target = process.env.KITARU_SERVER_URL ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/v1": { target, changeOrigin: true },
      "/health": { target, changeOrigin: true },
    },
  },
});
