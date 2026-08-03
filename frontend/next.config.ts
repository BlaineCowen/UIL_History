import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // better-sqlite3 is a native module. Keeping it external stops the bundler
  // from tracing and rewriting the .node binary into the server chunk.
  serverExternalPackages: ["better-sqlite3"],
};

export default nextConfig;
