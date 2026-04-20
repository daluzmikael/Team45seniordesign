/**
 * Remove Next.js output and bundler caches (fixes missing chunk *.js errors).
 * Usage: node scripts/clean.mjs
 */
import { rmSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"

const root = dirname(dirname(fileURLToPath(import.meta.url)))
for (const name of [".next", join("node_modules", ".cache")]) {
  try {
    rmSync(join(root, name), { recursive: true, force: true })
    console.log("removed", name)
  } catch {
    // ignore
  }
}
