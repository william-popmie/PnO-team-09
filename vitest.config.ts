import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    pool: "forks",
    isolate: true,
    fileParallelism: false,
    maxWorkers: 1,
    minWorkers: 1,
    include: [
      "src/**/*.{spec,test}.mts",
      "raft-consensus-algorithm/packages/**/src/**/*.{spec,test}.ts",
      "raft-consensus-algorithm/packages/**/src/**/*.{spec,test}.mts"
    ],
    exclude: ["build/**", "**/node_modules/**"]
  },
  coverage: {
    provider: "v8",
    include: [
      "src/**/*.mts",
      "raft-consensus-algorithm/packages/**/src/**/*.ts",
      "raft-consensus-algorithm/packages/**/src/**/*.mts"
    ],
    exclude: [
      "build/**",
      "**/dist/**",
      "**/node_modules/**",
      "**/*.{spec,test}.{ts,mts,mjs,js}",
      "**/*.d.ts"
    ]
  }
});
