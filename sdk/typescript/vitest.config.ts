import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Run test files sequentially to avoid race conditions on shared resources
    fileParallelism: false,
    // 30s per test — integration tests hit a real API
    testTimeout: 30_000,
    // Hook timeout for setup/teardown
    hookTimeout: 30_000,
    // Include .ts test files
    include: ['test/**/*.test.ts'],
  },
});
