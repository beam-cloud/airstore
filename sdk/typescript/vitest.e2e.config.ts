import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    fileParallelism: false,
    testTimeout: 300_000,
    hookTimeout: 30_000,
    include: ['test/**/*.e2e.test.ts'],
  },
});
