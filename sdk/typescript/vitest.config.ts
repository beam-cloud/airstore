import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    fileParallelism: false,
    testTimeout: 45_000,
    hookTimeout: 30_000,
    include: ['test/**/*.test.ts'],
    exclude: ['test/**/*.e2e.test.ts'],
  },
});
