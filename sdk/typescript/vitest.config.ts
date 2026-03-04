import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    fileParallelism: false,
    testTimeout: 15_000,
    hookTimeout: 15_000,
    include: ['test/**/*.test.ts'],
    exclude: ['test/**/*.e2e.test.ts'],
  },
});
