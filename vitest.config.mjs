import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Use Node.js environment for native binding compatibility
    environment: 'node',
    
    // Enable global test functions (describe, test, expect)
    globals: true,
    
    // Test file patterns
    include: ['test/**/*.test.{js,ts}'],
    exclude: ['node_modules', 'dist', 'build', 'deps', 'Release'],
    
    // Timeout configurations for native binding tests
    testTimeout: 30000, // 30 seconds for native operations
    hookTimeout: 10000, // 10 seconds for setup/teardown
    
    // Coverage configuration
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*.{js,ts}'],
      exclude: [
        'node_modules',
        'test',
        'deps',
        'build',
        'Release',
        'dist',
        '**/*.d.ts',
        '**/*.test.{js,ts}',
        '**/test/**'
      ],
      // Coverage thresholds
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 70,
        statements: 80
      }
    },
    
    // Setup files for environment configuration
    setupFiles: ['test/setup.ts'],
    
    // Enable isolation for native binding tests to prevent cross-test interference
    isolate: true,
    
    execArgv: ['--expose-gc'],
    pool: 'forks',
    fileParallelism: false,
    maxWorkers: 1,
    
    // Reporter configuration
    reporter: ['verbose'],
    
    // Disable file watching in CI environments
    watch: false
  },
  
  // ESBuild configuration for TypeScript support
  esbuild: {
    target: 'es2020'
  },
  
  // Resolve configuration to support importing from src
  resolve: {
    alias: {
      // Allow importing from src directory directly
      '@': '/src'
    }
  },
  
})
