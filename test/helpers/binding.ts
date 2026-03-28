import * as fs from 'fs';
import * as path from 'path';

function findStubLib(buildDir: string): boolean {
  if (!fs.existsSync(buildDir)) {
    return false;
  }
  const entries = fs.readdirSync(buildDir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isFile() && entry.name === 'librocketmq_stub.a') {
      return true;
    }
    if (entry.isDirectory()) {
      if (findStubLib(path.join(buildDir, entry.name))) {
        return true;
      }
    }
  }
  return false;
}

function ensureBindingBinary(rootDir: string): void {
  const buildPath = path.join(rootDir, 'build', 'rocketmq.node');
  if (!fs.existsSync(buildPath)) {
    throw new Error('Missing build/rocketmq.node. Run npm run build:test first.');
  }

  if (!findStubLib(path.join(rootDir, 'build'))) {
    throw new Error(
      'build/rocketmq.node exists but is not a stub build. ' +
      'Tests require the stub binary. Run: npm run build:test'
    );
  }
}

export {
  ensureBindingBinary
};
