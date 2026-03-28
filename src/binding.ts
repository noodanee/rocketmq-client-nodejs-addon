import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execSync } from 'child_process';
import { createRequire } from 'module';

function normalizeArch(arch: string): string | null {
  if (arch === 'x64') return 'x86_64';
  if (arch === 'arm64') return 'aarch64';
  return null;
}

function detectLibc(): 'gnu' | 'musl' {
  if (process.report && typeof process.report.getReport === 'function') {
    try {
      const report = process.report.getReport();
      if (report && typeof report === 'object' && 'header' in report) {
        const header = (report as any).header;
        if (header && typeof header === 'object') {
          const glibcVersionRuntime = (header as any).glibcVersionRuntime;
          if (typeof glibcVersionRuntime === 'string' && glibcVersionRuntime.length > 0) {
            return 'gnu';
          }
        }
      }
    } catch (_) {
      // ignore and continue with fallbacks
    }
  }

  try {
    const output = execSync('ldd --version', {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore']
    });
    if (/musl/i.test(output)) {
      return 'musl';
    }
    if (/glibc|gnu libc/i.test(output)) {
      return 'gnu';
    }
  } catch (_) {
    // ignore failures, fall back to fs detection
  }

  if (
    fs.existsSync('/lib/ld-musl-x86_64.so.1') ||
    fs.existsSync('/lib/ld-musl-aarch64.so.1') ||
    fs.existsSync('/lib/ld-musl.so.1')
  ) {
    return 'musl';
  }

  return 'gnu';
}

function getPlatform(): string {
  const platform = os.platform();
  const arch = os.arch();

  if (platform === 'linux') {
    const normalizedArch = normalizeArch(arch);
    if (!normalizedArch) {
      throw new Error(`Unsupported architecture for Linux: ${arch}`);
    }
    const libc = detectLibc();
    return `linux-${normalizedArch}-${libc}`;
  }

  if (platform === 'darwin') {
    return 'darwin-universal';
  }

  throw new Error(`Unsupported platform: ${platform}, architecture: ${arch}`);
}

function getBindingNames(platform: string): string[] {
  const candidates = new Set([`${platform}-rocketmq.node`, 'rocketmq.node']);

  if (platform.startsWith('linux-')) {
    candidates.add(`${platform.replace(/-(gnu|musl)$/, '')}-rocketmq.node`);
  }

  return [...candidates];
}

function getBindingDirectories(rootDir: string): string[] {
  const directories: string[] = [];
  const compiledDir = process.env.NODE_BINDINGS_COMPILED_DIR;

  if (compiledDir) {
    directories.push(path.isAbsolute(compiledDir) ? compiledDir : path.join(rootDir, compiledDir));
  }

  directories.push(path.join(rootDir, 'build'));
  directories.push(path.join(rootDir, 'Release'));
  directories.push(__dirname);

  return directories;
}

function loadBinding(): NativeBinding {
  const platform = getPlatform();
  const rootDir = path.resolve(__dirname, '..');
  const loadModule = createRequire(__filename);
  const searchedPaths: string[] = [];
  let lastError: Error | undefined;

  for (const directory of getBindingDirectories(rootDir)) {
    for (const bindingName of getBindingNames(platform)) {
      const candidate = path.join(directory, bindingName);
      searchedPaths.push(candidate);

      if (!fs.existsSync(candidate)) {
        continue;
      }

      try {
        const binding = loadModule(candidate) as NativeBinding | { default?: NativeBinding };
        return ('default' in binding && binding.default ? binding.default : binding) as NativeBinding;
      } catch (err) {
        lastError = err as Error;
      }
    }
  }

  const locations = searchedPaths.join(', ');
  if (!lastError) {
    throw new Error(`Failed to load RocketMQ addon (${platform}). Looked in: ${locations}`);
  }

  throw new Error(
    `Failed to load RocketMQ addon (${platform}) from ${locations}: ${lastError.message || 'unknown error'}`
  );
}

export interface NativeProducer {
  start(callback: (err: Error | null) => void): void;
  shutdown(callback: (err: Error | null) => void): void;
  send(
    topic: string,
    body: string | Buffer,
    options: Record<string, any>,
    callback: (err: Error | null, status?: number, msgId?: string, offset?: number) => void
  ): void;
  setSessionCredentials(accessKey: string, secretKey: string, onsChannel: string): void;
}

export interface NativePushConsumer {
  start(callback: (err: Error | null) => void): void;
  shutdown(callback: (err: Error | null) => void): void;
  isListenerIdle(): boolean;
  subscribe(topic: string, expression: string): void;
  setListener(callback: (msg: { topic: string; tags: string; keys: string; body: string; msgId: string }, ack: { done(success?: boolean): void }) => void): void;
  setSessionCredentials(accessKey: string, secretKey: string, onsChannel: string): void;
}

export interface NativeBinding {
  Producer: new (groupId: string, instanceName?: string | null, options?: Record<string, any>) => NativeProducer;
  PushConsumer: new (groupId: string, instanceName?: string | null, options?: Record<string, any>) => NativePushConsumer;
}

const nativeBinding: NativeBinding = loadBinding();

export default nativeBinding;
