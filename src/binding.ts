import * as fs from 'fs';
import * as os from 'os';
import { execSync } from 'child_process';
import bindings from 'bindings';

function normalizeArch(arch: string): string | null {
  if (arch === 'x64') return 'x86_64';
  if (arch === 'arm64') return 'aarch64';
  return arch;
}

function detectLibc(): 'gnu' | 'musl' {
  if (process.report && typeof process.report.getReport === 'function') {
    try {
      const report = process.report.getReport() as any;
      if (report?.header?.glibcVersionRuntime) {
        return 'gnu';
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

function loadBinding(): NativeBinding {
  const platform = getPlatform();
  const candidates = new Set([`${platform}-rocketmq.node`]);

  if (platform.startsWith('linux-')) {
    const legacy = platform.replace(/-(gnu|musl)$/, '');
    candidates.add(`${legacy}-rocketmq.node`);
  }

  let lastError: Error | undefined;
  for (const candidate of candidates) {
    try {
      return bindings(candidate);
    } catch (err) {
      lastError = err as Error;
    }
  }

  throw new Error(
    `Failed to load RocketMQ addon (${platform}): ${lastError?.message || 'unknown error'}`
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
  subscribe(topic: string, expression: string): void;
  setListener(callback: (msg: any, ack: any) => void): void;
  setSessionCredentials(accessKey: string, secretKey: string, onsChannel: string): void;
}

export interface NativeBinding {
  Producer: new (groupId: string, instanceName?: string | null, options?: Record<string, any>) => NativeProducer;
  PushConsumer: new (groupId: string, instanceName?: string | null, options?: Record<string, any>) => NativePushConsumer;
}

const nativeBinding: NativeBinding = loadBinding();

export default nativeBinding;

// CommonJS compatibility
// module.exports = nativeBinding;
// module.exports.default = nativeBinding;
