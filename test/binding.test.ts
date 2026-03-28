'use strict';

import { describe, test, expect, vi, afterEach } from 'vitest';
import * as path from 'path';

interface LoadOptions {
  platform: string;
  arch: string;
  reportHeader?: any;
  reportValue?: any;
  execSyncOutput?: string;
  execSyncError?: boolean;
  existsSync?: (path: string) => boolean;
  readdirSync?: (path: string) => string[];
  requireImpl?: (candidate: string) => any;
}

function createBinding() {
  return {
    Producer: function Producer() {},
    PushConsumer: function PushConsumer() {}
  };
}

function setupBindingImport(options: LoadOptions) {
  vi.resetModules();

  const requireMock = vi.fn(options.requireImpl ?? (() => createBinding()));
  const createRequireMock = vi.fn(() => requireMock);
  vi.doMock('module', () => ({ createRequire: createRequireMock }));
  vi.doMock('os', () => ({
    platform: () => options.platform,
    arch: () => options.arch
  }));

  const execSyncMock = options.execSyncError
    ? vi.fn(() => {
      throw new Error('execSync error');
    })
    : vi.fn(() => options.execSyncOutput ?? '');
  vi.doMock('child_process', () => ({ execSync: execSyncMock }));

  const existsSyncMock = vi.fn((path: string) => (options.existsSync ? options.existsSync(path) : false));
  const readdirSyncMock = vi.fn((path: string) => (options.readdirSync ? options.readdirSync(path) : []));
  vi.doMock('fs', () => ({ existsSync: existsSyncMock, readdirSync: readdirSyncMock }));

  const originalGetReport = process.report?.getReport;
  const hasReportValue = Object.prototype.hasOwnProperty.call(options, 'reportValue');
  if (process.report) {
    if (hasReportValue) {
      process.report.getReport = () => options.reportValue;
    } else if (options.reportHeader === null) {
      (process.report as any).getReport = undefined;
    } else if (options.reportHeader !== undefined) {
      process.report.getReport = () => ({ header: options.reportHeader });
    } else {
      process.report.getReport = () => ({});
    }
  }

  const load = async () => {
    try {
      return await import('../src/binding');
    } finally {
      if (process.report && originalGetReport) {
        process.report.getReport = originalGetReport;
      }
    }
  };

  return { load, requireMock, createRequireMock, execSyncMock, existsSyncMock, readdirSyncMock };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.doUnmock('module');
  vi.doUnmock('os');
  vi.doUnmock('child_process');
  vi.doUnmock('fs');
  vi.resetModules();
});

describe('binding loader', () => {
  const repoRoot = path.join(__dirname, '..');

  test('linux gnu from report', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('linux arm64 normalizes arch', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'arm64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-aarch64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-aarch64-gnu-rocketmq.node'));
  });

  test('linux musl from execSync output', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: {},
      execSyncOutput: 'musl',
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-musl-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-musl-rocketmq.node'));
  });

  test('linux gnu from execSync output', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: {},
      execSyncOutput: 'GNU libc',
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('linux gnu when process.report is unavailable', async () => {
    const originalReport = Object.getOwnPropertyDescriptor(process, 'report');
    Object.defineProperty(process, 'report', {
      value: undefined,
      configurable: true
    });
    try {
      const { load, requireMock } = setupBindingImport({
        platform: 'linux',
        arch: 'x64',
        execSyncOutput: 'GNU libc',
        existsSync: (candidate: string) =>
          candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
      });

      await load();

      expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
    } finally {
      if (originalReport) {
        Object.defineProperty(process, 'report', originalReport);
      }
    }
  });

  test('linux execSync output unknown falls back to gnu', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: {},
      execSyncOutput: 'unknown',
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('linux report header non-object falls back to execSync', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: 'invalid',
      execSyncOutput: 'GNU libc',
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('linux report value null falls back to execSync', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportValue: null,
      execSyncOutput: 'GNU libc',
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('linux musl from fs fallback', async () => {
    const { load, requireMock, execSyncMock, existsSyncMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: {},
      execSyncError: true,
      existsSync: (candidate: string) =>
        candidate === '/lib/ld-musl-x86_64.so.1' ||
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-musl-rocketmq.node')
    });

    await load();

    expect(execSyncMock).toHaveBeenCalled();
    expect(existsSyncMock).toHaveBeenCalled();
    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-musl-rocketmq.node'));
  });

  test('linux gnu from fallback default', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: {},
      execSyncError: true,
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('darwin uses universal binding', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'darwin',
      arch: 'arm64',
      reportHeader: {},
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'darwin-universal-rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'darwin-universal-rocketmq.node'));
  });

  test('falls back to legacy linux candidate', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node') ||
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-rocketmq.node'),
      requireImpl: (candidate: string) => {
        if (candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node')) {
          throw new Error('fail');
        }
        return createBinding();
      }
    });

    await load();

    expect(requireMock).toHaveBeenNthCalledWith(1, path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'));
    expect(requireMock).toHaveBeenNthCalledWith(2, path.join(repoRoot, 'build', 'linux-x86_64-rocketmq.node'));
  });

  test('falls back to generic build artifact', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'darwin',
      arch: 'arm64',
      reportHeader: {},
      existsSync: (candidate: string) => candidate === path.join(repoRoot, 'build', 'rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'build', 'rocketmq.node'));
  });

  test('falls back to Release generic artifact', async () => {
    const { load, requireMock } = setupBindingImport({
      platform: 'darwin',
      arch: 'arm64',
      reportHeader: {},
      existsSync: (candidate: string) => candidate === path.join(repoRoot, 'Release', 'rocketmq.node')
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(repoRoot, 'Release', 'rocketmq.node'));
  });

  test('production mode: Release directory searched before build when isProduction is true', async () => {
    const releaseDir = path.join(repoRoot, 'Release');
    const { load, requireMock } = setupBindingImport({
      platform: 'darwin',
      arch: 'arm64',
      reportHeader: {},
      existsSync: (candidate: string) =>
        candidate === releaseDir ||
        candidate === path.join(releaseDir, 'darwin-universal-rocketmq.node'),
      readdirSync: (dir: string) => {
        if (dir === releaseDir) return ['darwin-universal-rocketmq.node'];
        return [];
      }
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(releaseDir, 'darwin-universal-rocketmq.node'));
  });

  test('production mode: linux platform-specific binary loaded from Release', async () => {
    const releaseDir = path.join(repoRoot, 'Release');
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === releaseDir ||
        candidate === path.join(releaseDir, 'linux-x86_64-gnu-rocketmq.node'),
      readdirSync: (dir: string) => {
        if (dir === releaseDir) return ['linux-x86_64-gnu-rocketmq.node', 'linux-aarch64-gnu-rocketmq.node'];
        return [];
      }
    });

    await load();

    expect(requireMock).toHaveBeenCalledWith(path.join(releaseDir, 'linux-x86_64-gnu-rocketmq.node'));
  });

  test('production mode: build dir with non-matching files does not trigger dev mode', async () => {
    const buildDir = path.join(repoRoot, 'build');
    const releaseDir = path.join(repoRoot, 'Release');
    const { load, requireMock } = setupBindingImport({
      platform: 'linux',
      arch: 'arm64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === buildDir ||
        candidate === releaseDir ||
        candidate === path.join(releaseDir, 'linux-aarch64-gnu-rocketmq.node'),
      readdirSync: (dir: string) => {
        if (dir === buildDir) return ['CMakeCache.txt', 'Makefile'];
        if (dir === releaseDir) return ['linux-aarch64-gnu-rocketmq.node'];
        return [];
      }
    });

    await load();

    // Should load from Release (searched first in production mode)
    expect(requireMock).toHaveBeenCalledWith(path.join(releaseDir, 'linux-aarch64-gnu-rocketmq.node'));
  });

  test('throws when all candidates fail', async () => {
    const { load } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'),
      requireImpl: () => {
        throw new Error('nope');
      }
    });

    await expect(load()).rejects.toThrow(/Failed to load RocketMQ addon/);
  });

  test('throws with unknown error when last error message is empty', async () => {
    const { load } = setupBindingImport({
      platform: 'linux',
      arch: 'x64',
      reportHeader: { glibcVersionRuntime: '2.31' },
      existsSync: (candidate: string) =>
        candidate === path.join(repoRoot, 'build', 'linux-x86_64-gnu-rocketmq.node'),
      requireImpl: () => {
        throw new Error('');
      }
    });

    await expect(load()).rejects.toThrow(/unknown error/);
  });

  test('throws on unsupported platform', async () => {
    const { load } = setupBindingImport({
      platform: 'win32',
      arch: 'x64',
      reportHeader: {}
    });

    await expect(load()).rejects.toThrow(/Unsupported platform/);
  });

  test('throws on unsupported linux arch', async () => {
    const { load } = setupBindingImport({
      platform: 'linux',
      arch: '',
      reportHeader: {}
    });

    await expect(load()).rejects.toThrow(/Unsupported architecture/);
  });
});
