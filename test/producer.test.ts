'use strict';

import { spawnSync } from 'child_process';
import { describe, test, expect, vi } from 'vitest';
import { randomBytes } from 'crypto';
import * as path from 'path';

import { RocketMQProducer } from '../src/producer';
import { Status, LogLevel } from '../src/constants';
import binding from '../src/binding';

const repoRoot = path.join(__dirname, '..');

let testCounter = 0;
function uniqueGroupName(prefix: string = 'test-group'): string {
  const suffix = randomBytes(6).toString('hex');
  return `${prefix}-${process.pid}-${Date.now()}-${testCounter++}-${suffix}`;
}

function setEnv(env: Record<string, string | undefined>): Record<string, string | undefined> {
  const original: Record<string, string | undefined> = {};
  for (const key of Object.keys(env)) {
    original[key] = process.env[key];
    if (env[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = env[key];
    }
  }
  return original;
}

function restoreEnv(env: Record<string, string | undefined>, original: Record<string, string | undefined>): void {
  for (const key of Object.keys(env)) {
    if (original[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = original[key];
    }
  }
}

async function nextTick(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

async function waitUntilThrows(fn: () => void, timeoutMs = 1000): Promise<Error> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      fn();
    } catch (error) {
      return error instanceof Error ? error : new Error(String(error));
    }
    await nextTick();
  }
  throw new Error('Expected function to throw');
}

interface ProducerScenarioResult {
  callbackCalled?: boolean;
  callbackError?: string | null;
  completed?: boolean;
  errors: Array<{ type: string; message: string }>;
  events?: string[];
  send1Called?: boolean;
  send2Called?: boolean;
  sendCalled?: boolean;
  sendDone?: boolean;
  sendError?: string | null;
}

function runProducerNodeScenario(
  body: string,
  env: Record<string, string | undefined> = {}
): { result: ProducerScenarioResult; stderr: string } {
  const childEnv: NodeJS.ProcessEnv = {
    ...process.env,
    NODE_BINDINGS_COMPILED_DIR: 'build'
  };

  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) {
      delete childEnv[key];
    } else {
      childEnv[key] = value;
    }
  }

  const script = [
    'process.env.NODE_BINDINGS_COMPILED_DIR = "build";',
    `const { ensureBindingBinary } = require(${JSON.stringify(path.join(repoRoot, 'test/helpers/binding'))});`,
    `ensureBindingBinary(${JSON.stringify(repoRoot)});`,
    `const binding = require(${JSON.stringify(path.join(repoRoot, 'src/binding'))}).default;`,
    'const { randomBytes } = require("crypto");',
    'const tick = () => new Promise((resolve) => setImmediate(resolve));',
    'const waitFor = async (predicate, timeoutMs = 1000) => {',
    '  const deadline = Date.now() + timeoutMs;',
    '  while (!predicate()) {',
    '    if (Date.now() >= deadline) {',
    '      return false;',
    '    }',
    '    await tick();',
    '  }',
    '  return true;',
    '};',
    'const group = `probe-${process.pid}-${Date.now()}-${randomBytes(4).toString("hex")}`;',
    'const errors = [];',
    'process.on("uncaughtException", (err) => {',
    '  errors.push({',
    '    type: "uncaughtException",',
    '    message: err && err.message ? err.message : String(err)',
    '  });',
    '});',
    'process.on("unhandledRejection", (reason) => {',
    '  errors.push({',
    '    type: "unhandledRejection",',
    '    message: reason && reason.message ? reason.message : String(reason)',
    '  });',
    '});',
    'async function main() {',
    body,
    '}',
    'main().then((result) => {',
    '  process.stdout.write(JSON.stringify(result));',
    '  setTimeout(() => process.exit(0), 2000).unref();',
    '}, (err) => {',
    '  console.error(err);',
    '  setTimeout(() => process.exit(1), 2000).unref();',
    '});'
  ].join('\n');

  const child = spawnSync(
    process.execPath,
    [
      '--force-node-api-uncaught-exceptions-policy=true',
      '--expose-gc',
      '-r',
      'ts-node/register/transpile-only',
      '-e',
      script
    ],
    {
      cwd: repoRoot,
      env: childEnv,
      encoding: 'utf8',
      timeout: 5000
    }
  );

  if (child.error) {
    throw new Error(
      `Producer subprocess failed: ${child.error.message}\nstdout:\n${child.stdout}\nstderr:\n${child.stderr}`
    );
  }

  if (child.status !== 0) {
    throw new Error(
      `Producer subprocess failed with code ${child.status ?? 'null'}\nstdout:\n${child.stdout}\nstderr:\n${child.stderr}`
    );
  }

  return {
    result: JSON.parse(child.stdout.trim()) as ProducerScenarioResult,
    stderr: child.stderr.trim()
  };
}

describe('Producer constructor tests', () => {
  test('with groupId only', () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    expect(producer).toBeTruthy();
    expect(producer.status).toBe(Status.STOPPED);
  });

  test('with groupId and options', () => {
    const producer = new RocketMQProducer(uniqueGroupName(), {
      nameServer: 'localhost:9876',
      logLevel: 'DEBUG'
    });
    expect(producer).toBeTruthy();
  });

  test('with groupId, instanceName, and options', () => {
    const producer = new RocketMQProducer(uniqueGroupName(), 'instance-1', {
      nameServer: 'localhost:9876'
    });
    expect(producer).toBeTruthy();
  });

  test('with groupId and instanceName only', () => {
    const producer = new RocketMQProducer(uniqueGroupName(), 'instance-only');
    expect(producer).toBeTruthy();
  });

  test('logLevel string to number conversion', () => {
    const producer = new RocketMQProducer(uniqueGroupName(), { logLevel: LogLevel.ERROR });
    expect(producer).toBeTruthy();
  });

  test('logLevel invalid string defaults', () => {
    const producer = new RocketMQProducer(uniqueGroupName(), { logLevel: 'invalid_level' as any });
    expect(producer).toBeTruthy();
  });
});

describe('Producer setSessionCredentials tests', () => {
  test('with valid args', () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    const result = producer.setSessionCredentials('accessKey', 'secretKey', 'channel');
    expect(result).toBe(true);
  });

  test('rejects after producer has started', async () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    await producer.start();
    try {
      expect(() => producer.setSessionCredentials('accessKey', 'secretKey', 'channel')).toThrow(/Cannot set session credentials/);
    } finally {
      await producer.shutdown();
    }
  });

  test('rejects while producer is starting', async () => {
    const env = { ROCKETMQ_STUB_PRODUCER_START_DELAY_MS: '120' };
    const original = setEnv(env);
    const core: any = new binding.Producer(uniqueGroupName());
    try {
      let startSettled = false;
      const startPromise = new Promise<void>((resolve, reject) => {
        core.start((err: Error | null) => {
          startSettled = true;
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
      const error = await waitUntilThrows(() => {
        core.setSessionCredentials('accessKey', 'secretKey', 'channel');
      });
      expect(error.message).toMatch(/Cannot set session credentials/);
      expect(startSettled).toBe(false);
      await startPromise;
      await new Promise<void>((resolve, reject) => {
        core.shutdown((err: Error | null) => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
    } finally {
      restoreEnv(env, original);
    }
  });

  test('throws on invalid accessKey', async () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    await expect(async () => {
      await producer.setSessionCredentials(123 as any, 'secretKey', 'channel');
    }).rejects.toThrow(/accessKey must be a string/);
  });

  test('throws on invalid secretKey', async () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    await expect(async () => {
      await producer.setSessionCredentials('accessKey', 123 as any, 'channel');
    }).rejects.toThrow(/secretKey must be a string/);
  });

  test('throws on invalid channel', async () => {
    const producer = new RocketMQProducer(uniqueGroupName());
    await expect(async () => {
      await producer.setSessionCredentials('accessKey', 'secretKey', 123 as any);
    }).rejects.toThrow(/onsChannel must be a string/);
  });
});

describe('Producer internal helpers', () => {
  test('getStatusName returns expected values', () => {
    const producer: any = new RocketMQProducer(uniqueGroupName());
    expect(producer.getStatusName(Status.STOPPED)).toBe('STOPPED');
    expect(producer.getStatusName(Status.STARTED)).toBe('STARTED');
    expect(producer.getStatusName(Status.STARTING)).toBe('STARTING');
    expect(producer.getStatusName(Status.STOPPING)).toBe('STOPPING');
    expect(producer.getStatusName(999)).toBe('UNKNOWN');
  });
});
describe('Producer start/shutdown lifecycle tests', () => {
  const baseEnv = {
    ROCKETMQ_STUB_PRODUCER_START_ERROR: undefined,
    ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: undefined
  };

  test('start() with Promise pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      expect(producer.status).toBe(Status.STARTED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start() with callback pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await new Promise<void>((resolve, reject) => {
        producer.start((err: any) => {
          if (err) return reject(err);
          resolve();
        });
      });
      expect(producer.status).toBe(Status.STARTED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start() error with Promise pattern', async () => {
    const env = { ROCKETMQ_STUB_PRODUCER_START_ERROR: '1' };
    const original = setEnv(env);
    try {
      const producer = new RocketMQProducer('test-group-start-error-promise');
      await expect(producer.start()).rejects.toThrow(/producer start error/);
    } finally {
      restoreEnv(env, original);
    }
  });

  test('start() error in production reports via emitWarning', async () => {
    const env = { ROCKETMQ_STUB_PRODUCER_START_ERROR: '1' };
    const original = setEnv(env);
    const originalNodeEnv = process.env.NODE_ENV;
    const originalWarn = process.emitWarning;
    let called = 0;
    try {
      process.env.NODE_ENV = 'production';
      process.emitWarning = () => {
        called += 1;
      };
      const producer = new RocketMQProducer('test-group-start-error-production');
      await expect(producer.start()).rejects.toThrow(/producer start error/);
    } finally {
      process.env.NODE_ENV = originalNodeEnv;
      process.emitWarning = originalWarn;
      restoreEnv(env, original);
    }
  });

  test('start() error with callback pattern', async () => {
    const env = { ROCKETMQ_STUB_PRODUCER_START_ERROR: '1' };
    const original = setEnv(env);
    try {
      const producer = new RocketMQProducer('test-group-start-error-callback');
      await new Promise<void>((resolve) => {
        producer.start((err: any) => {
          expect(err).toBeTruthy();
          expect(err.message).toMatch(/producer start error/);
          resolve();
        });
      });
    } finally {
      restoreEnv(env, original);
    }
  });

  test('start failure permanently disables producer (no retry)', async () => {
    const env = { ...baseEnv, ROCKETMQ_STUB_PRODUCER_START_ERROR: '1' };
    const original = setEnv(env);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      await expect(producer.start()).rejects.toThrow(/producer start error/);
      expect(producer.status).toBe(Status.STOPPED);
      expect((producer as any).shutdownCompleted).toBe(true);

      restoreEnv(env, original);
      await expect(producer.start()).rejects.toThrow(/Producer cannot be restarted after shutdown/);
    } finally {
      restoreEnv(env, original);
    }
  });

  test('shutdown() with Promise pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await producer.shutdown();
      expect(producer.status).toBe(Status.STOPPED);
      producer = null;
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() with callback pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await new Promise<void>((resolve, reject) => {
        producer.shutdown((err: any) => {
          if (err) return reject(err);
          resolve();
        });
      });
      expect(producer.status).toBe(Status.STOPPED);
      producer = null;
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start() queued during shutdown rejects after shutdown completes', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const shutdownPromise = producer.shutdown();
      const restartPromise = producer.start();
      await expect(restartPromise).rejects.toThrow(/cannot be restarted after shutdown/);
      await shutdownPromise;
      expect(producer.status).toBe(Status.STOPPED);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown queued when busy uses queue status', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      producer.pendingOperations = 1;
      await expect(producer.shutdown()).resolves.toBeUndefined();
      producer.pendingOperations = 0;
      expect(producer.status).toBe(Status.STOPPED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown queued when status STARTING rejects inside queue', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer: any = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STARTING;
      producer.pendingOperations = 1;
      await expect(producer.shutdown()).rejects.toThrow(/Producer is starting, please wait for start to complete/);
      producer.pendingOperations = 0;
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start handles synchronous core exception', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    let originalCore: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      originalCore = producer.core;
      producer.core = {
        start: () => {
          throw new Error('sync start boom');
        }
      };
      await expect(producer.start()).rejects.toThrow(/sync start boom/);
    } finally {
      if (producer && originalCore) {
        producer.core = originalCore;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start handles non-Error throw', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    let originalCore: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      originalCore = producer.core;
      producer.core = {
        start: () => {
          throw 'string boom';
        }
      };
      await expect(producer.start()).rejects.toThrow(/string boom/);
    } finally {
      if (producer && originalCore) {
        producer.core = originalCore;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start ignores duplicate error callbacks', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    let originalCore: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      originalCore = producer.core;
      producer.core = {
        start: (cb: any) => {
          cb(new Error('duplicate boom'));
          cb(new Error('duplicate boom'));
        },
        shutdown: (cb: any) => cb(null)
      };
      await expect(producer.start()).rejects.toThrow(/duplicate boom/);
    } finally {
      if (producer && originalCore) {
        producer.core = originalCore;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start ignores duplicate success callbacks', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    let originalCore: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      originalCore = producer.core;
      producer.core = {
        start: (cb: any) => {
          cb(null);
          cb(null);
        },
        shutdown: (cb: any) => cb(null)
      };
      await expect(producer.start()).resolves.toBeUndefined();
      await producer.shutdown();
    } finally {
      if (producer && originalCore) {
        producer.core = originalCore;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('operation queue handles internal error', async () => {
    const original = setEnv(baseEnv);
    const originalWarn = process.emitWarning;
    let producer: any;
    let called = 0;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      producer.operationQueue = Promise.reject(new Error('queue boom'));
      process.emitWarning = (...args: any[]) => {
        called += 1;
        if (called === 1) {
          throw new Error('warn boom');
        }
        originalWarn.apply(process, args as any);
      };
      producer.start(() => {});
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    } finally {
      process.emitWarning = originalWarn;
      if (producer) {
        producer.pendingOperations = 0;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() error resets to STOPPED (non-retryable)', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const errEnv = { ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: '1' };
      const errOriginal = setEnv(errEnv);
      try {
        await expect(producer.shutdown()).rejects.toThrow(/producer shutdown error/);
      } finally {
        restoreEnv(errEnv, errOriginal);
      }
      expect(producer.status).toBe(Status.STOPPED);
      expect(producer.shutdownCompleted).toBe(true);
      producer = null;
    } finally {
      if (producer && (producer.status === Status.STARTED || producer.status === Status.STOPPING)) {
        try { await producer.shutdown(); } catch (_) {}
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('queued shutdown rejects when status becomes STOPPED', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STARTED;
      (producer as any).pendingOperations = 1;
      const promise = producer.shutdown();
      producer.status = Status.STOPPED;
      await expect(promise).rejects.toThrow(/Producer is already stopped/);
      expect((producer as any).pendingOperations).toBe(1);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('status transitions STOPPED -> STARTING -> STARTED', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      expect(producer.status).toBe(Status.STOPPED);

      // 调用 start() 后状态立即变为 STARTING
      const promise = producer.start();
      expect(producer.status).toBe(Status.STARTING);

      await promise;
      expect(producer.status).toBe(Status.STARTED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('status transitions STARTED -> STOPPING -> STOPPED', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      expect(producer.status).toBe(Status.STARTED);

      // 调用 shutdown() 后状态立即变为 STOPPING
      const promise = producer.shutdown();
      expect(producer.status).toBe(Status.STOPPING);

      await promise;
      expect(producer.status).toBe(Status.STOPPED);
      producer = null;
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start does not change status while queue is busy', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer: any = new RocketMQProducer(uniqueGroupName());
      producer.operationQueue = new Promise<void>(() => {});
      producer.pendingOperations = 1;
      producer.start();
      expect(producer.status).toBe(Status.STOPPED);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start() throws when not STOPPED', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await expect(producer.start()).rejects.toThrow(/Producer is already started/);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() throws when not STARTED', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      await expect(producer.shutdown()).rejects.toThrow(/Producer is already stopped/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start() rejects when STOPPING', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      await expect(producer.start()).rejects.toThrow(/Producer is stopping, please wait for shutdown to complete/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start() rejects when STARTING', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STARTING;
      await expect(producer.start()).rejects.toThrow(/Producer is already starting/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start() handles unknown status before queue', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      producer.status = 999;
      await producer.start();
      expect(producer.status).toBe(Status.STARTED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() rejects when STARTING', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STARTING;
      await expect(producer.shutdown()).rejects.toThrow(/Producer is starting, please wait for start to complete/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() handles unknown status before queue', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      producer.status = 999;
      await producer.shutdown();
      expect(producer.status).toBe(Status.STOPPED);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('start queued when status STOPPING rejects inside queue', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer: any = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      producer.pendingOperations = 1;
      await expect(producer.start()).rejects.toThrow(/Producer is stopping, please wait for shutdown to complete/);
      producer.pendingOperations = 0;
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('queue catch skips logging in production', async () => {
    const original = setEnv(baseEnv);
    const originalNodeEnv = process.env.NODE_ENV;
    let producer: any;
    const uncaughtHandler = () => {};
    process.on('uncaughtException', uncaughtHandler);
    try {
      process.env.NODE_ENV = 'production';
      producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      producer.pendingOperations = 1;
      producer.start(() => {
        throw new Error('callback boom');
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
    } finally {
      process.off('uncaughtException', uncaughtHandler);
      process.env.NODE_ENV = originalNodeEnv;
      if (producer) {
        producer.pendingOperations = 0;
        producer.status = Status.STOPPED;
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('queue catch handles unexpected status errors in production', async () => {
    const original = setEnv(baseEnv);
    const originalNodeEnv = process.env.NODE_ENV;
    let producer: any;
    try {
      process.env.NODE_ENV = 'production';
      producer = new RocketMQProducer(uniqueGroupName());
      producer.pendingOperations = 1;
      Object.defineProperty(producer, 'status', {
        get() {
          throw new Error('status boom');
        },
        configurable: true
      });
      producer.start(() => {});
      await new Promise<void>((resolve) => setImmediate(resolve));
    } finally {
      process.env.NODE_ENV = originalNodeEnv;
      if (producer) {
        delete (producer as any).status;
        producer.status = Status.STOPPED;
        producer.pendingOperations = 0;
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('queue catch handles errors when emitWarning throws', async () => {
    const original = setEnv(baseEnv);
    const originalWarn = process.emitWarning;
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      producer.pendingOperations = 1;
      Object.defineProperty(producer, 'status', {
        get() {
          throw new Error('status boom');
        },
        configurable: true
      });
      process.emitWarning = () => {
        throw new Error('warn boom');
      };
      producer.start(() => {});
      await new Promise<void>((resolve) => setImmediate(resolve));
    } finally {
      process.emitWarning = originalWarn;
      if (producer) {
        delete (producer as any).status;
        producer.status = Status.STOPPED;
        producer.pendingOperations = 0;
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown queued when status STOPPING retries inside queue', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer: any = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      producer.pendingOperations = 1;
      await expect(producer.shutdown()).rejects.toThrow(/Producer is not started/);
      producer.pendingOperations = 0;
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('shutdown() retries when STOPPING and queue busy', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer: any = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      producer.pendingOperations = 1;
      await expect(producer.shutdown()).rejects.toThrow(/Producer is not started/);
      producer.pendingOperations = 0;
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('start after shutdown rejects', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await producer.shutdown();
      await expect(producer.start()).rejects.toThrow(/cannot be restarted after shutdown/);
      expect(producer.status).toBe(Status.STOPPED);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('multiple producers with producerRef counting', async () => {
    const original = setEnv(baseEnv);
    let producer1: any, producer2: any;
    try {
      producer1 = new RocketMQProducer(uniqueGroupName());
      producer2 = new RocketMQProducer(uniqueGroupName());
      await producer1.start();
      await producer2.start();
      await producer1.shutdown();
      producer1 = null;
      await producer2.shutdown();
      producer2 = null;
    } finally {
      if (producer1 && producer1.status === Status.STARTED) {
        await producer1.shutdown();
      }
      if (producer2 && producer2.status === Status.STARTED) {
        await producer2.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });
});
describe('Producer send tests', () => {
  const baseEnv = {
    ROCKETMQ_STUB_PRODUCER_START_ERROR: undefined,
    ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: undefined,
    ROCKETMQ_STUB_SEND_EXCEPTION: undefined,
    ROCKETMQ_STUB_SEND_THROW: undefined
  };

  test('with string body using Promise', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const result = await producer.send('test-topic', 'hello world');
      expect(result.status).toBe(0);
      expect(result.statusStr).toBe('OK');
      expect(result.msgId).toBeTruthy();
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with Buffer body using Promise', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const result = await producer.send('test-topic', Buffer.from('hello buffer'));
      expect(result.status).toBe(0);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with callback pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await new Promise<void>((resolve, reject) => {
        producer.send('test-topic', 'hello', (err: any, result: any) => {
          if (err) return reject(err);
          expect(result.status).toBe(0);
          resolve();
        });
      });
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with options', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const result = await producer.send('test-topic', 'hello', { tags: 'tagA', keys: 'key1' });
      expect(result.status).toBe(0);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with options and callback', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await new Promise<void>((resolve, reject) => {
        producer.send('test-topic', 'hello', { tags: 'tagA' }, (err: any, result: any) => {
          if (err) return reject(err);
          expect(result.status).toBe(0);
          resolve();
        });
      });
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with empty string body returns EMPTY_BODY', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const result = await producer.send('test-topic', '');
      expect(result.status).toBe(-1);
      expect(result.statusStr).toBe('EMPTY_BODY');
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('with empty Buffer body returns EMPTY_BODY', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const result = await producer.send('test-topic', Buffer.alloc(0));
      expect(result.status).toBe(-1);
      expect(result.statusStr).toBe('EMPTY_BODY');
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('maps unknown status and empty msgId', async () => {
    let producer: any;
    let originalCore: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      originalCore = producer.core;
      producer.core = {
        start: (cb: any) => cb(null),
        shutdown: (cb: any) => cb(null),
        send: (_topic: string, _body: any, _options: any, cb: any) => cb(null, 4, undefined, 5),
        setSessionCredentials: () => {}
      };
      await producer.start();
      const result = await producer.send('test-topic', 'body');
      expect(result.status).toBe(4);
      expect(result.statusStr).toBe('UNKNOWN');
      expect(result.msgId).toBe('');
      expect(result.offset).toBe(5);
      await producer.shutdown();
    } finally {
      if (producer && originalCore) {
        producer.core = originalCore;
      }
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
    }
  });

  test('empty body with callback', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      await new Promise<void>((resolve) => {
        producer.send('test-topic', '', (err: any, result: any) => {
          expect(err).toBe(null);
          expect(result.status).toBe(-1);
          expect(result.statusStr).toBe('EMPTY_BODY');
          resolve();
        });
      });
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('error with Promise pattern (onException)', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const errEnv = { ROCKETMQ_STUB_SEND_EXCEPTION: '1' };
      const errOriginal = setEnv(errEnv);
      try {
        await expect(producer.send('test-topic', 'hello')).rejects.toThrow(/producer send exception/);
      } finally {
        restoreEnv(errEnv, errOriginal);
      }
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('error with callback pattern', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      const errEnv = { ROCKETMQ_STUB_SEND_EXCEPTION: '1' };
      const errOriginal = setEnv(errEnv);
      try {
        await new Promise<void>((resolve) => {
          producer.send('test-topic', 'hello', (err: any) => {
            expect(err).toBeTruthy();
            expect(err.message).toMatch(/producer send exception/);
            resolve();
          });
        });
      } finally {
        restoreEnv(errEnv, errOriginal);
      }
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('throws on invalid topic', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      expect(() => {
        producer.send(123, 'body');
      }).toThrow(/topic must be a string/);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('throws on invalid body type', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      expect(() => {
        producer.send('topic', 123);
      }).toThrow(/body must be a string or Buffer/);
    } finally {
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('send when producer is STOPPED returns error via Promise', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      // Producer is STOPPED by default
      await expect(producer.send('topic', 'body')).rejects.toThrow(/Producer must be started.*STOPPED/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send when producer is STOPPED returns error via callback', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      // Producer is STOPPED by default - callback is now always async
      await new Promise<void>((resolve) => {
        producer.send('topic', 'body', (err: any) => {
          expect(err).toBeTruthy();
          expect(err.message).toMatch(/Producer must be started.*STOPPED/);
          resolve();
        });
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send Promise rejects after shutdown when callback delayed', async () => {
    const original = setEnv(baseEnv);
    let producer: any;
    try {
      producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();
      process.env.ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS = '5000';
      const sendPromise = producer.send('topic', 'body');
      process.env.ROCKETMQ_STUB_PRODUCER_SHUTDOWN_WAIT_MS = '10';
      await producer.shutdown();
      expect(producer.status).toBe(Status.STOPPED);
      await expect(sendPromise).rejects.toThrow('Send cancelled: producer shutdown timeout');
    } finally {
      delete process.env.ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS;
      delete process.env.ROCKETMQ_STUB_PRODUCER_SHUTDOWN_WAIT_MS;
      restoreEnv(baseEnv, original);
      if (producer && producer.status === Status.STARTED) {
        await producer.shutdown();
      }
    }
  });

  test('send callback errors propagate when send succeeds', async () => {
    const { result } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let callbackCalled = false;
      await new Promise((resolve) => {
        producer.send('topic', 'body', {}, () => {
          callbackCalled = true;
          resolve();
          throw new Error('send callback boom');
        });
      });
      await new Promise((resolve) => setTimeout(resolve, 50));
      await new Promise((resolve) => producer.shutdown(() => resolve()));
      return { callbackCalled, errors };
    `);

    expect(result.callbackCalled).toBe(true);
    expect(result.errors).toEqual([{ type: 'uncaughtException', message: 'send callback boom' }]);
  });

  test('send callback errors propagate in production', async () => {
    const { result } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      await new Promise((resolve) => {
        producer.send('topic', 'body', {}, () => {
          resolve();
          throw new Error('send callback boom');
        });
      });
      await new Promise((resolve) => setTimeout(resolve, 50));
      await new Promise((resolve) => producer.shutdown(() => resolve()));
      return { errors };
    `, {
      NODE_ENV: 'production'
    });

    expect(result.errors).toEqual([{ type: 'uncaughtException', message: 'send callback boom' }]);
  });

  test('send when producer is STARTING returns error via Promise', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STARTING;
      await expect(producer.send('topic', 'body')).rejects.toThrow(/Producer must be started.*STARTING/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send when producer is STOPPING returns error via Promise', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new RocketMQProducer(uniqueGroupName());
      producer.status = Status.STOPPING;
      await expect(producer.send('topic', 'body')).rejects.toThrow(/Producer must be started.*STOPPING/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });
});

describe('Producer static properties', () => {
  test('SEND_RESULT static property', () => {
    expect(RocketMQProducer.SEND_RESULT.OK).toBe(0);
    expect(RocketMQProducer.SEND_RESULT.FLUSH_DISK_TIMEOUT).toBe(1);
    expect(RocketMQProducer.SEND_RESULT.FLUSH_SLAVE_TIMEOUT).toBe(2);
    expect(RocketMQProducer.SEND_RESULT.SLAVE_NOT_AVAILABLE).toBe(3);
  });
});
describe('Producer C++ binding coverage tests', () => {
  const baseEnv = {
    ROCKETMQ_STUB_PRODUCER_START_ERROR: undefined,
    ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: undefined,
    ROCKETMQ_STUB_SEND_EXCEPTION: undefined,
    ROCKETMQ_STUB_SEND_THROW: undefined
  };

  test('constructor with all options (covers SetOptions branches)', () => {
    const producer = new binding.Producer(uniqueGroupName(), 'instance-1', {
      nameServer: 'localhost:9876',
      groupName: 'override-group',
      maxMessageSize: 1024 * 1024,
      compressLevel: 5,
      sendMessageTimeout: 3000,
      logLevel: 3,
      logDir: '/tmp/logs',
      logFileSize: 1024 * 1024 * 10,
      logFileNum: 5
    });
    expect(producer).toBeTruthy();
  });

  test('constructor with logLevel boundary values', () => {
    const producerLow = new binding.Producer(uniqueGroupName(), 'instance-1', {
      logLevel: -1
    });
    const producerHigh = new binding.Producer(uniqueGroupName(), 'instance-1', {
      logLevel: 999
    });
    expect(producerLow).toBeTruthy();
    expect(producerHigh).toBeTruthy();
  });

  test('setSessionCredentials with non-string accessKey throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      producer.setSessionCredentials(123 as any, 'secret', 'channel');
    }).toThrow(/All arguments must be strings/);
  });

  test('setSessionCredentials with non-string secretKey throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      producer.setSessionCredentials('access', 123 as any, 'channel');
    }).toThrow(/All arguments must be strings/);
  });

  test('setSessionCredentials with non-string channel throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      producer.setSessionCredentials('access', 'secret', 123 as any);
    }).toThrow(/All arguments must be strings/);
  });

  test('setSessionCredentials with too few arguments throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      (producer as any).setSessionCredentials('access', 'secret');
    }).toThrow(/Wrong number of arguments/);
  });

  test('start without callback throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      (producer as any).start();
    }).toThrow(/Function expected/);
  });

  test('start with non-function throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      (producer as any).start('not a function');
    }).toThrow(/Function expected/);
  });

  test('shutdown without callback throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      (producer as any).shutdown();
    }).toThrow(/Function expected/);
  });

  test('shutdown with non-function throws', () => {
    const producer = new binding.Producer(uniqueGroupName());
    expect(() => {
      (producer as any).shutdown('not a function');
    }).toThrow(/Function expected/);
  });

  test('send with too few arguments throws', () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      expect(() => {
        (producer as any).send('topic', 'body', {});
      }).toThrow(/Wrong number of arguments/);
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send with non-string topic throws', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      await new Promise<void>((resolve) => {
        producer.start(() => resolve());
      });
      expect(() => {
        producer.send(123 as any, 'body', {}, () => {});
      }).toThrow(/Topic must be a string/);
      await new Promise<void>((resolve) => {
        producer.shutdown(() => resolve());
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send with non-function callback throws', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      await new Promise<void>((resolve) => {
        producer.start(() => resolve());
      });
      expect(() => {
        (producer as any).send('topic', 'body', {}, 'not a function');
      }).toThrow(/Callback must be a function/);
      await new Promise<void>((resolve) => {
        producer.shutdown(() => resolve());
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send with non-object options skips native options parsing', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      await new Promise<void>((resolve) => {
        producer.start(() => resolve());
      });
      await new Promise<void>((resolve, reject) => {
        producer.send('topic', 'body', 123 as any, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      });
      await new Promise<void>((resolve) => {
        producer.shutdown(() => resolve());
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send with invalid body type throws', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      await new Promise<void>((resolve) => {
        producer.start(() => resolve());
      });
      expect(() => {
        producer.send('topic', 12345 as any, {}, () => {});
      }).toThrow(/Message body must be a string or buffer/);
      await new Promise<void>((resolve) => {
        producer.shutdown(() => resolve());
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('send with ROCKETMQ_STUB_SEND_THROW triggers asynchronous exception', async () => {
    const original = setEnv(baseEnv);
    try {
      const producer = new binding.Producer(uniqueGroupName());
      await new Promise<void>((resolve) => {
        producer.start(() => resolve());
      });
      const errEnv = { ROCKETMQ_STUB_SEND_THROW: '1' };
      const errOriginal = setEnv(errEnv);
      try {
        // ROCKETMQ_STUB_SEND_THROW 现在通过 callback 报告异常,而不是同步抛出
        const error = await new Promise<any>((resolve) => {
          producer.send('topic', 'body', {}, (err: any) => {
            resolve(err);
          });
        });
        expect(error).toBeTruthy();
        expect(error.message).toMatch(/producer send throw/);
      } finally {
        restoreEnv(errEnv, errOriginal);
      }
      await new Promise<void>((resolve) => {
        producer.shutdown(() => resolve());
      });
    } finally {
      restoreEnv(baseEnv, original);
    }
  });

  test('BlockingCall failure triggers cleanup (coverage branch)', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let callbackCalled = false;
      producer.send('topic', 'body', {}, () => {
        callbackCalled = true;
      });
      // Shutdown waits for pending sends; by the time it completes,
      // TSFN Finalize has fired and dropped the undeliverable callback.
      await new Promise((resolve) => producer.shutdown(() => resolve()));
      await tick();
      await tick();
      return { callbackCalled, errors };
    `, {
      ROCKETMQ_STUB_PRODUCER_BLOCKING_FAIL: '1'
    });

    // When BlockingCall fails, Finalize invokes the JS callback with the
    // delivery error so the JS-layer settler is rejected immediately.
    expect(result.callbackCalled).toBe(true);
    expect(result.errors).toEqual([]);
    expect(stderr).toMatch(/Failed to schedule JavaScript callback/);
  });

  test('shutdown waits for in-flight async send callback', async () => {
    const { result } = runProducerNodeScenario(`
      const events = [];
      for (let i = 0; i < 25; i++) {
        const producer = new binding.Producer(\`\${group}-\${i}\`);
        await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
        producer.send('topic', 'body', {}, (err) => {
          if (err) {
            throw err;
          }
          events.push('send');
        });
        await new Promise((resolve, reject) => {
          producer.shutdown((err) => {
            if (err) {
              reject(err);
              return;
            }
            events.push('shutdown');
            resolve();
          });
        });
        await tick();
        await tick();
        if (events.slice(-2).join(',') !== 'send,shutdown') {
          break;
        }
      }
      return { errors, events };
    `, {
      ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '10'
    });

    expect(result.errors).toEqual([]);
    expect(result.events).toEqual(Array.from({ length: 25 }, () => ['send', 'shutdown']).flat());
  });

  test('__testForceDestroy does not block on in-flight async send callback', async () => {
    // ForceDestroyForTest calls SafeShutdown(false) — does NOT wait for pending
    // sends. The async send callback fires independently after destroy returns.
    const { result } = runProducerNodeScenario(`
      const events = [];
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      producer.send('topic', 'body', {}, (err) => {
        if (err) {
          throw err;
        }
        events.push('send');
      });
      producer.__testForceDestroy();
      events.push('destroy');
      const completed = await waitFor(() => events.includes('send') || errors.length > 0, 2000);
      return { completed, errors, events };
    `, {
      ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '50'
    });

    // Now SafeShutdown waits for pending sends, so callback should complete
    expect(result.errors).toEqual([]);
    expect(result.completed).toBe(true);
    expect(result.events).toEqual(['destroy', 'send']);
  });

  test('shutdown timeout cancels inflight callbacks', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const events = [];
      const producer = new binding.Producer(group);

      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let sendCalled = false;
      producer.send('topic', 'body', {}, (err) => {
        sendCalled = true;
        if (err) {
          events.push('send-error: ' + err.message);
        } else {
          events.push('send');
        }
      });
      await new Promise((resolve, reject) => {
        producer.shutdown((err) => {
          if (err) {
            reject(err);
            return;
          }
          events.push('shutdown');
          resolve();
        });
      });
      await tick();
      await tick();

      return { events, errors, sendCalled };
    `, {
      ROCKETMQ_STUB_PRODUCER_SHUTDOWN_WAIT_MS: '10',
      ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '50'
    });

    expect(result.errors).toEqual([]);
    // send's callback is dropped because CancelPendingSends aborted the TSFN
    // to break the prevent_gc circular reference (fallback reclamation chain).
    expect(result.sendCalled).toBe(false);
    expect(result.events).toContain('shutdown');
    expect(stderr).toContain('Timed out waiting for pending send callbacks during shutdown');
  });

  test('CallJs null env check (coverage branch)', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let callbackCalled = false;
      producer.send('topic', 'body', {}, () => {
        callbackCalled = true;
      });
      await waitFor(() => callbackCalled || errors.length > 0);
      await new Promise((resolve) => producer.shutdown(() => resolve()));
      return { callbackCalled, errors };
    `, {
      ROCKETMQ_STUB_PRODUCER_CALLJS_NULL_ENV: '1'
    });

    expect(result.callbackCalled).toBe(false);
    expect(result.errors).toEqual([]);
    expect(stderr).toBe('');
  });

  test('CallJs Napi::Error triggers uncaughtException', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let callbackCalled = false;
      producer.send('topic', 'body', {}, () => {
        callbackCalled = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 100));
      await new Promise((resolve) => producer.shutdown(() => resolve()));
      return { callbackCalled, errors };
    `, {
      ROCKETMQ_STUB_PRODUCER_CALLJS_THROW: '1'
    });

    expect(result.callbackCalled).toBe(false);
    expect(result.errors).toEqual([{ type: 'uncaughtException', message: 'producer calljs throw' }]);
  });

  test('destructor handles shutdown error', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      producer.__testForceDestroy();
      return { errors };
    `, {
      ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: '1'
    });

    expect(result.errors).toEqual([]);
    expect(stderr).toMatch(/Producer shutdown failed/);
  });

  test('destructor skips orphan cleanup when shutdown fails (no double-free)', async () => {
    const { result, stderr } = runProducerNodeScenario(`
      const producer = new binding.Producer(group);
      await new Promise((resolve, reject) => producer.start((err) => err ? reject(err) : resolve()));
      let sendDone = false;
      let sendError = null;
      producer.send('TopicTest', 'body', {}, (err) => {
        sendDone = true;
        sendError = err;
      });
      await tick();
      producer.__testForceDestroy();
      // Wait for async callback to fire (or be cleaned up)
      for (let i = 0; i < 20; i++) await tick();
      return { sendDone, sendError: sendError ? sendError.message : null, errors };
    `, {
      ROCKETMQ_STUB_PRODUCER_SHUTDOWN_ERROR: '1',
      ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '500'
    });

    expect(result.errors).toEqual([]);
    expect(stderr).toMatch(/Producer shutdown failed/);
    expect(result.sendDone).toBe(true);
  });
});
