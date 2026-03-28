'use strict';

import { spawnSync } from 'child_process';
import * as path from 'path';
import { describe, test, expect, vi } from 'vitest';
import { LogLevel, Status } from '../src/constants';

import { RocketMQPushConsumer } from '../src/consumer';
import binding from '../src/binding';

const repoRoot = path.join(__dirname, '..');

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

interface ConsumerScenarioResult {
  errors: Array<{ type: string; message: string }>;
  [key: string]: unknown;
}

function runConsumerNodeScenario(
  body: string,
  env: Record<string, string | undefined> = {}
): { result: ConsumerScenarioResult; stderr: string } {
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
      timeout: 30000
    }
  );

  if (child.error) {
    throw new Error(
      `Consumer subprocess failed: ${child.error.message}\nstdout:\n${child.stdout ?? ''}\nstderr:\n${child.stderr ?? ''}`
    );
  }

  if (child.status !== 0) {
    throw new Error(
      `Consumer subprocess failed with code ${child.status ?? 'null'}\nstdout:\n${child.stdout ?? ''}\nstderr:\n${child.stderr ?? ''}`
    );
  }

  return {
    result: JSON.parse(child.stdout.trim()) as ConsumerScenarioResult,
    stderr: child.stderr.trim()
  };
}

describe('PushConsumer tests', () => {
  describe('PushConsumer constructor tests', () => {
    test('constructor overloads and logLevel mapping', () => {
      const c1 = new RocketMQPushConsumer('G1', { nameServer: '127.0.0.1', logLevel: LogLevel.DEBUG });
      const c2 = new RocketMQPushConsumer('G2', 'INST', { logLevel: LogLevel.NUM });
      const c3 = new RocketMQPushConsumer('G3', 'INST', {});

      expect(c1.status).toBe(Status.STOPPED);
      expect(c2.status).toBe(Status.STOPPED);
      expect(c3.status).toBe(Status.STOPPED);
    });

    test('constructor options mapping', () => {
      const consumer = new RocketMQPushConsumer('G1', {
        nameServer: '127.0.0.1',
        groupName: 'GROUP_A',
        threadCount: 3,
        maxBatchSize: 16,
        maxReconsumeTimes: 2,
        logLevel: 1,
        logDir: '/tmp',
        logFileSize: 8,
        logFileNum: 2
      });

      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('constructor logLevel boundary values', () => {
      const c1 = new RocketMQPushConsumer('G1', { logLevel: -1 as any });
      const c2 = new RocketMQPushConsumer('G2', { logLevel: 999 as any });
      expect(c1.status).toBe(Status.STOPPED);
      expect(c2.status).toBe(Status.STOPPED);
    });

    test('constructor logLevel string mapping', () => {
      const consumer = new RocketMQPushConsumer('G4', { logLevel: 'warn' as any });
      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('constructor instanceName without options', () => {
      const consumer = new RocketMQPushConsumer('G5', 'INST_ONLY');
      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('constructor logLevel invalid string defaults', () => {
      const consumer = new RocketMQPushConsumer('G6', { logLevel: 'invalid_level' as any });
      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('constructor threadCount validation', () => {
      // threadCount: 0 should throw
      expect(() => {
        new RocketMQPushConsumer('G7', { threadCount: 0 });
      }).toThrow();

      // threadCount: -1 should throw
      expect(() => {
        new RocketMQPushConsumer('G8', { threadCount: -1 });
      }).toThrow();

      // threadCount: 1.5 (non-integer) should throw
      expect(() => {
        new RocketMQPushConsumer('G8a', { threadCount: 1.5 });
      }).toThrow();

      // threadCount: 2.9 (non-integer) should throw
      expect(() => {
        new RocketMQPushConsumer('G8b', { threadCount: 2.9 });
      }).toThrow();

      // threadCount: 2147483648 (Int32 max + 1) should throw
      expect(() => {
        new RocketMQPushConsumer('G8c', { threadCount: 2147483648 });
      }).toThrow();

      // threadCount: 1e20 (way beyond int32) should throw
      expect(() => {
        new RocketMQPushConsumer('G8d', { threadCount: 1e20 });
      }).toThrow();

      // threadCount: 1 should succeed
      const c1 = new RocketMQPushConsumer('G9', { threadCount: 1 });
      expect(c1.status).toBe(Status.STOPPED);

      // threadCount: 8 should succeed
      const c2 = new RocketMQPushConsumer('G10', { threadCount: 8 });
      expect(c2.status).toBe(Status.STOPPED);
    });
  });

  describe('PushConsumer configuration tests', () => {
    test('setListener replaces previous listener', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      consumer.core.setListener(() => {});
      consumer.core.setListener(() => {});
      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('setListener release throw is swallowed', () => {
      const env = { ROCKETMQ_STUB_CONSUMER_RELEASE_THROW: '1' };
      const original = setEnv(env);
      try {
        const consumer = new RocketMQPushConsumer('G1', {});
        consumer.core.setListener(() => {});
        expect(consumer.status).toBe(Status.STOPPED);
      } finally {
        restoreEnv(env, original);
      }
    });

    test('setSessionCredentials validates input', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(consumer.setSessionCredentials('a', 'b', 'c')).toBe(true);
      expect(() => consumer.setSessionCredentials(1 as any, 'b', 'c')).toThrow(/accessKey must be a string/);
      expect(() => consumer.setSessionCredentials('a', 1 as any, 'c')).toThrow(/secretKey must be a string/);
      expect(() => consumer.setSessionCredentials('a', 'b', 1 as any)).toThrow(/onsChannel must be a string/);
    });

    test('setSessionCredentials rejects after consumer has started', async () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      await consumer.start();
      try {
        expect(() => consumer.setSessionCredentials('a', 'b', 'c')).toThrow(/Cannot set session credentials/);
      } finally {
        await consumer.shutdown();
      }
    });

    test('setSessionCredentials rejects while consumer is starting', async () => {
      const env = { ROCKETMQ_STUB_CONSUMER_START_DELAY_MS: '120' };
      const original = setEnv(env);
      const core: any = new binding.PushConsumer('G1', null, {});
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
          core.setSessionCredentials('a', 'b', 'c');
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

    test('subscribe is serialized with native start', async () => {
      const env = {
        ROCKETMQ_STUB_CONSUMER_START_DELAY_MS: '120',
        ROCKETMQ_STUB_CONSUMER_FAIL_ON_CONCURRENT_ACCESS: '1'
      };
      const original = setEnv(env);
      const core: any = new binding.PushConsumer('G1', null, {});
      let started = false;
      try {
        const startPromise = new Promise<void>((resolve, reject) => {
          core.start((err: Error | null) => {
            if (err) {
              reject(err);
              return;
            }
            resolve();
          });
        });
        await nextTick();

        const deadline = Date.now() + 80;
        let concurrentError: Error | null = null;
        while (Date.now() < deadline) {
          try {
            core.subscribe('TopicA', '*');
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            if (/consumer concurrent access/.test(err.message)) {
              concurrentError = err;
              break;
            }
          }
        }

        await startPromise;
        started = true;
        expect(concurrentError).toBeNull();
      } finally {
        if (started) {
          await new Promise<void>((resolve, reject) => {
            core.shutdown((err: Error | null) => {
              if (err) {
                reject(err);
                return;
              }
              resolve();
            });
          });
        }
        restoreEnv(env, original);
      }
    });

    test('setListener is serialized with native start', async () => {
      const env = {
        ROCKETMQ_STUB_CONSUMER_START_DELAY_MS: '120',
        ROCKETMQ_STUB_CONSUMER_FAIL_ON_CONCURRENT_ACCESS: '1'
      };
      const original = setEnv(env);
      const core: any = new binding.PushConsumer('G1', null, {});
      let started = false;
      try {
        const startPromise = new Promise<void>((resolve, reject) => {
          core.start((err: Error | null) => {
            if (err) {
              reject(err);
              return;
            }
            resolve();
          });
        });
        await nextTick();

        const deadline = Date.now() + 80;
        let concurrentError: Error | null = null;
        while (Date.now() < deadline) {
          try {
            core.setListener(() => {});
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            if (/consumer concurrent access/.test(err.message)) {
              concurrentError = err;
              break;
            }
          }
        }

        await startPromise;
        started = true;
        expect(concurrentError).toBeNull();
      } finally {
        if (started) {
          await new Promise<void>((resolve, reject) => {
            core.shutdown((err: Error | null) => {
              if (err) {
                reject(err);
                return;
              }
              resolve();
            });
          });
        }
        restoreEnv(env, original);
      }
    });

    test('core method validation errors', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.core.start(undefined as any)).toThrow(/Function expected/);
      expect(() => consumer.core.shutdown(1 as any)).toThrow(/Function expected/);
      expect(() => (consumer.core as any).subscribe('TopicOnly')).toThrow(/Wrong number of arguments/);
      expect(() => (consumer.core as any).subscribe('TopicOnly', 1 as any)).toThrow(/Topic and expression must be strings/);
      expect(() => (consumer.core as any).setListener(123 as any)).toThrow(/Function expected/);
      expect(() => (consumer.core as any).setSessionCredentials('a', 'b')).toThrow(/Wrong number of arguments/);
      expect(() => (consumer.core as any).setSessionCredentials('a', 1, 'c')).toThrow(/All arguments must be strings/);
    });
  });

  describe('PushConsumer subscribe tests', () => {
    test('subscribe supports topic and expression', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.subscribe('TopicA')).not.toThrow();
      expect(() => consumer.subscribe('TopicA', 'TagA')).not.toThrow();
    });

    test('subscribe propagates native errors', async () => {
      const env = { ROCKETMQ_STUB_CONSUMER_SUBSCRIBE_ERROR: '1' };
      const original = setEnv(env);
      try {
        const consumer = new RocketMQPushConsumer('G1', {});
        expect(() => consumer.subscribe('TopicC')).toThrow(/consumer subscribe error/);
      } finally {
        restoreEnv(env, original);
      }
    });

    test('subscribe throws on empty topic', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.subscribe('')).toThrow(/Topic must be a non-empty string/);
    });

    test('subscribe throws on null topic', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.subscribe(null as any)).toThrow(/Topic must be a non-empty string/);
    });

    test('subscribe throws on invalid expression type', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.subscribe('TopicA', 123 as any)).toThrow(/Expression must be a string if provided/);
    });

    test('subscribe treats null expression as empty string', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      expect(() => consumer.subscribe('TopicA', null as any)).not.toThrow();
    });

    test('subscribe throws when consumer is stopping', async () => {
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        await consumer.start();

        // Start shutdown but don't await
        const shutdownPromise = consumer.shutdown();

        // Try to subscribe while status is STOPPING
        // Since the status changes to STOPPING inside the queue, we need to manually set it
        consumer.status = Status.STOPPING;
        expect(() => consumer.subscribe('TopicA')).toThrow(/Cannot subscribe while consumer is stopping/);

        // Restore status to let shutdown complete
        consumer.status = Status.STARTED;
        await shutdownPromise;
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
      }
    });

    test('subscribe allowed during starting state', () => {
      const consumer = new RocketMQPushConsumer('G1', {}) as any;
      consumer.status = Status.STARTING;
      expect(() => consumer.subscribe('TopicA')).not.toThrow();
    });
  });

  describe('PushConsumer lifecycle and message handling tests', () => {
    test('start and shutdown callback pattern', async () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      await new Promise<void>((resolve, reject) => {
        consumer.start((err: any) => {
          if (err) return reject(err);
          resolve();
        });
      });
      await new Promise<void>((resolve, reject) => {
        consumer.shutdown((err: any) => {
          if (err) return reject(err);
          resolve();
        });
      });
    });

    test('start and shutdown promise path with message event', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', { nameServer: '127.0.0.1' });
        const message = new Promise((resolve) => {
          consumer.once('message', (msg: any, ack: any) => {
            ack.done();
            resolve(msg);
          });
        });
        await consumer.start();
        const msg: any = await message;
        expect(msg.topic).toBe('TopicTest');
        expect(msg.tags).toBe('TagA');
        expect(msg.keys).toBe('KeyA');
        expect(msg.body).toBe('Hello');
        expect(msg.msgId).toBe('MSGID');
        await consumer.shutdown();
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('message ack false path', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        const seen = new Promise((resolve) => {
          consumer.once('message', (msg: any, ack: any) => {
            ack.done(false);
            resolve(msg);
          });
        });
        await consumer.start();
        await seen;
        await consumer.shutdown();
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('external shutdown waits for ack drain when async handler is in-flight', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        let handlerDone = false;
        const messageReceived = new Promise<void>((resolve) => {
          consumer.once('message', async (_msg: any, ack: any) => {
            resolve();
            await new Promise((r) => setTimeout(r, 300));
            ack.done(true);
            handlerDone = true;
          });
        });

        await consumer.start();
        await messageReceived;

        expect(handlerDone).toBe(false);
        await Promise.race([
          consumer.shutdown(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 2000))
        ]);

        // External shutdown is NOT from handler's sync call stack, so fromHandler=false.
        // Shutdown properly waits for the async handler's ack before completing.
        expect(handlerDone).toBe(true);
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('shutdown inside message handler resolves', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        const done = new Promise<void>((resolve, reject) => {
          consumer.once('message', async (_msg: any, ack: any) => {
            try {
              ack.done();
              await consumer.shutdown();
              resolve();
            } catch (err) {
              reject(err);
            }
          });
        });

        await consumer.start();
        await Promise.race([
          done,
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 2000))
        ]);

        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('async handler shutdown resolves without deadlock', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        const done = new Promise<void>((resolve, reject) => {
          consumer.once('message', async (_msg: any, ack: any) => {
            try {
              await new Promise((r) => setTimeout(r, 50));
              ack.done();
              await consumer.shutdown();
              resolve();
            } catch (err) {
              reject(err);
            }
          });
        });

        await consumer.start();
        await Promise.race([
          done,
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 2000))
        ]);
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('async handler: ack then shutdown resolves cleanly', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        const done = new Promise<void>((resolve, reject) => {
          consumer.once('message', async (_msg: any, ack: any) => {
            try {
              await new Promise((r) => setTimeout(r, 50));
              ack.done();
              await consumer.shutdown();
              resolve();
            } catch (err) {
              reject(err);
            }
          });
        });

        await consumer.start();
        await Promise.race([
          done,
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 2500))
        ]);
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('external shutdown resolves without deadlock when handler is in-flight', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        const messageReceived = new Promise<void>((resolve) => {
          consumer.once('message', async (_msg: any, ack: any) => {
            resolve();
            await new Promise((r) => setTimeout(r, 200));
            ack.done(true);
          });
        });

        await consumer.start();
        await messageReceived;

        // Shutdown waits for ack drain: external shutdown (fromHandler=false) properly
        // waits for the async handler to ack before completing.
        await Promise.race([
          consumer.shutdown(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 2000))
        ]);
        consumer = null;
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('handler shutdown before ack.done does not deadlock', async () => {
      const { result, stderr } = runConsumerNodeScenario(`
        const { RocketMQPushConsumer } = require(${JSON.stringify(path.join(repoRoot, 'src/consumer'))});
        const consumer = new RocketMQPushConsumer(group, {});
        let ackCalled = false;
        const done = new Promise((resolve, reject) => {
          consumer.once('message', async (_msg, ack) => {
            try {
              await consumer.shutdown();
              ack.done(true);
              ackCalled = true;
              resolve();
            } catch (err) {
              reject(err);
            }
          });
        });

        await consumer.start();
        await Promise.race([
          done,
          new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown deadlock')), 10000))
        ]);
        return { ackCalled, errors };
      `, {
        ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1'
      });

      expect(result.errors).toEqual([]);
      expect(result.ackCalled).toBe(true);
      expect(stderr).not.toMatch(/Failed to set value in ConsumerAck/);
    });

    test('message handler throws emits error and auto nacks', async () => {
      const env = { ROCKETMQ_STUB_CONSUME_MESSAGE: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        
        const errorPromise = new Promise((resolve) => {
          consumer.once('error', (err: any, msg: any, ack: any) => resolve({ err, msg, ack }));
        });
        
        consumer.once('message', () => {
          throw new Error('handler boom');
        });

        consumer.subscribe('TopicA', '*');
        await consumer.start();

        const result: any = await Promise.race([
          errorPromise,
          new Promise((_, reject) => setTimeout(() => reject(new Error('error event timeout')), 2000))
        ]);

        expect(result.err.message).toMatch(/handler boom/);
        expect(result.msg).toBeTruthy();
        expect(result.ack).toBeTruthy();
      } finally {
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
        restoreEnv(env, original);
      }
    });

    test('registering multiple message listeners is allowed (EventEmitter fan-out)', () => {
      const consumer = new RocketMQPushConsumer('G1', {});
      consumer.on('message', () => {});
      expect(() => {
        consumer.on('message', () => {});
      }).not.toThrow();
      expect(consumer.listenerCount('message')).toBe(2);
    });

    test('message handler throws logs error via console.error without error listener', () => {
      const { result, stderr } = runConsumerNodeScenario(`
        const { RocketMQPushConsumer } = require(${JSON.stringify(path.join(repoRoot, 'src/consumer'))});
        const consumer = new RocketMQPushConsumer(group, {});
        consumer.once('message', () => {
          throw new Error('no listener boom');
        });
        consumer.subscribe('TopicA', '*');
        await consumer.start();
        await tick();
        await tick();
        await consumer.shutdown();
        return { errors };
      `, {
        ROCKETMQ_STUB_CONSUME_MESSAGE: '1'
      });

      expect(result.errors.length).toBe(0);
      expect(stderr).toMatch(/no listener boom/);
    });

    test('async handler rejection logs error via console.error without error listener', () => {
      const { result, stderr } = runConsumerNodeScenario(`
        const { RocketMQPushConsumer } = require(${JSON.stringify(path.join(repoRoot, 'src/consumer'))});
        const consumer = new RocketMQPushConsumer(group, {});
        consumer.once('message', async () => {
          throw new Error('async no listener boom');
        });
        consumer.subscribe('TopicA', '*');
        await consumer.start();
        await tick();
        await tick();
        await consumer.shutdown();
        return { errors };
      `, {
        ROCKETMQ_STUB_CONSUME_MESSAGE: '1'
      });

      expect(result.errors.length).toBe(0);
      expect(stderr).toMatch(/async no listener boom/);
    });
  });

  describe('PushConsumer error handling tests', () => {
    test('start error with Promise pattern', async () => {
      const env1 = { ROCKETMQ_STUB_CONSUMER_START_ERROR: '1' };
      const original1 = setEnv(env1);
      try {
        const consumer = new RocketMQPushConsumer('G1', {});
        await expect(consumer.start()).rejects.toThrow(/consumer start error/);
      } finally {
        restoreEnv(env1, original1);
      }
    });

    test('shutdown error resets to STOPPED (non-retryable)', async () => {
      const env = { ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR: '1' };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        await consumer.start();
        await expect(consumer.shutdown()).rejects.toThrow(/consumer shutdown error/);
        expect(consumer.status).toBe(Status.STOPPED);
        expect(consumer.shutdownCompleted).toBe(true);
        restoreEnv(env, original);
        await expect(consumer.shutdown()).rejects.toThrow(/Consumer is already stopped/);
      } finally {
        restoreEnv(env, original);
      }
    });

    test('shutdown rejects when STARTING', async () => {
      const consumer: any = new RocketMQPushConsumer('G1', {});
      consumer.status = Status.STARTING;
      await expect(consumer.shutdown()).rejects.toThrow(/Consumer is starting, please wait for start to complete/);
    });

    test('start rejects when STARTING', async () => {
      const consumer: any = new RocketMQPushConsumer('G1', {});
      consumer.status = Status.STARTING;
      await expect(consumer.start()).rejects.toThrow(/Consumer is already starting/);
    });

    test('start rejects when STOPPING', async () => {
      const consumer: any = new RocketMQPushConsumer('G1', {});
      consumer.status = Status.STOPPING;
      await expect(consumer.start()).rejects.toThrow(/Consumer is stopping, please wait for shutdown to complete/);
    });

    test('start handles synchronous core exception', async () => {
      vi.resetModules();
      vi.doMock('../src/binding', () => ({
        default: {
          PushConsumer: class {
            setListener() {}
            start() {
              throw new Error('sync start boom');
            }
            shutdown(cb: any) {
              cb(null);
            }
            subscribe() {}
            setSessionCredentials() {}
          }
        }
      }));
      const { RocketMQPushConsumer: MockConsumer } = await import('../src/consumer');
      const consumer = new MockConsumer('G1', {});
      await expect(consumer.start()).rejects.toThrow(/sync start boom/);
      expect(consumer.status).toBe(Status.STOPPED);
      vi.resetModules();
      vi.unmock('../src/binding');
    });

    test('start handles non-Error core exception', async () => {
      vi.resetModules();
      vi.doMock('../src/binding', () => ({
        default: {
          PushConsumer: class {
            setListener() {}
            start() {
              throw 'sync start boom';
            }
            shutdown(cb: any) {
              cb(null);
            }
            subscribe() {}
            setSessionCredentials() {}
          }
        }
      }));
      const { RocketMQPushConsumer: MockConsumer } = await import('../src/consumer');
      const consumer = new MockConsumer('G1', {});
      await expect(consumer.start()).rejects.toThrow(/sync start boom/);
      expect(consumer.status).toBe(Status.STOPPED);
      vi.resetModules();
      vi.unmock('../src/binding');
    });

    test('shutdown handles synchronous core exception (resets to STOPPED, non-retryable)', async () => {
      vi.resetModules();
      vi.doMock('../src/binding', () => ({
        default: {
          PushConsumer: class {
            setListener() {}
            start(cb: any) {
              cb(null);
            }
            shutdown() {
              throw new Error('sync shutdown boom');
            }
            subscribe() {}
            setSessionCredentials() {}
          }
        }
      }));
      const { RocketMQPushConsumer: MockConsumer } = await import('../src/consumer');
      const consumer = new MockConsumer('G1', {});
      consumer.status = Status.STARTED;
      await expect(consumer.shutdown()).rejects.toThrow(/sync shutdown boom/);
      expect(consumer.status).toBe(Status.STOPPED);
      expect((consumer as any).shutdownCompleted).toBe(true);
      vi.resetModules();
      vi.unmock('../src/binding');
    });

    test('shutdown error with Promise pattern (resets to STOPPED, non-retryable)', async () => {
      let consumer: any;
      const env2 = { ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR: '1' };
      const original2 = setEnv(env2);
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        await consumer.start();
        await expect(consumer.shutdown()).rejects.toThrow(/consumer shutdown error/);
        expect(consumer.status).toBe(Status.STOPPED);
        expect(consumer.shutdownCompleted).toBe(true);
        restoreEnv(env2, original2);
        await expect(consumer.shutdown()).rejects.toThrow(/Consumer is already stopped/);
      } finally {
        restoreEnv(env2, original2);
      }
    });

    test('operation queue handles internal error', async () => {
      const originalWarn = process.emitWarning;
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        consumer.operationQueue = Promise.reject(new Error('queue boom'));
        process.emitWarning = (...args: any[]) => {
          (originalWarn as Function).apply(process, args);
        };
        consumer.start(() => {});
        await new Promise<void>((resolve) => {
          setImmediate(resolve);
        });
      } finally {
        process.emitWarning = originalWarn;
        if (consumer) {
          consumer.operationQueue = Promise.resolve();
        }
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
      }
    });

    test('operation queue emits warning even when emitWarning throws', async () => {
      const originalWarn = process.emitWarning;
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        consumer.operationQueue = Promise.reject(new Error('queue boom'));
        process.emitWarning = () => {
          throw new Error('warn boom');
        };
        consumer.start(() => {});
        await new Promise<void>((resolve) => {
          setImmediate(resolve);
        });
      } finally {
        process.emitWarning = originalWarn;
        if (consumer) {
          consumer.operationQueue = Promise.resolve();
        }
        if (consumer && consumer.status === Status.STARTED) {
          await consumer.shutdown();
        }
      }
    });

    test('shutdown error resets to STOPPED (non-retryable, with async consume)', async () => {
      const env = {
        ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1',
        ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR: '1'
      };
      const original = setEnv(env);
      let consumer: any;
      try {
        consumer = new RocketMQPushConsumer('G1', {});
        await consumer.start();
        await expect(consumer.shutdown()).rejects.toThrow(/consumer shutdown error/);
        expect(consumer.status).toBe(Status.STOPPED);
        expect(consumer.shutdownCompleted).toBe(true);
        restoreEnv(env, original);
        await expect(consumer.shutdown()).rejects.toThrow(/Consumer is already stopped/);
      } finally {
        restoreEnv(env, original);
      }
    });

    test('destructor handles shutdown error', async () => {
      const { result, stderr } = runConsumerNodeScenario(`
        const consumer = new binding.PushConsumer(group, null, {});
        await new Promise((resolve, reject) => consumer.start((err) => err ? reject(err) : resolve()));
        consumer.__testForceDestroy();
        return { errors };
      `, {
        ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR: '1'
      });

      expect(result.errors).toEqual([]);
      expect(stderr).toMatch(/Consumer shutdown failed/);
    });

    test('destroyed-during-start retires listener via OnError path', async () => {
      const { result, stderr } = runConsumerNodeScenario(`
        const consumer = new binding.PushConsumer(group, null, {});
        consumer.setListener(() => {});
        consumer.start(() => {});
        consumer.__testForceDestroy();
        await new Promise((r) => setTimeout(r, 200));
        return { errors };
      `);

      expect(result.errors).toEqual([]);
    });

  });

  describe('PushConsumer native edge branches', () => {
    test('native edge cases coverage', async () => {
      function withEnv(values: Record<string, string>, fn: () => any): any {
        const original: Record<string, string | undefined> = {};
        for (const key of Object.keys(values)) {
          original[key] = process.env[key];
          process.env[key] = values[key];
        }
        const done = () => {
          for (const key of Object.keys(values)) {
            if (original[key] === undefined) {
              delete process.env[key];
            } else {
              process.env[key] = original[key];
            }
          }
        };
        const result = fn();
        if (result && typeof result.then === 'function') {
          return result.finally(done);
        }
        done();
        return result;
      }

      async function runCase(env: Record<string, any>): Promise<void> {
        await withEnv(env, async () => {
          let consumer: any;
          const needsUncaughtHandler = !!(
            process.env.ROCKETMQ_STUB_THROW_JS_LISTENER ||
            process.env.ROCKETMQ_STUB_CONSUMER_LISTENER_ERROR
          );
          const swallowUncaught = (): void => {};
          if (needsUncaughtHandler) {
            // Register a persistent listener so that BOTH the synchronous
            // throw path AND any deferred re-throw on the next tick are
            // swallowed. Using `once()` here would leak the handler when
            // no exception fires, polluting subsequent tests' handlers.
            process.on('uncaughtException', swallowUncaught);
          }
          try {
            consumer = new RocketMQPushConsumer('G1', {});
            if (process.env.ROCKETMQ_STUB_THROW_JS_LISTENER) {
              consumer.core.setListener(() => {
                throw new Error('listener error');
              });
            }
            consumer.subscribe('TopicA', '*');
            await consumer.start();
            await consumer.shutdown();
          } catch (err) {
            // Ignore expected errors from stub configurations
          } finally {
            if (consumer && consumer.status !== Status.STOPPED) {
              try {
                await consumer.shutdown();
              } catch (err) {
                // Ignore shutdown errors
              }
            }
            if (needsUncaughtHandler) {
              process.off('uncaughtException', swallowUncaught);
            }
          }
        });
      }

      const cases = [
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_NULL_DATA: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_NULL_ENV: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_ACK_EMPTY: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_ACK_NULL: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_SET: '1', ROCKETMQ_STUB_CONSUMER_LISTENER_ERROR: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_THROW: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_BLOCKING_FAIL: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT_SKIP_CALL: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT_SKIP_CALL: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT_SKIP_CALL: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET: '1', ROCKETMQ_STUB_CONSUMER_PROMISE_PRESET_FALSE: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_ABORT_TSFN: '1', ROCKETMQ_STUB_CONSUMER_TIMEOUT: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_THROW_JS_LISTENER: '1' },
        { ROCKETMQ_STUB_CONSUME_MESSAGE: '1', ROCKETMQ_STUB_CONSUMER_FORCE_FUTURE_ERROR: '1' }
      ];
      for (const env of cases) {
        await runCase(env);
      }
    });
  });

  describe('completion deduplication regression', () => {
    test('start with callback then throw should only settle once', async () => {
      // Regression test for: consumer.ts catch block using finishError() with settled protection
      // This ensures that if this.core[method] callback succeeds but then throws synchronously,
      // the completion is only settled once (not twice)
      const original = setEnv({ ROCKETMQ_STUB_CONSUMER_START_CALLBACK_THEN_THROW: '1' });
      let uncaughtError: Error | null = null;
      const uncaughtHandler = (err: Error) => { uncaughtError = err; };
      process.on('uncaughtException', uncaughtHandler);
      try {
        const consumer = new RocketMQPushConsumer('G1', {});
        let settleCount = 0;

        await new Promise<void>((resolve) => {
          consumer.start(() => {
            settleCount++;
            resolve();
          });
        });

        // Wait for any uncaught exception to propagate
        await new Promise<void>((resolve) => setImmediate(resolve));

        // Should only settle once due to settled protection
        expect(settleCount).toBe(1);
        // The throw in OnOK should have propagated as uncaught exception
        // (not caught by the callback, because it happens after callback returns)
        expect(uncaughtError).toBeInstanceOf(Error);
        // Status should remain STARTED because finishError wasn't called (settled already true)
        // The throw happens after OnOK() which already resolved successfully
        expect(consumer.status).toBe(Status.STARTED);
      } finally {
        process.off('uncaughtException', uncaughtHandler);
        restoreEnv({ ROCKETMQ_STUB_CONSUMER_START_CALLBACK_THEN_THROW: '1' }, original);
      }
    });
  });
});
