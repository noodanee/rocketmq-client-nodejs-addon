'use strict';

import { describe, test, expect } from 'vitest';
import * as path from 'path';
import * as childProcess from 'child_process';

const rootDir = path.join(__dirname, '..');

function runInitWithEnv(extraEnv: Record<string, string>): Buffer {
  const env = { ...process.env, ...extraEnv, NODE_BINDINGS_COMPILED_DIR: 'build' };
  return childProcess.execFileSync(
    process.execPath,
    ['-e', 'require(\'./dist/binding\')'],
    { cwd: rootDir, env }
  );
}

describe('RocketMQ initialization tests', () => {
  test('module init without debug flag', () => {
    expect(() => {
      runInitWithEnv({ ROCKETMQ_DEBUG_STACK: '' });
    }).not.toThrow();
  });

  test('module init with debug flag', () => {
    expect(() => {
      runInitWithEnv({ ROCKETMQ_DEBUG_STACK: '1' });
    }).not.toThrow();
  });
});

describe('RocketMQ index.ts exports', () => {
  test('Producer alias export', async () => {
    const { Producer, RocketMQProducer } = await import('../src/index');
    expect(Producer).toBe(RocketMQProducer);
    expect(typeof Producer).toBe('function');
  });

  test('PushConsumer alias export', async () => {
    const { PushConsumer, RocketMQPushConsumer } = await import('../src/index');
    expect(PushConsumer).toBe(RocketMQPushConsumer);
    expect(typeof PushConsumer).toBe('function');
  });

  test('LogLevel and Status exports', async () => {
    const { LogLevel, Status } = await import('../src/index');
    expect(LogLevel).toBeTruthy();
    expect(Status).toBeTruthy();
    expect(Status.STOPPED).toBe(0);
    expect(Status.STARTED).toBe(1);
  });

  test('SendResultStatus export', async () => {
    const { SendResultStatus } = await import('../src/index');
    expect(SendResultStatus).toBeTruthy();
    expect(SendResultStatus.OK).toBe(0);
  });
});
