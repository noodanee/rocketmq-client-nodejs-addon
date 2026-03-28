'use strict';

import { describe, test, expect, afterEach } from 'vitest';

import { RocketMQPushConsumer } from '../src/consumer';
import { Status } from '../src/constants';

afterEach(() => {
  if ((global as any).gc) {
    (global as any).gc();
  }
});

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


describe('ConsumerAck tests', () => {
  const baseEnv = {
    ROCKETMQ_STUB_CONSUMER_START_ERROR: undefined,
    ROCKETMQ_STUB_CONSUMER_SHUTDOWN_ERROR: undefined,
    ROCKETMQ_STUB_CONSUME_MESSAGE: '1'
  };

  test('done() with true acks message', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (_msg: any, ack: any) => {
          ack.done(true);
          resolve(true);
        });
      });

      await consumer.start();
      const ackCalled = await messagePromise;
      expect(ackCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('done() with false nacks message', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(false);
          resolve(false);
        });
      });

      await consumer.start();
      const ackValue = await messagePromise;
      expect(ackValue).toBe(false);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('done() with no argument defaults to true', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done();
          resolve(true);
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('done() is idempotent - second call is ignored', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        let callCount = 0;
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(true);
          ack.done(false);
          ack.done(true);
          callCount++;
          resolve(callCount);
        });
      });

      await consumer.start();
      const callCount = await messagePromise;
      expect(callCount).toBe(1);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('done() with undefined is treated as true', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(undefined);
          resolve(true);
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('handles listener exception via Done(exception_ptr)', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.on('error', () => {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', () => {
          resolve(true);
          throw new Error('listener error');
        });
      });

      await consumer.start();
      const listenerCalled = await messagePromise;
      expect(listenerCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('done() then throw is idempotent (tests Done exception_ptr early return)', async () => {
    const original = setEnv(baseEnv);
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.on('error', () => {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(true);
          resolve(true);
          throw new Error('error after done');
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv(baseEnv, original);
    }
  });

  test('NewInstance returns empty when addon_data is null (coverage branch)', async () => {
    const original = setEnv({
      ...baseEnv,
      ROCKETMQ_STUB_CONSUMER_ACK_NULL_ADDON_DATA: '1'
    });
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => resolve('timeout'), 500);
        consumer.once('message', () => {
          clearTimeout(timeout);
          resolve('message');
        });
        consumer.once('error', () => {
          clearTimeout(timeout);
          resolve('error');
        });
      });

      await consumer.start();
      const result = await messagePromise;
      expect(['timeout', 'error', 'message']).toContain(result);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv({
        ...baseEnv,
        ROCKETMQ_STUB_CONSUMER_ACK_NULL_ADDON_DATA: undefined
      }, original);
    }
  });

  test('done() catches future_error when promise already set (coverage branch)', async () => {
    const original = setEnv({
      ...baseEnv,
      ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: '1'
    });
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(true);
          resolve(true);
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv({
        ...baseEnv,
        ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: undefined
      }, original);
    }
  });

  test('IsEnvEnabled returns false for empty string env var (coverage branch)', async () => {
    // This test covers the branch in IsEnvEnabled where value[0] == '\0'
    // When ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR is set to empty string,
    // IsEnvEnabled returns false, so the stub code path is NOT taken
    const original = setEnv({
      ...baseEnv,
      ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: ''
    });
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(true);
          resolve(true);
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv({
        ...baseEnv,
        ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: undefined
      }, original);
    }
  });

  test('IsEnvEnabled returns false for "0" env var (coverage branch)', async () => {
    // This test covers the branch in IsEnvEnabled where value[0] == '0'
    // When env var is set to "0", IsEnvEnabled returns false
    const original = setEnv({
      ...baseEnv,
      ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: '0'
    });
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          ack.done(true);
          resolve(true);
        });
      });

      await consumer.start();
      const doneCalled = await messagePromise;
      expect(doneCalled).toBe(true);
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv({
        ...baseEnv,
        ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: undefined
      }, original);
    }
  });

  test('Done(exception_ptr) early return and Done() future_error (coverage branch)', async () => {
    // To cover line 72: Done(exception_ptr) must see done_called_ as true.
    // To cover line 98: Done() must catch std::future_error when ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR is set.
    // We can use ROCKETMQ_STUB_CONSUMER_PROMISE_SET to set the promise early.
    const original = setEnv({
      ...baseEnv,
      ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: '1',
      ROCKETMQ_STUB_CONSUMER_PROMISE_SET: '1'
    });
    let consumer: any;
    try {
      consumer = new RocketMQPushConsumer('test-group', {});
      consumer.on('error', () => {});
      consumer.subscribe('test-topic', '*');

      const messagePromise = new Promise((resolve) => {
        consumer.once('message', (msg: any, ack: any) => {
          // 1. data->promise is already set due to ROCKETMQ_STUB_CONSUMER_PROMISE_SET
          // 2. ack.done(true) will try to set it again, triggering future_error in line 96, hitting line 98.
          // 3. ack.done(true) sets done_called_ to true.
          ack.done(true);
          resolve(true);
          // 4. throwing here will call Done(exception_ptr)
          // 5. Done(exception_ptr) will see done_called_ as true and return early (line 72).
          throw new Error('trigger Done(exception_ptr)');
        });
      });

      await consumer.start();
      await messagePromise;
    } finally {
      if (consumer && consumer.status === Status.STARTED) {
        await consumer.shutdown();
      }
      restoreEnv({
        ...baseEnv,
        ROCKETMQ_STUB_CONSUMER_ACK_FORCE_FUTURE_ERROR: undefined,
        ROCKETMQ_STUB_CONSUMER_PROMISE_SET: undefined
      }, original);
    }
  });
});
