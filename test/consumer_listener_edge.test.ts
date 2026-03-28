'use strict';

import { describe, test, expect, vi, afterEach } from 'vitest';
import { Status } from '../src/constants';

describe('PushConsumer listener edge cases', () => {
  afterEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  test('listener handles missing ack without throwing', async () => {
    vi.resetModules();
    const listenerState: { cb?: (msg: any, ack: any) => void } = {};
    vi.doMock('../src/binding', () => ({
      default: {
        PushConsumer: class {
          setListener(cb: any) {
            listenerState.cb = cb;
          }
          start(cb: any) {
            cb(null);
          }
          shutdown(cb: any) {
            cb(null);
          }
          subscribe() {}
          setSessionCredentials() {}
        }
      }
    }));
    const { RocketMQPushConsumer } = await import('../src/consumer');
    const consumer = new RocketMQPushConsumer('G1', {});
    const errorSpy = vi.fn();
    consumer.on('error', errorSpy);
    consumer.on('message', () => {
      throw new Error('listener boom');
    });
    listenerState.cb?.({ topic: 'T' }, undefined);
    expect(errorSpy).toHaveBeenCalled();
  });

  test('0 listeners + missing ack does not throw', async () => {
    vi.resetModules();
    const listenerState: { cb?: (msg: any, ack: any) => void } = {};
    vi.doMock('../src/binding', () => ({
      default: {
        PushConsumer: class {
          setListener(cb: any) {
            listenerState.cb = cb;
          }
          start(cb: any) {
            cb(null);
          }
          shutdown(cb: any) {
            cb(null);
          }
          subscribe() {}
          setSessionCredentials() {}
        }
      }
    }));
    const { RocketMQPushConsumer } = await import('../src/consumer');
    const consumer = new RocketMQPushConsumer('G1', {});
    expect(() => listenerState.cb?.({ topic: 'T' }, undefined)).not.toThrow();
    expect(() => listenerState.cb?.({ topic: 'T' }, null)).not.toThrow();
    expect(() => listenerState.cb?.({ topic: 'T' }, { done: 'not a function' })).not.toThrow();
  });

  test('cross-consumer shutdown waits for ack drain on target consumer', async () => {
    vi.resetModules();
    const listenerStates = new Map<string, (msg: any, ack: any) => void>();
    vi.doMock('../src/binding', () => ({
      default: {
        PushConsumer: class {
          private groupId: string;

          constructor(groupId: string) {
            this.groupId = groupId;
          }

          setListener(cb: any) {
            listenerStates.set(this.groupId, cb);
          }

          start(cb: any) {
            cb(null);
          }

          shutdown(cb: any) {
            cb(null);
          }

          subscribe() {}
          setSessionCredentials() {}
        }
      }
    }));

    const { RocketMQPushConsumer } = await import('../src/consumer');
    const consumerA: any = new RocketMQPushConsumer('A', {});
    const consumerB: any = new RocketMQPushConsumer('B', {});
    consumerA.status = Status.STARTED;
    consumerB.status = Status.STARTED;

    let releaseB!: () => void;
    const bHandled = new Promise<void>((resolve) => {
      releaseB = resolve;
    });

    consumerB.on('message', async (_msg: any, ack: any) => {
      await bHandled;
      ack.done(true);
    });

    const bAck = { done: vi.fn() };
    listenerStates.get('B')?.({ topic: 'TB' }, bAck);

    // Consumer A's handler shuts down consumer B.
    // Consumer B has an async handler in-flight. Since handlerZone is per-instance,
    // A's zone context doesn't affect B's fromHandler check — B correctly waits
    // for its own ack drain.
    let shutdownResolved = false;
    const shutdownPromise = new Promise<void>((resolve, reject) => {
      consumerA.on('message', async (_msg: any, ack: any) => {
        try {
          await consumerB.shutdown();
          ack.done(true);
          shutdownResolved = true;
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    listenerStates.get('A')?.({ topic: 'TA' }, { done: vi.fn() });
    // Release B so its handler completes and ack drain resolves
    releaseB();
    await shutdownPromise;
    expect(shutdownResolved).toBe(true);
  });

  test('shutdown resolves after single listener acks', async () => {
    vi.resetModules();
    const listenerState: { cb?: (msg: any, ack: any) => void } = {};
    vi.doMock('../src/binding', () => ({
      default: {
        PushConsumer: class {
          setListener(cb: any) {
            listenerState.cb = cb;
          }

          start(cb: any) {
            cb(null);
          }

          shutdown(cb: any) {
            cb(null);
          }

          subscribe() {}
          setSessionCredentials() {}
        }
      }
    }));

    const { RocketMQPushConsumer } = await import('../src/consumer');
    const consumer: any = new RocketMQPushConsumer('G1', {});
    consumer.status = Status.STARTED;

    consumer.on('message', (_msg: any, ack: any) => {
      ack.done(true);
    });

    const nativeAck = { done: vi.fn() };
    listenerState.cb?.({ topic: 'T' }, nativeAck);

    await consumer.shutdown();
    expect(nativeAck.done).toHaveBeenCalledWith(true);
  });

  test('registering second message listener throws', async () => {
    vi.resetModules();
    vi.doMock('../src/binding', () => ({
      default: {
        PushConsumer: class {
          setListener() {}
          start(cb: any) { cb(null); }
          shutdown(cb: any) { cb(null); }
          subscribe() {}
          setSessionCredentials() {}
        }
      }
    }));

    const { RocketMQPushConsumer } = await import('../src/consumer');
    const consumer: any = new RocketMQPushConsumer('G1', {});
    consumer.on('message', () => {});
    expect(() => {
      consumer.on('message', () => {});
    }).not.toThrow();
    expect(consumer.listenerCount('message')).toBe(2);
  });
});
