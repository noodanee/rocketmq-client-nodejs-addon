'use strict';

import { describe, test, expect, afterEach } from 'vitest';
import { randomBytes } from 'crypto';

import { RocketMQProducer } from '../src/producer';
import { RocketMQPushConsumer } from '../src/consumer';
import { Status } from '../src/constants';

let testCounter = 0;
function uniqueGroupName(prefix: string = 'test-group'): string {
  const suffix = randomBytes(6).toString('hex');
  return `${prefix}-${process.pid}-${Date.now()}-${testCounter++}-${suffix}`;
}

const envKeys: string[] = [];

function setEnv(env: Record<string, string>): void {
  for (const [key, value] of Object.entries(env)) {
    envKeys.push(key);
    process.env[key] = value;
  }
}

function clearEnv(): void {
  for (const key of envKeys) {
    delete process.env[key];
  }
  envKeys.length = 0;
}

describe('Race Condition Fix Tests', () => {
  afterEach(() => {
    clearEnv();
  });

  describe('Producer race condition tests', () => {
    test('concurrent start calls - second call should be rejected properly', async () => {
      const producer = new RocketMQProducer(uniqueGroupName());

      const promise1 = producer.start();
      const promise2 = producer.start();

      const results = await Promise.allSettled([promise1, promise2]);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect((results[1] as PromiseRejectedResult).reason.message).toMatch(/Producer is already (starting|started)/);

      expect(producer.status).toBe(Status.STARTED);

      await producer.shutdown();
    });

    test('start failure permanently disables producer (no retry)', async () => {
      setEnv({ ROCKETMQ_STUB_PRODUCER_START_ERROR: '1' });

      const producer = new RocketMQProducer(uniqueGroupName());

      await expect(producer.start()).rejects.toThrow(/producer start error/);
      expect(producer.status).toBe(Status.STOPPED);

      clearEnv();

      await expect(producer.start()).rejects.toThrow(/Producer cannot be restarted after shutdown/);
    });

    test('concurrent operations are properly serialized', async () => {
      const producer = new RocketMQProducer(uniqueGroupName());

      const operations = [
        producer.start(),
        producer.start(),
        producer.start()
      ];

      const results = await Promise.allSettled(operations);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect(results[2].status).toBe('rejected');

      expect(producer.status).toBe(Status.STARTED);
      await producer.shutdown();
    });

    test('shutdown with inflight async send should wait for pending callbacks', async () => {
      setEnv({ ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '50' });

      const producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();

      const sendPromise = producer.send('test-topic', 'test-message', {});
      const shutdownPromise = producer.shutdown();

      const results = await Promise.allSettled([sendPromise, shutdownPromise]);

      // Shutdown should succeed after waiting for the inflight send
      expect(results[1].status).toBe('fulfilled');
      expect(producer.status).toBe(Status.STOPPED);
    });
  });

  describe('Consumer race condition tests', () => {
    test('concurrent start calls - second call should be rejected properly', async () => {
      const consumer = new RocketMQPushConsumer(uniqueGroupName());

      const promise1 = consumer.start();
      const promise2 = consumer.start();

      const results = await Promise.allSettled([promise1, promise2]);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect((results[1] as PromiseRejectedResult).reason.message).toMatch(/Consumer is already (starting|started)/);

      expect(consumer.status).toBe(Status.STARTED);

      await consumer.shutdown();
    });

    test('start failure permanently disables consumer (no retry)', async () => {
      setEnv({ ROCKETMQ_STUB_CONSUMER_START_ERROR: '1' });

      const consumer = new RocketMQPushConsumer(uniqueGroupName());

      await expect(consumer.start()).rejects.toThrow(/consumer start error/);
      expect(consumer.status).toBe(Status.STOPPED);

      clearEnv();

      await expect(consumer.start()).rejects.toThrow(/Consumer cannot be restarted after shutdown/);
    });

    test('operations are properly queued and serialized', async () => {
      const consumer = new RocketMQPushConsumer(uniqueGroupName());

      await consumer.start();
      expect(consumer.status).toBe(Status.STARTED);

      const shutdownPromise1 = consumer.shutdown();
      const shutdownPromise2 = consumer.shutdown();

      const results = await Promise.allSettled([shutdownPromise1, shutdownPromise2]);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect(consumer.status).toBe(Status.STOPPED);
    });

    test('start after shutdown rejects (one-time-use consumer)', async () => {
      const consumer = new RocketMQPushConsumer(uniqueGroupName());
      consumer.on('message', (msg, ack) => { ack.done(true); });

      await consumer.start();
      expect(consumer.status).toBe(Status.STARTED);
      await consumer.shutdown();
      expect(consumer.status).toBe(Status.STOPPED);

      await expect(consumer.start()).rejects.toThrow(/cannot be restarted after shutdown/);
    });

    test('native operations should be serialized under concurrent access probe', async () => {
      setEnv({
        ROCKETMQ_STUB_CONSUMER_FAIL_ON_CONCURRENT_ACCESS: '1',
        ROCKETMQ_STUB_CONSUMER_START_DELAY_MS: '10',
      });

      const consumer = new RocketMQPushConsumer(uniqueGroupName());
      consumer.subscribe('test-topic', '*');

      // If our native locking is wrong, the stub throws on concurrent access
      await consumer.start();
      await consumer.shutdown();

      // Restart is now prohibited (one-time-use semantics)
      await expect(consumer.start()).rejects.toThrow(/cannot be restarted after shutdown/);
    });
  });
});
