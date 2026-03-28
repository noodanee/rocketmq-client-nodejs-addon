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

describe('SIGABRT Fix Tests', () => {
  afterEach(() => {
    clearEnv();
  });

  describe('Producer SIGABRT prevention', () => {
    test('rapid restart cycles should not cause SIGABRT', async () => {
      for (let i = 0; i < 3; i++) {
        const producer = new RocketMQProducer(uniqueGroupName());
        await producer.start();
        expect(producer.status).toBe(Status.STARTED);

        await producer.shutdown();
        expect(producer.status).toBe(Status.STOPPED);

        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });

    test('shutdown with inflight async send should complete gracefully', async () => {
      setEnv({ ROCKETMQ_STUB_SEND_ASYNC_DELAY_MS: '100' });

      const producer = new RocketMQProducer(uniqueGroupName());
      await producer.start();

      const sendPromise = producer.send('test-topic', 'test-message', {});
      const shutdownPromise = producer.shutdown();

      const results = await Promise.allSettled([sendPromise, shutdownPromise]);

      // Both should settle without SIGABRT; send may succeed or fail gracefully
      expect(results[1].status).toBe('fulfilled');
    });

    test('concurrent operations during restart should be handled safely', async () => {
      const producer = new RocketMQProducer(uniqueGroupName());

      await producer.start();

      const shutdownPromise = producer.shutdown();

      await expect(producer.send('test-topic', 'test-message', {})).rejects.toThrow();

      await shutdownPromise;
    });

    test('multiple producer instances with rapid lifecycle should not interfere', async () => {
      const producers = Array.from({ length: 3 }, (_, i) =>
        new RocketMQProducer(uniqueGroupName(`test-group-${i}`))
      );

      await Promise.all(producers.map(p => p.start()));
      await Promise.all(producers.map(p => p.shutdown()));

      producers.forEach(p => {
        expect(p.status).toBe(Status.STOPPED);
      });
    });
  });

  describe('Consumer SIGABRT prevention', () => {
    test('rapid lifecycle with new instances should not cause SIGABRT', async () => {
      for (let i = 0; i < 3; i++) {
        const consumer = new RocketMQPushConsumer(uniqueGroupName());
        consumer.on('message', (message, ack) => { ack.done(true); });

        await consumer.start();
        expect(consumer.status).toBe(Status.STARTED);

        await consumer.shutdown();
        expect(consumer.status).toBe(Status.STOPPED);

        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });

    test('listener replacement during active consumption should be safe', async () => {
      setEnv({ ROCKETMQ_STUB_CONSUME_MESSAGE_ASYNC: '1' });

      const consumer = new RocketMQPushConsumer(uniqueGroupName());
      const received: number[] = [];

      consumer.on('message', (message, ack) => {
        received.push(0);
        ack.done(true);
      });

      await consumer.start();

      // Replace EventEmitter handlers while async message delivery is in-flight
      consumer.removeAllListeners('message');
      consumer.on('message', (message, ack) => {
        received.push(1);
        ack.done(true);
      });

      // Wait for async message delivery (stub fires after 100ms)
      await new Promise(resolve => setTimeout(resolve, 300));

      await consumer.shutdown();

      // Message should have been received by one of the handlers
      expect(received.length).toBeGreaterThanOrEqual(1);
    });

    test('lifecycle with message delivery should not cause SIGABRT', async () => {
      setEnv({ ROCKETMQ_STUB_CONSUME_MESSAGE: '1' });

      for (let i = 0; i < 3; i++) {
        const consumer = new RocketMQPushConsumer(uniqueGroupName());
        const messages: string[] = [];

        consumer.on('message', (msg, ack) => {
          messages.push(msg.body);
          ack.done(true);
        });

        await consumer.start();
        expect(consumer.status).toBe(Status.STARTED);

        await consumer.shutdown();
        expect(consumer.status).toBe(Status.STOPPED);

        expect(messages.length).toBe(1);
      }
    });

    test('multiple consumer instances should not interfere during shutdown', async () => {
      const consumers = Array.from({ length: 3 }, (_, i) =>
        new RocketMQPushConsumer(uniqueGroupName(`test-group-${i}`))
      );

      consumers.forEach(consumer => {
        consumer.on('message', (message, ack) => {
          ack.done(true);
        });
      });

      await Promise.all(consumers.map(c => c.start()));
      await Promise.all(consumers.map(c => c.shutdown()));

      consumers.forEach(c => {
        expect(c.status).toBe(Status.STOPPED);
      });
    });
  });

  describe('Mixed producer/consumer scenarios', () => {
    test('rapid mixed operations should not cause SIGABRT', async () => {
      for (let i = 0; i < 2; i++) {
        const producer = new RocketMQProducer(uniqueGroupName('test-producer-group'));
        const consumer = new RocketMQPushConsumer(uniqueGroupName('test-consumer-group'));

        consumer.on('message', (message, ack) => {
          ack.done(true);
        });

        await Promise.all([
          producer.start(),
          consumer.start()
        ]);

        await new Promise(resolve => setTimeout(resolve, 10));

        await Promise.all([
          producer.shutdown(),
          consumer.shutdown()
        ]);

        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });
  });
});
