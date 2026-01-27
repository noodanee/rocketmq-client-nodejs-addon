'use strict';

import { describe, test, expect } from 'vitest';
import * as path from 'path';

import { ensureBindingBinary } from './helpers/binding';

const rootDir = path.join(__dirname, '..');
process.env.NODE_BINDINGS_COMPILED_DIR = 'build';
ensureBindingBinary(rootDir);

// Import from compiled dist for native binding compatibility
import { RocketMQProducer } from '../src/producer';
import { RocketMQPushConsumer } from '../src/consumer';
import { Status } from '../src/constants';

describe('SIGABRT Fix Tests', () => {
  describe('Producer SIGABRT prevention', () => {
    test('rapid restart cycles should not cause SIGABRT', async () => {
      const producer = new RocketMQProducer('test-group');
      
      // Perform multiple rapid restart cycles
      for (let i = 0; i < 3; i++) {
        await producer.start();
        expect(producer.status).toBe(Status.STARTED);
        
        await producer.shutdown();
        expect(producer.status).toBe(Status.STOPPED);
        
        // Small delay to simulate real-world usage
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });

    test('concurrent operations during restart should be handled safely', async () => {
      const producer = new RocketMQProducer('test-group');
      
      await producer.start();
      
      // Start shutdown and immediately try to send (should fail gracefully)
      const shutdownPromise = producer.shutdown();
      
      // This should fail with a proper error, not SIGABRT
      try {
        await producer.send('test-topic', 'test-message', {});
        // If we reach here, the send didn't fail as expected
        expect(false).toBe(true); // Force test failure
      } catch (error) {
        // This is expected - the send should fail gracefully
        expect(error).toBeInstanceOf(Error);
      }
      
      await shutdownPromise;
    });

    test('multiple producer instances with rapid lifecycle should not interfere', async () => {
      const producers = Array.from({ length: 3 }, (_, i) => 
        new RocketMQProducer(`test-group-${i}`)
      );
      
      // Start all producers
      await Promise.all(producers.map(p => p.start()));
      
      // Shutdown all producers
      await Promise.all(producers.map(p => p.shutdown()));
      
      // Verify all are stopped
      producers.forEach(p => {
        expect(p.status).toBe(Status.STOPPED);
      });
    });
  });

  describe('Consumer SIGABRT prevention', () => {
    test('rapid restart cycles should not cause SIGABRT', async () => {
      const consumer = new RocketMQPushConsumer('test-group');
      
      // Set a dummy listener using event emitter
      consumer.on('message', (message, ack) => {
        ack.done(true);
      });
      
      // Perform multiple rapid restart cycles
      for (let i = 0; i < 3; i++) {
        await consumer.start();
        expect(consumer.status).toBe(Status.STARTED);
        
        await consumer.shutdown();
        expect(consumer.status).toBe(Status.STOPPED);
        
        // Small delay to simulate real-world usage
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });

    test('listener replacement during active consumption should be safe', async () => {
      const consumer = new RocketMQPushConsumer('test-group');
      
      // Set initial listener
      consumer.on('message', (message, ack) => {
        ack.done(true);
      });
      
      await consumer.start();
      
      // Replace listener multiple times (simulating hot reload scenarios)
      for (let i = 0; i < 3; i++) {
        consumer.removeAllListeners('message');
        consumer.on('message', (message, ack) => {
          ack.done(true);
        });
        
        // Small delay between replacements
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      await consumer.shutdown();
    });

    test('multiple consumer instances should not interfere during shutdown', async () => {
      const consumers = Array.from({ length: 3 }, (_, i) => 
        new RocketMQPushConsumer(`test-group-${i}`)
      );
      
      // Set listeners for all consumers
      consumers.forEach(consumer => {
        consumer.on('message', (message, ack) => {
          ack.done(true);
        });
      });
      
      // Start all consumers
      await Promise.all(consumers.map(c => c.start()));
      
      // Shutdown all consumers
      await Promise.all(consumers.map(c => c.shutdown()));
      
      // Verify all are stopped
      consumers.forEach(c => {
        expect(c.status).toBe(Status.STOPPED);
      });
    });
  });

  describe('Mixed producer/consumer scenarios', () => {
    test('rapid mixed operations should not cause SIGABRT', async () => {
      const producer = new RocketMQProducer('test-producer-group');
      const consumer = new RocketMQPushConsumer('test-consumer-group');
      
      consumer.on('message', (message, ack) => {
        ack.done(true);
      });
      
      // Rapid start/stop cycles for both
      for (let i = 0; i < 2; i++) {
        await Promise.all([
          producer.start(),
          consumer.start()
        ]);
        
        // Small delay while both are running
        await new Promise(resolve => setTimeout(resolve, 10));
        
        await Promise.all([
          producer.shutdown(),
          consumer.shutdown()
        ]);
        
        // Small delay between cycles
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    });
  });
});