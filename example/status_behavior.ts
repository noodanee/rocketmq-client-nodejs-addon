import { RocketMQProducer } from '../src/producer';
import { Status } from '../src/constants';

async function demonstrateStatusBehavior(): Promise<void> {
  const producer = new RocketMQProducer('demo-group');

  console.log('=== 新的状态行为演示 ===');
  console.log('初始状态:', getStatusName(producer.status)); // STOPPED

  console.log('\n1. 调用 start() 后状态不会立即改变');
  const startPromise = producer.start();
  console.log('调用 start() 后立即检查状态:', getStatusName(producer.status)); // 可能还是 STOPPED

  await startPromise;
  console.log('start() 完成后状态:', getStatusName(producer.status)); // STARTED

  console.log('\n2. 并发调用会被正确处理');
  const promise1 = producer.start(); // 应该失败
  const promise2 = producer.start(); // 应该失败

  try {
    await promise1;
    console.log('第一个 start() 调用成功');
  } catch (err: any) {
    console.log('第一个 start() 调用失败:', err.message);
  }

  try {
    await promise2;
    console.log('第二个 start() 调用成功');
  } catch (err: any) {
    console.log('第二个 start() 调用失败:', err.message);
  }

  console.log('\n3. 清理');
  await producer.shutdown();
  console.log('最终状态:', getStatusName(producer.status)); // STOPPED
}

function getStatusName(status: Status): string {
  switch (status) {
  case Status.STOPPED: return 'STOPPED';
  case Status.STARTED: return 'STARTED';
  case Status.STARTING: return 'STARTING';
  case Status.STOPPING: return 'STOPPING';
  default: return 'UNKNOWN';
  }
}

demonstrateStatusBehavior().catch(console.error);
