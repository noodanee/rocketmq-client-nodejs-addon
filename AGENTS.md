# CLAUDE.md

## Quick Start

```bash
npm run build:test      # 构建测试版本 (含 C++ stub，无需真实 RocketMQ)
npm run vitest          # 跑测试
npm run validate        # typecheck + lint + test 全量验证
```

## Architecture

Native addon (C++) ↔ TypeScript wrapper ↔ User code

```
lib/                    # C++ N-API 实现 (node-addon-api)
├── rocketmq.cpp        #   addon 入口，注册 Producer/PushConsumer
├── producer.{h,cpp}    #   Producer: start/shutdown/send (AsyncWorker + TSFN)
├── push_consumer.{h,cpp}  # PushConsumer: start/shutdown/subscribe/listener
├── consumer_ack.{h,cpp}   # ACK 回调对象
├── common_utils.{h,cpp}   # 共享工具 (参数校验、日志配置)
└── addon_data.h        #   per-addon instance data

src/                    # TypeScript 封装
├── binding.ts          #   native binary 加载 + 接口类型声明
├── constants.ts        #   Status enum, LogLevel enum
├── producer.ts         #   RocketMQProducer class (Promise 队列串行化)
├── consumer.ts         #   RocketMQPushConsumer class (EventEmitter)
└── index.ts            #   public API re-export

test/                   # Vitest + C++ stub
├── mocks/rocketmq/    #   C++ stub 实现 (替代真实 SDK)
├── helpers/            #   测试辅助函数
└── *.test.ts           #   各模块测试

deps/rocketmq/          # 真实 RocketMQ C++ SDK (仅生产构建)
```

## Build Commands

```bash
npm run build           # 完整构建 (native + TS)
npm run build:native    # 仅 native (真实 SDK)
npm run build:test      # native stub + TS (测试用，快)
npm run build:ts        # 仅 TypeScript → dist/
```

## Test Commands

```bash
npm run test            # build:test + vitest
npm run vitest          # 仅跑测试 (需先 build:test)
npm run test:coverage   # 带覆盖率 (阈值: lines/functions 80%, branches 70%)
```

## Verify (CI 等价)

```bash
npm run validate        # typecheck && lint && test
```

## Key Conventions

### C++ 层

- **Lifecycle 是一次性的**：Producer/Consumer start 失败或 shutdown 后不可重新 start
- **状态机**：`LifecycleState` enum + `std::atomic` + `TryTransitionState()` CAS
- **异步模式**：
  - start/shutdown → `Napi::AsyncWorker` (在 libuv 线程池执行)
  - send callback → `napi_threadsafe_function` (SDK 线程回调 → JS 主线程)
- **参数校验**：入口处用 `utils::ValidateCallback` / `ValidateStringArguments`
- **命名空间**：所有代码在 `__node_rocketmq__` 内
- **条件编译**：`ROCKETMQ_USE_STUB` 启用 stub；`ROCKETMQ_COVERAGE` 启用覆盖率

### TypeScript 层

- **Promise/Callback 双模式**：所有异步方法同时支持两种调用风格
- **串行化**：start/shutdown 通过 `operationQueue` Promise 链保证串行
- **状态检查**：send 前检查 `this.status === Status.STARTED`
- **优雅关闭**：shutdown 时 drain pending sends，reject 所有未完成的 send

### 测试

- 测试使用 C++ stub（`test/mocks/rocketmq/`），不依赖真实 broker
- `pool: 'forks'` + `fileParallelism: false` 确保 native addon 隔离
- `--expose-gc` 用于 GC 相关测试

## Breaking Changes (v1.1.2 → v2.0.0)

- Producer/Consumer 变为一次性对象：start 失败或 shutdown 后不可重新 start
- shutdown() 失败后状态重置为 STOPPED（不再保留 STOPPING）
- 真实 SDK 不支持 start 失败后重试，JS 封装已对齐此行为