# @maboke123/raft-core

A TypeScript implementation of the Raft consensus algorithm.
Handles leader election, log replication, snapshots, and cluster membership changes.

## Install

```bash
npm install @maboke123/raft-core
# Optional: gRPC transport
npm install @maboke123/raft-grpc
```

## Quick start - 3-node cluster in-process

```ts
import {
  RaftNode,
  InMemoryNodeStorage,
} from "@maboke123/raft-core";
import { MockTransport } from "@maboke123/raft-core/testing";

class KeyValueStore {
  private data = new Map<string, unknown>();

  async apply(command: { type: string; payload: any }) {
    if (command.type === "SET") {
      this.data.set(command.payload.key, command.payload.value);
    }
  }

  getState() {
    return Object.fromEntries(this.data);
  }

  async takeSnapshot() {
    return Buffer.from(JSON.stringify(this.getState()));
  }

  async installSnapshot(data: Buffer) {
    const parsed = JSON.parse(data.toString());
    this.data = new Map(Object.entries(parsed));
  }
}

const nodeIds = ["node1", "node2", "node3"];
const nodes: RaftNode[] = [];

for (const nodeId of nodeIds) {
  const peers = nodeIds
    .filter((id) => id !== nodeId)
    .map((id) => ({ id, address: `localhost:${5000 + nodeIds.indexOf(id)}` }));

  const node = new RaftNode({
    config: {
      nodeId,
      address: `localhost:${5000 + nodeIds.indexOf(nodeId)}`,
      peers,
      electionTimeoutMinMs: 150,
      electionTimeoutMaxMs: 300,
      heartbeatIntervalMs: 50,
    },
    storage: new InMemoryNodeStorage(),
    transport: new MockTransport(nodeId),
    stateMachine: new KeyValueStore(),
  });

  nodes.push(node);
}

await Promise.all(nodes.map((n) => n.start()));

await new Promise((resolve) => setTimeout(resolve, 500));

const leader = nodes.find((n) => n.isLeader());
if (!leader) {
  throw new Error("No leader elected");
}

console.log(`Leader: ${leader.getNodeId()}`);

const result = await leader.submitCommand({
  type: "SET",
  payload: { key: "hello", value: "world" },
});

console.log(result);

for (const node of nodes) {
  console.log(node.getNodeId(), node.getApplicationState());
}
```

## Using gRPC transport (real network)

```ts
import { RaftNode, DiskNodeStorage } from "@maboke123/raft-core";
import { GrpcTransport } from "@maboke123/raft-grpc";

class KeyValueStore {
  private data = new Map<string, unknown>();

  async apply(command: { type: string; payload: any }) {
    if (command.type === "SET") {
      this.data.set(command.payload.key, command.payload.value);
    }
  }

  getState() {
    return Object.fromEntries(this.data);
  }

  async takeSnapshot() {
    return Buffer.from(JSON.stringify(this.getState()));
  }

  async installSnapshot(data: Buffer) {
    const parsed = JSON.parse(data.toString());
    this.data = new Map(Object.entries(parsed));
  }
}

const node = new RaftNode({
  config: {
    nodeId: "node1",
    address: "localhost:50051",
    peers: [
      { id: "node2", address: "localhost:50052" },
      { id: "node3", address: "localhost:50053" },
    ],
    electionTimeoutMinMs: 150,
    electionTimeoutMaxMs: 300,
    heartbeatIntervalMs: 50,
  },
  storage: new DiskNodeStorage("./data/node1"),
  transport: new GrpcTransport("node1", 50051, {
    node2: "localhost:50052",
    node3: "localhost:50053",
  }),
  stateMachine: new KeyValueStore(),
});
```

### gRPC TLS certificates

`GrpcTransport` supports two modes:

1. Insecure mode (no `certPaths`) for local development only.
2. Mutual TLS mode (`certPaths` provided) for real deployments.

When using TLS, each node needs:

1. A shared CA certificate file (`caCert`).
2. A node certificate file (`nodeCert`).
3. A node private key file (`nodeKey`).

Example:

```ts
const transport = new GrpcTransport(
  "node1",
  50051,
  {
    node2: "localhost:50052",
    node3: "localhost:50053",
  },
  {
    caCert: "./certs/ca/ca.crt",
    nodeCert: "./certs/node1/node1.crt",
    nodeKey: "./certs/node1/node1.key",
  }
);
```

For local demos, you can generate certificates with [scripts/GenerateCerts.sh](../../scripts/GenerateCerts.sh).
Do not commit private keys in your repository.

## Observing the cluster with events

```ts
import { RaftNode, InMemoryNodeStorage, LocalEventBus } from "@maboke123/raft-core";
import { MockTransport } from "@maboke123/raft-core/testing";

const bus = new LocalEventBus();

const node = new RaftNode({
  config: {
    nodeId: "node1",
    address: "localhost:5001",
    peers: [],
    electionTimeoutMinMs: 150,
    electionTimeoutMaxMs: 300,
    heartbeatIntervalMs: 50,
  },
  storage: new InMemoryNodeStorage(),
  transport: new MockTransport("node1"),
  stateMachine: new MyStateMachine(),
  eventBus: bus,
});

bus.subscribe((event) => {
  if (event.type === "LeaderElected") {
    console.log(`New leader: ${event.leaderId} in term ${event.term}`);
  }
  if (event.type === "FatalError") {
    console.error(`Fatal: ${event.reason}`);
    process.exit(1);
  }
});
```

## Implementing a custom transport

```ts
import type { Transport, MessageHandler, NodeId } from "@maboke123/raft-core";

export class MyWebSocketTransport implements Transport {
  async start() {}

  async stop() {}

  isStarted() {
    return true;
  }

  async send(peerId: NodeId, message: any): Promise<any> {
    return {};
  }

  onMessage(handler: MessageHandler) {
    void handler;
  }
}
```

## Implementing a custom storage backend

```ts
import type {
  NodeStorage,
  MetaStorage,
  LogStorage,
  SnapshotStorage,
  ConfigStorage,
} from "@maboke123/raft-core";

class RedisMetaStorage implements MetaStorage {
  async open() {}
  async close() {}
  isOpen() { return true; }
  async read() { return null; }
  async write() {}
}

class RedisConfigStorage implements ConfigStorage {
  async open() {}
  async close() {}
  isOpen() { return true; }
  async read() { return null; }
  async write() {}
}

class RedisLogStorage implements LogStorage {
  async open() {}
  async close() {}
  isOpen() { return true; }
  async readMeta() { return { snapshotIndex: 0, snapshotTerm: 0, lastIndex: 0, lastTerm: 0 }; }
  async append() {}
  async getEntry() { return null; }
  async getEntries() { return []; }
  async truncateFrom() {}
  async compact() {}
  async reset() {}
}

class RedisSnapshotStorage implements SnapshotStorage {
  async open() {}
  async close() {}
  isOpen() { return true; }
  async save() {}
  async load() { return null; }
  async readMetadata() { return null; }
}

export class RedisNodeStorage implements NodeStorage {
  meta: MetaStorage;
  config: ConfigStorage;
  log: LogStorage;
  snapshot: SnapshotStorage;

  constructor() {
    this.meta = new RedisMetaStorage();
    this.config = new RedisConfigStorage();
    this.log = new RedisLogStorage();
    this.snapshot = new RedisSnapshotStorage();
  }

  async open() {}

  async close() {}

  isOpen() {
    return true;
  }
}
```
