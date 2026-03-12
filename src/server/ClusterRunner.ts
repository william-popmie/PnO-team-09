import { createConfig, NodeId } from "../core/Config";
import { RaftNode } from "../core/RaftNode";
import { RaftEventBus } from "../events/RaftEvents";
import { Command } from "../log/LogEntry";
import { ClusterRunnerInterface, CommittedConfig } from "./ClusterRunnerInterface";
import { InMemoryNodeStorage } from "../storage/inMemory/InMemoryNodeStorage";
import { SystemClock } from "../timing/Clock";
import { TimerConfig } from "../timing/TimerManager";
import { MockTransport } from "../transport/MockTransport";
import { SystemRandom } from "../util/Random";

export interface ClusterRunnerOptions {
    nodeCount: number;
    timerConfig: TimerConfig;
}

class NoOpStateMachine {
    async apply(command: Command): Promise<void> {}
    getState(): null { return null; }
    async takeSnapshot(): Promise<Buffer> { return Buffer.alloc(0); }
    async installSnapshot(snapshot: Buffer): Promise<void> {}
}

export class ClusterRunner implements ClusterRunnerInterface {
    private nodes: Map<NodeId, RaftNode> = new Map();
    private nodeIds: NodeId[] = [];
    private committedConfig: CommittedConfig = { voters: [], learners: [] };

    constructor(
        private bus: RaftEventBus,
        private options: ClusterRunnerOptions
    ) {}

    async start(): Promise<void> {
        const { nodeCount, timerConfig } = this.options;

        this.nodeIds = Array.from({ length: nodeCount }, (_, i) => `node${i + 1}`);

        for (const nodeId of this.nodeIds) {
            const address = `localhost:${52000 + this.nodeIds.indexOf(nodeId)}`;

            const peerMembers = this.nodeIds
                .filter(id => id !== nodeId)
                .map(id => ({ id, address: `localhost:${52000 + this.nodeIds.indexOf(id)}` }));

            const config = createConfig(
                nodeId,
                address,
                peerMembers,
                timerConfig.electionTimeoutMin,
                timerConfig.electionTimeoutMax,
                timerConfig.heartbeatInterval
            );

            const nodeStorage = new InMemoryNodeStorage();
            await nodeStorage.open();

            const node = new RaftNode(
                config,
                nodeStorage,
                new MockTransport(nodeId, new SystemRandom()),
                new NoOpStateMachine(),
                new SystemClock(),
                new SystemRandom(),
                undefined,
                this.bus
            );

            this.nodes.set(nodeId, node);
        }

        for (const node of this.nodes.values()) {
            await node.start();
        }

        this.committedConfig = {
            voters: this.nodeIds.map((id, index) => ({ id, address: `localhost:${52000 + index}` })),
            learners: []
        };
    }

    async stop(): Promise<void> {
        for (const node of this.nodes.values()) {
            if (node.isStarted()) {
                await node.stop();
            }
        }
        MockTransport.reset();
    }

    getNodeIds(): NodeId[] {
        return [...this.nodeIds];
    }

    async crashNode(nodeId: NodeId): Promise<void> {
        const node = this.nodes.get(nodeId);
        if (node && node.isStarted()) {
            await node.stop();
        }
    }

    async recoverNode(nodeId: NodeId): Promise<void> {
        const node = this.nodes.get(nodeId);
        if (node && !node.isStarted()) {
            await node.start();
        }
    }

    partitionNodes(groups: NodeId[][]): void {
        MockTransport.partition(...groups);
    }

    healPartition(): void {
        MockTransport.healPartition();
    }

    setDropRate(nodeId: NodeId, rate: number): void {
        MockTransport.setDropRate(nodeId, rate);
    }

    async submitCommand(command: Command, targetLeaderId?: NodeId): Promise<void> {
        const candidates = targetLeaderId
            ? [this.nodes.get(targetLeaderId)!].filter(Boolean)
            : Array.from(this.nodes.values()).filter(n => n.isStarted() && n.isLeader());

        if (candidates.length === 0) {
            throw new Error('No leader available to submit command');
        }

        const result = await candidates[0].submitCommand(command);
        if (!result.success) {
            throw new Error(`Command submission failed: ${result.error}`);
        }
    }

    cutLink(nodeA: NodeId, nodeB: NodeId): void {
        MockTransport.cutLink(nodeA, nodeB);
        this.bus.emit({
            eventId: crypto.randomUUID(),
            timestamp: performance.now(),
            wallTime: Date.now(),
            type: "LinkCut",
            nodeA,
            nodeB,
        });
    }

    healLink(nodeA: NodeId, nodeB: NodeId): void {
        MockTransport.healLink(nodeA, nodeB);
        this.bus.emit({
            eventId: crypto.randomUUID(),
            timestamp: performance.now(),
            wallTime: Date.now(),
            type: "LinkHealed",
            nodeA,
            nodeB,
        });
    }

    healAllLinks(): void {
        MockTransport.healAllLinks();
        this.bus.emit({
            eventId: crypto.randomUUID(),
            timestamp: performance.now(),
            wallTime: Date.now(),
            type: "AllLinksHealed",
        });
    }

    async addServer(nodeId: NodeId, address: string, asLearner: boolean): Promise<void> {
        if (this.nodes.has(nodeId)) {
            throw new Error(`Node ${nodeId} already exists in the cluster`);
        }

        const port = parseInt(address.split(':')[1]);
        if (isNaN(port)) {
            throw new Error(`Invalid address format: ${address}`);
        }

        const leader = Array.from(this.nodes.values()).find(n => n.isStarted() && n.isLeader());
        if (!leader) {
            throw new Error('No leader available to add server');
        }

        const { timerConfig } = this.options;

        const allCurrentMembers = [
            ...this.committedConfig.voters,
            ...this.committedConfig.learners
        ];

        const peerMembers = allCurrentMembers.map(m => ({ id: m.id, address: m.address }));

        const config = createConfig(
            nodeId,
            address,
            peerMembers,
            timerConfig.electionTimeoutMin,
            timerConfig.electionTimeoutMax,
            timerConfig.heartbeatInterval
        );

        const nodeStorage = new InMemoryNodeStorage();
        await nodeStorage.open();

        const node = new RaftNode(
            config,
            nodeStorage,
            new MockTransport(nodeId, new SystemRandom()),
            new NoOpStateMachine(),
            new SystemClock(),
            new SystemRandom(),
            undefined,
            this.bus
        );

        this.nodes.set(nodeId, node);
        this.nodeIds.push(nodeId);

        await node.start();

        for (const [existingId, existingNode] of this.nodes) {
            if (existingId !== nodeId && existingNode.isStarted()) {
                await existingNode.registerPeer(nodeId, address);
            }
        }

        const success = await leader.addServer(nodeId, address, asLearner);

        if (!success) {
            this.nodes.delete(nodeId);
            this.nodeIds.pop();
            await node.stop();
            throw new Error(`Failed to add server ${nodeId} to the cluster`);
        }

        const member = { id: nodeId, address };
        if (asLearner) {
            this.committedConfig = {
                voters: this.committedConfig.voters,
                learners: [...this.committedConfig.learners, member]
            };
        } else {
            this.committedConfig = {
                voters: [...this.committedConfig.voters, member],
                learners: this.committedConfig.learners
            };
        }
    }

    async removeServer(nodeId: NodeId): Promise<void> {
        const leader = Array.from(this.nodes.values()).find(n => n.isStarted() && n.isLeader());
        if (!leader) {
            throw new Error('No leader available to remove server');
        }

        await leader.removeServer(nodeId);

        const node = this.nodes.get(nodeId);
        if (node && node.isStarted()) {
            await node.stop();
        }

        for (const [existingId, existingNode] of this.nodes) {
            if (existingId !== nodeId && existingNode.isStarted()) {
                await existingNode.removePeer(nodeId);
            }
        }

        this.nodes.delete(nodeId);
        this.nodeIds = this.nodeIds.filter(id => id !== nodeId);
        this.committedConfig = {
            voters: this.committedConfig.voters.filter(m => m.id !== nodeId),
            learners: this.committedConfig.learners.filter(m => m.id !== nodeId)
        };
    }

    async promoteServer(nodeId: NodeId): Promise<void> {
        const leader = Array.from(this.nodes.values()).find(n => n.isStarted() && n.isLeader());
        if (!leader) {
            throw new Error('No leader available to promote server');
        }

        await leader.promoteServer(nodeId);

        const member = this.committedConfig.learners.find(m => m.id === nodeId);
        if (member) {
            this.committedConfig = {
                voters: [...this.committedConfig.voters, member],
                learners: this.committedConfig.learners.filter(m => m.id !== nodeId)
            };
        }
    }

    getCommittedConfig(): CommittedConfig {
        return this.committedConfig;
    }

    isLeader(nodeId: NodeId): boolean {
        const node = this.nodes.get(nodeId);
        return node !== undefined && node.isStarted() && node.isLeader();
    }
}