import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { RaftNode } from './RaftNode';
import { RaftState } from './StateMachine';
import { RaftError } from '../util/Error';
import { InMemoryStorage } from '../storage/Storage';
import { createConfig } from './Config';
import { MockClock } from '../timing/Clock';
import { SeededRandom } from '../util/Random';
import { ConsoleLogger } from '../util/Logger';

describe('RaftNode.ts, RaftNode', () => {
    const nodeId = 'node1';
    const peers = ['node2', 'node3'];

    let storage: InMemoryStorage;
    let transport: {
        isStarted: ReturnType<typeof vi.fn>;
        start: ReturnType<typeof vi.fn>;
        stop: ReturnType<typeof vi.fn>;
        send: ReturnType<typeof vi.fn>;
        onMessage: ReturnType<typeof vi.fn>;
    };
    let clock: MockClock;
    let random: SeededRandom;
    let appStateMachine: {
        apply: ReturnType<typeof vi.fn>;
        getState: ReturnType<typeof vi.fn>;
    };
    let node: RaftNode;
    let config: ReturnType<typeof createConfig>;

    const forceLeader = (n: RaftNode, term = 1) => {
        const stateMachine = (n as any)['stateMachine'];
        stateMachine['currentState'] = RaftState.Leader;
        stateMachine['currentLeader'] = nodeId;
        const logManager = (n as any)['logManager'];
        stateMachine['leaderState'] = new (require('../state/LeaderState').LeaderState)(peers, logManager.getLastIndex());
        (n as any)['persistentState']['currentTerm'] = term;
    };

    const tickApplyLoop = async (times = 3) => {
        for (let i = 0; i < times; i++) {
            clock.advanceMs(15);
            await clock.tick();
            await new Promise(r => setTimeout(r, 0));
        }
    };

    beforeEach(async () => {
        storage = new InMemoryStorage();
        await storage.open();

        transport = {
            isStarted: vi.fn().mockReturnValue(false),
            start: vi.fn().mockResolvedValue(undefined),
            stop: vi.fn().mockResolvedValue(undefined),
            send: vi.fn().mockResolvedValue(undefined),
            onMessage: vi.fn()
        };

        clock = new MockClock();
        random = new SeededRandom(12345);

        appStateMachine = {
            apply: vi.fn().mockResolvedValue('ok'),
            getState: vi.fn().mockReturnValue({})
        };

        config = createConfig(nodeId, peers, 150, 300, 50);

        node = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random);
    });

    afterEach(async () => {
        try {
            if (node.isStarted()) {
                await node.stop();
            }
        } catch (e) {
        }
    });

    it('should throw when config is invalid', () => {
        expect(() => new RaftNode(
            { ...config, nodeId: '' } as any,
            storage, transport as any, appStateMachine as any, clock, random
        )).toThrow();
    });

    it('should not be started initially', () => {
        expect(node.isStarted()).toBe(false);
    });

    it('should use provided logger if given', () => {
        const logger = {
            info: vi.fn(), debug: vi.fn(), warn: vi.fn(), error: vi.fn()
        };
        const nodeWithLogger = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random, logger);
        expect((nodeWithLogger as any)['logger']).toBe(logger);
    });

    it('should fall back to console if logger is not provided', () => {
        const nodeWithoutLogger = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random);
        expect((nodeWithoutLogger as any)['logger']).toBeInstanceOf(ConsoleLogger);
    });

    it('should start succesfully', async () => {
        await node.start();
        expect(node.isStarted()).toBe(true);
    });

    it('should throw RaftError if already started', async () => {
        await node.start();
        await expect(node.start()).rejects.toThrow(RaftError);
    });

    it('should open storage if not already open on start', async () => {
        const raftNode = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random);

        const openSpy = vi.spyOn(storage, 'open').mockResolvedValue(undefined);
        vi.spyOn(storage, 'isOpen').mockReturnValueOnce(false);

        await raftNode.start();
        expect(openSpy).toHaveBeenCalled();
        await raftNode.stop();
    });

    it('should not re open storage if already open on start', async () => {
        const openSpy = vi.spyOn(storage, 'open').mockResolvedValue(undefined);
        await node.start();
        expect(openSpy).not.toHaveBeenCalled();
    });

    it('should start transport if not already started on start', async () => {
        transport.isStarted.mockReturnValueOnce(false);
        await node.start();
        expect(transport.start).toHaveBeenCalled();
        await node.stop();
    });

    it('should not re start transport if already started on start', async () => {
        transport.isStarted.mockReturnValueOnce(true);
        await node.start();
        expect(transport.start).not.toHaveBeenCalled();
        await node.stop();
    });

    it('should register onMessage handler on transport start', async () => {
        await node.start();
        expect(transport.onMessage).toHaveBeenCalledWith(expect.any(Function));
    });

    it('should initialize with term 0 on first start', async () => {
        await node.start();
        expect(node.getCurrentTerm()).toBe(0);
    });

    it('should initialize with empty log on first start', async () => {
        await node.start();
        expect(node.getLastLogIndex()).toBe(0);
    });

    it('should restore persisted term after restart', async () => {
        await node.start();
        await (node as any)['persistentState'].updateTermAndVote(5, null);
        await node.stop();

        await storage.open();

        const newNode = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random);
        await newNode.start();
        expect(newNode.getCurrentTerm()).toBe(5);
        await newNode.stop();
    });

    it('should restore persisted log after restart', async () => {
        await node.start();
        const logManager = (node as any)['logManager'];
        await logManager.appendCommand({ type: 'set', payload: { key: 'x', value: 10}}, 1);
        await node.stop()

        await storage.open();

        const newNode = new RaftNode(config, storage, transport as any, appStateMachine as any, clock, random);
        await newNode.start();
        expect(newNode.getLastLogIndex()).toBe(1);
        await newNode.stop();
    });

    it('should throw RaftError wrapping underlying error on start failure', async () => {
        transport.start.mockRejectedValue(new Error('Transport failed to start'));
        await expect(node.start()).rejects.toThrow(RaftError);
    });

    it('should start apply loop on start', async () => {
        await node.start();
        expect((node as any)['applyLoopRunning']).toBe(true);
    });

    it('should stop succesfully', async () => {
        await node.start();
        await node.stop();
        expect(node.isStarted()).toBe(false);
    });

    it('should trhow RaftError if not started', async () => {
        await expect(node.stop()).rejects.toThrow(RaftError);
    })

    it('should stop transport if it is started', async () => {
        transport.isStarted.mockReturnValue(true);
        await node.start();
        await node.stop();
        expect(transport.stop).toHaveBeenCalled();
    });

    it('should not stop transport if it is not started', async () => {
        transport.isStarted.mockReturnValue(false);
        await node.start();
        await node.stop();
        expect(transport.stop).not.toHaveBeenCalled();
    });

    it('should close storage on stop', async () => {
        await node.start();
        await node.stop();
        expect(storage.isOpen()).toBe(false);
    });

    it('should stop apply loop on stop', async () => {
        await node.start();
        await node.stop();
        expect((node as any)['applyLoopRunning']).toBe(false);
    });

    it('should trhow RaftError wrapping underlying error on stop failure', async () => {
        await node.start();
        transport.isStarted.mockReturnValue(true);
        transport.stop.mockRejectedValue(new Error('Transport failed to stop'));
        await expect(node.stop()).rejects.toThrow(RaftError);
    });
})
