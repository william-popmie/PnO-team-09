import { describe, it, expect } from "vitest";
import { PersistentState } from "./PersistentState";
import { InMemoryStorage} from "../storage/legacy/InMemoryStorage";
import { StorageCodec, StorageOperation } from "../storage/StorageUtil";
import { PersistentStateError, StorageError } from "../util/Error";

describe('PersistentState.ts, PersistentState', () => {

    class FailingStorage extends InMemoryStorage {
        async batch(operations: StorageOperation[]): Promise<void> {
            throw new StorageError('Storage batch error');
        }
    }

    class FailingStorage2 extends InMemoryStorage {
        async batch(operations: StorageOperation[]): Promise<void> {
            throw new Error('Generic error');
        }
    }

    class FailingStorage3 extends InMemoryStorage {
        async get(key: string): Promise<Buffer | null> {
            throw new StorageError('Storage get error');
        }
    }

    it('should initialize with empty storage', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        const snapshot = await persistentState.initialize();
        expect(snapshot.currentTerm).toBe(0);
        expect(snapshot.votedFor).toBeNull();
    });

    it('should return early when already initialized', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        const snapshot = await persistentState.initialize();
        expect(snapshot.currentTerm).toBe(0);
        expect(snapshot.votedFor).toBeNull();
    });

    it('should throw when storage is not open', async () => {
        const storage = new InMemoryStorage();
        const persistentState = new PersistentState(storage);
        await expect(persistentState.initialize()).rejects.toThrow(PersistentStateError);
    });

    it('should throw when calling a method before initialization', () => {
        const storage = new InMemoryStorage();
        const persistentState = new PersistentState(storage);
        expect(() => persistentState.getCurrentTerm()).toThrow(PersistentStateError);
    });

    it('should throw PersistentStateError if restore fails with StorageError', async () => {
        const storage = new FailingStorage3();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await expect(persistentState.initialize()).rejects.toThrow(PersistentStateError);
    });

    it('should restore existing term from storage', async () => {
        const storage = new InMemoryStorage();
        await storage.open();

        await storage.set('raft:currentTerm', StorageCodec.encodeNumber(5));

        const persistentState = new PersistentState(storage);
        const snapshot = await persistentState.initialize();
        expect(snapshot.currentTerm).toBe(5);
        expect(persistentState.getCurrentTerm()).toBe(5);
        expect(persistentState.getVotedFor()).toBeNull();
    });

    it('should throw if votedFor is set but currentTerm is missing', async () => {
        const storage = new InMemoryStorage();
        await storage.open();

        await storage.set('raft:votedFor', StorageCodec.encodeString('node1'));

        const persistentState = new PersistentState(storage);
        await expect(persistentState.initialize()).rejects.toThrow(PersistentStateError);
    });

    it('should get and set current term', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        expect(persistentState.getCurrentTerm()).toBe(0);
        await persistentState.setCurrentTerm(5);
        expect(persistentState.getCurrentTerm()).toBe(5);
    });

    it('should get and set votedFor', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        expect(persistentState.getVotedFor()).toBeNull();
        await persistentState.updateTermAndVote(1, 'node1');
        expect(persistentState.getVotedFor()).toBe('node1');
    });

    it('updateTermandVote should throw for non-integer term', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.updateTermAndVote("not an integer" as any, 'node1')).rejects.toThrow(PersistentStateError);
    });

    it('updateTermandVote should throw for negative term', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.updateTermAndVote(-1, 'node1')).rejects.toThrow(PersistentStateError);
    });

    it('updateTermandVote should throw for new term less than current term', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await persistentState.setCurrentTerm(5);
        await expect(persistentState.updateTermAndVote(4, 'node1')).rejects.toThrow(PersistentStateError);
    });

    it('updateTermandVote should work for votedFor not null', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await persistentState.updateTermAndVote(1, 'node1');
        expect(persistentState.getCurrentTerm()).toBe(1);
        expect(persistentState.getVotedFor()).toBe('node1');
    });

    it('updateTermandVote should work for votedFor null', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await persistentState.updateTermAndVote(1, 'node1');
        await persistentState.updateTermAndVote(2, null);
        expect(persistentState.getCurrentTerm()).toBe(2);
        expect(persistentState.getVotedFor()).toBeNull();
    });

    it('updateTermandVote should return early if term and votedFor are unchanged', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await persistentState.updateTermAndVote(1, 'node1');
        expect(persistentState.getCurrentTerm()).toBe(1);
        expect(persistentState.getVotedFor()).toBe('node1');
        await persistentState.updateTermAndVote(1, 'node1');
        expect(persistentState.getCurrentTerm()).toBe(1);
        expect(persistentState.getVotedFor()).toBe('node1');
    });

    it('updateTermandVote should throw PersistentStateError on StorageError', async () => {
        const storage = new FailingStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.updateTermAndVote(1, 'node1')).rejects.toThrow(PersistentStateError);
    });

    it('updateTermandVote should throw PersistentStateError on generic error', async () => {
        const storage = new FailingStorage2();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.updateTermAndVote(1, 'node1')).rejects.toThrow(PersistentStateError);
    });

    it('clear should reset the state', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await persistentState.updateTermAndVote(1, 'node1');
        await persistentState.clear();
        expect(persistentState.getCurrentTerm()).toBe(0);
        expect(persistentState.getVotedFor()).toBeNull();
    });

    it('clear should throw PersistentStateError on StorageError', async () => {
        const storage = new FailingStorage();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.clear()).rejects.toThrow(PersistentStateError);
    });

    it('clear should throw PersistentStateError on generic error', async () => {
        const storage = new FailingStorage2();
        await storage.open();
        const persistentState = new PersistentState(storage);
        await persistentState.initialize();
        await expect(persistentState.clear()).rejects.toThrow(PersistentStateError);
    });
});