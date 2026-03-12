import { SnapshotStorage, Snapshot, SnapshotMetaData } from "../storage/interfaces/SnapshotStorage";
import { StorageError } from "../util/Error";

export type { SnapshotMetaData, Snapshot };

export interface SnapshotManagerInterface {
    saveSnapshot(snapshot: Snapshot): Promise<void>;
    loadSnapshot(): Promise<Snapshot | null>;
    hasSnapshot(): boolean;
    getSnapshotMetadata(): SnapshotMetaData | null;
}

export class SnapshotManager implements SnapshotManagerInterface {

    private cachedIndex: number = 0;
    private cachedTerm: number = 0;
    private initialized: boolean = false;

    constructor(
        private readonly snapshotStorage: SnapshotStorage
    ) {}

    async initialize(): Promise<SnapshotMetaData | null> {
        if (this.initialized) {
            return this.cachedIndex > 0
                ? { lastIncludedIndex: this.cachedIndex, lastIncludedTerm: this.cachedTerm }
                : null;
        }

        const meta = await this.snapshotStorage.readMetadata();

        if (!meta) {
            this.initialized = true;
            return null;
        }

        this.cachedIndex = meta.lastIncludedIndex;
        this.cachedTerm = meta.lastIncludedTerm;
        this.initialized = true;

        return { lastIncludedIndex: this.cachedIndex, lastIncludedTerm: this.cachedTerm };
    }

    async saveSnapshot(snapshot: Snapshot): Promise<void> {
        this.ensureInitialized();

        await this.snapshotStorage.save(snapshot);

        this.cachedIndex = snapshot.lastIncludedIndex;
        this.cachedTerm = snapshot.lastIncludedTerm;
    }

    async loadSnapshot(): Promise<Snapshot | null> {
        this.ensureInitialized();

        if (this.cachedIndex === 0) {
            return null;
        }

        return await this.snapshotStorage.load();
    }

    hasSnapshot(): boolean {
        this.ensureInitialized();
        return this.cachedIndex > 0;
    }

    getSnapshotMetadata(): SnapshotMetaData | null {
        this.ensureInitialized();
        return this.cachedIndex > 0 ? { lastIncludedIndex: this.cachedIndex, lastIncludedTerm: this.cachedTerm } : null;
    }

    private ensureInitialized(): void {
        if (!this.initialized) {
            throw new StorageError("SnapshotManager is not initialized. Call initialize() before using.");
        }
    }
}