import { StorageError } from "../../util/Error";
import { Snapshot, SnapshotMetaData, SnapshotStorage } from "../interfaces/SnapshotStorage";

export class InMemorySnapshotStorage implements SnapshotStorage {
    private snapshot: Snapshot | null = null;
    private isOpenFlag = false;

    async open(): Promise<void> {
        if (this.isOpenFlag) throw new StorageError("InMemorySnapshotStorage is already open");
        this.isOpenFlag = true;
    }

    async close(): Promise<void> {
        this.ensureOpen();
        this.isOpenFlag = false;
    }

    isOpen(): boolean {
        return this.isOpenFlag;
    }

    async readMetadata(): Promise<SnapshotMetaData | null> {
        this.ensureOpen();
        if (!this.snapshot) return null;
        return {
            lastIncludedIndex: this.snapshot.lastIncludedIndex,
            lastIncludedTerm: this.snapshot.lastIncludedTerm,
        };
    }

    async save(snapshot: Snapshot): Promise<void> {
        this.ensureOpen();
        this.snapshot = {
            lastIncludedIndex: snapshot.lastIncludedIndex,
            lastIncludedTerm: snapshot.lastIncludedTerm,
            data: Buffer.from(snapshot.data),
            config: {
                voters: snapshot.config.voters.map(m => ({ ...m })),
                learners: snapshot.config.learners.map(m => ({ ...m })),
            },
        };
    }

    async load(): Promise<Snapshot | null> {
        this.ensureOpen();
        if (!this.snapshot) return null;
        return {
            lastIncludedIndex: this.snapshot.lastIncludedIndex,
            lastIncludedTerm: this.snapshot.lastIncludedTerm,
            data: Buffer.from(this.snapshot.data),
            config: {
                voters: this.snapshot.config.voters.map(m => ({ ...m })),
                learners: this.snapshot.config.learners.map(m => ({ ...m })),
            },
        };
    }

    private ensureOpen(): void {
        if (!this.isOpenFlag) throw new StorageError("InMemorySnapshotStorage is not open");
    }
}