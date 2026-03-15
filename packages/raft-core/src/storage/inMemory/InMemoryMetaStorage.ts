import { StorageError } from "../../util/Error";
import { MetaData, MetaStorage } from "../interfaces/MetaStorage";
import { NodeId } from "../../core/Config";

export class InMemoryMetaStorage implements MetaStorage {
    private data: MetaData | null = null;
    private isOpenFlag = false;

    async open(): Promise<void> {
        if (this.isOpenFlag) throw new StorageError("InMemoryMetaStorage is already open");
        this.isOpenFlag = true;
    }

    async close(): Promise<void> {
        this.ensureOpen();
        this.isOpenFlag = false;
    }

    isOpen(): boolean {
        return this.isOpenFlag;
    }

    async read(): Promise<MetaData | null> {
        this.ensureOpen();
        return this.data ? { ...this.data } : null;
    }

    async write(term: number, votedFor: NodeId | null): Promise<void> {
        this.ensureOpen();
        this.data = { term, votedFor };
    }

    private ensureOpen(): void {
        if (!this.isOpenFlag) throw new StorageError("InMemoryMetaStorage is not open");
    }
}