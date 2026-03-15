import { StorageError } from "../../util/Error";
import { LogEntry } from "../../log/LogEntry";
import { LogStorage, LogStorageMeta } from "../interfaces/LogStorage";

export class InMemoryLogStorage implements LogStorage {
    private entries: Map<number, LogEntry> = new Map();
    private snapshotIndex = 0;
    private snapshotTerm = 0;
    private lastIndex = 0;
    private lastTerm = 0;
    private isOpenFlag = false;

    async open(): Promise<void> {
        if (this.isOpenFlag) throw new StorageError("InMemoryLogStorage is already open");
        this.isOpenFlag = true;
    }

    async close(): Promise<void> {
        this.ensureOpen();
        this.isOpenFlag = false;
    }

    isOpen(): boolean {
        return this.isOpenFlag;
    }

    async readMeta(): Promise<LogStorageMeta> {
        this.ensureOpen();
        return {
            snapshotIndex: this.snapshotIndex,
            snapshotTerm: this.snapshotTerm,
            lastIndex: this.lastIndex,
            lastTerm: this.lastTerm,
        };
    }

    async append(entries: LogEntry[]): Promise<void> {
        this.ensureOpen();
        if (entries.length === 0) return;

        for (const entry of entries) {
            this.entries.set(entry.index, entry);
        }

        const last = entries[entries.length - 1];
        this.lastIndex = last.index;
        this.lastTerm = last.term;
    }

    async getEntry(index: number): Promise<LogEntry | null> {
        this.ensureOpen();
        if (index <= this.snapshotIndex || index > this.lastIndex) return null;
        return this.entries.get(index) ?? null;
    }

    async getEntries(from: number, to: number): Promise<LogEntry[]> {
        this.ensureOpen();
        const result: LogEntry[] = [];
        for (let i = from; i <= to; i++) {
            const entry = this.entries.get(i);
            if (!entry || i <= this.snapshotIndex) {
                throw new StorageError(`Missing log entry at index ${i}`);
            }
            result.push(entry);
        }
        return result;
    }

    async truncateFrom(index: number): Promise<void> {
        this.ensureOpen();

        for (let i = index; i <= this.lastIndex; i++) {
            this.entries.delete(i);
        }

        const newLastIndex = index - 1;

        if (newLastIndex <= this.snapshotIndex) {
            this.lastIndex = this.snapshotIndex;
            this.lastTerm = this.snapshotTerm;
        } else {
            this.lastIndex = newLastIndex;
            const prev = this.entries.get(newLastIndex);
            this.lastTerm = prev ? prev.term : 0;
        }
    }

    async compact(upToIndex: number, term: number): Promise<void> {
        this.ensureOpen();

        for (let i = this.snapshotIndex + 1; i <= upToIndex; i++) {
            this.entries.delete(i);
        }

        this.snapshotIndex = upToIndex;
        this.snapshotTerm = term;

        if (upToIndex >= this.lastIndex) {
            this.lastIndex = upToIndex;
            this.lastTerm = term;
        }
    }

    async reset(snapshotIndex: number, snapshotTerm: number): Promise<void> {
        this.ensureOpen();
        this.entries.clear();
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.lastIndex = snapshotIndex;
        this.lastTerm = snapshotTerm;
    }

    private ensureOpen(): void {
        if (!this.isOpenFlag) throw new StorageError("InMemoryLogStorage is not open");
    }
}