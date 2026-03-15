import { NodeStorage } from "../interfaces/NodeStorage";
import { InMemoryMetaStorage } from "./InMemoryMetaStorage";
import { InMemoryConfigStorage } from "./InMemoryConfigStorage";
import { InMemoryLogStorage } from "./InMemoryLogStorage";
import { InMemorySnapshotStorage } from "./InMemorySnapshotStorage";

export class InMemoryNodeStorage implements NodeStorage {
    meta: InMemoryMetaStorage;
    config: InMemoryConfigStorage;
    log: InMemoryLogStorage;
    snapshot: InMemorySnapshotStorage;

    constructor() {
        this.meta = new InMemoryMetaStorage();
        this.config = new InMemoryConfigStorage();
        this.log = new InMemoryLogStorage();
        this.snapshot = new InMemorySnapshotStorage();
    }

    async open(): Promise<void> {
        if (!this.meta.isOpen()) await this.meta.open();
        if (!this.config.isOpen()) await this.config.open();
        if (!this.log.isOpen()) await this.log.open();
        if (!this.snapshot.isOpen()) await this.snapshot.open();
    }

    async close(): Promise<void> {
        if (this.meta.isOpen()) await this.meta.close();
        if (this.config.isOpen()) await this.config.close();
        if (this.log.isOpen()) await this.log.close();
        if (this.snapshot.isOpen()) await this.snapshot.close();
    }

    isOpen(): boolean {
        return this.meta.isOpen() && this.config.isOpen() && this.log.isOpen() && this.snapshot.isOpen();
    }
}