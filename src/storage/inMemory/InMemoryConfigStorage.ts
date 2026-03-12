import { StorageError } from "../../util/Error";
import { ClusterMember } from "../../config/ClusterConfig";
import { ConfigStorage, ConfigStorageData } from "../interfaces/ConfigStorage";

export class InMemoryConfigStorage implements ConfigStorage {
    private data: ConfigStorageData | null = null;
    private isOpenFlag = false;

    async open(): Promise<void> {
        if (this.isOpenFlag) throw new StorageError("InMemoryConfigStorage is already open");
        this.isOpenFlag = true;
    }

    async close(): Promise<void> {
        this.ensureOpen();
        this.isOpenFlag = false;
    }

    isOpen(): boolean {
        return this.isOpenFlag;
    }

    async read(): Promise<ConfigStorageData | null> {
        this.ensureOpen();
        return this.data
            ? { voters: [...this.data.voters], learners: [...this.data.learners] }
            : null;
    }

    async write(voters: ClusterMember[], learners: ClusterMember[]): Promise<void> {
        this.ensureOpen();
        this.data = {
            voters: voters.map(m => ({ ...m })),
            learners: learners.map(m => ({ ...m })),
        };
    }

    private ensureOpen(): void {
        if (!this.isOpenFlag) throw new StorageError("InMemoryConfigStorage is not open");
    }
}