import { StorageOperation } from "../StorageUtil";

export interface Storage {
    get(key: string): Promise<Buffer | null>;
    set(key: string, value: Buffer): Promise<void>;
    delete(key: string): Promise<void>;
    batch(operations: StorageOperation[]): Promise<void>;
    open(): Promise<void>;
    close(): Promise<void>;
    isOpen(): boolean;
}