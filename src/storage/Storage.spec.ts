import { describe, it, expect } from "vitest";
import { InMemoryStorage, StorageOperation, StorageCodec } from "./Storage";

describe('Storage.ts, InMemoryStorage', () => {
    it('should open and close storage', async () => {
        const storage = new InMemoryStorage();
        expect(storage.isOpen()).toBe(false);
        await storage.open();
        expect(storage.isOpen()).toBe(true);
        await storage.close();
        expect(storage.isOpen()).toBe(false);
    });

    it('should throw if already open or closed', async () => {
        const storage = new InMemoryStorage();
        await expect(storage.close()).rejects.toThrow("Storage is not open");
        await storage.open();
        await expect(storage.open()).rejects.toThrow("Storage is already open");
        await storage.close();
    });

    it('should set and get values', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const key = "testKey";
        const value = Buffer.from("testValue");
        await storage.set(key, value);
        const retrieved = await storage.get(key);
        expect(retrieved).toEqual(value);
        await storage.close();
    });

    it('should delete values', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const key = "testKey";
        const value = Buffer.from("testValue");
        await storage.set(key, value);
        await storage.delete(key);
        const retrieved = await storage.get(key);
        expect(retrieved).toBeNull();
        await storage.close();
    });

    it('should perform batch operations', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const operations: StorageOperation[] = [
            { type: "set", key: "key1", value: Buffer.from("value1") },
            { type: "set", key: "key2", value: Buffer.from("value2") },
            { type: "delete", key: "key1" }
        ];
        await storage.batch(operations);
        const value1 = await storage.get("key1");
        const value2 = await storage.get("key2");
        expect(value1).toBeNull();
        expect(value2).toEqual(Buffer.from("value2"));
        await storage.close();
    });

    it('should throw when an operation in batch is set and value is missing', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const operations: StorageOperation[] = [
            { type: "set", key: "key1" },
        ];
        await expect(storage.batch(operations)).rejects.toThrow("Value is required for set operation");
        await storage.close();
    });

    it('should throw when an operation in batch is not set or delete', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        const operations: StorageOperation[] = [
            { type: "invalid" as any, key: "key1" },
        ];
        await expect(storage.batch(operations)).rejects.toThrow("Invalid operation type");
        await storage.close();
    });

    it('should return keys', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        await storage.set("key1", Buffer.from("value1"));
        await storage.set("key2", Buffer.from("value2"));
        const keys = storage.keys();
        expect(keys.sort()).toEqual(["key1", "key2"]);
        await storage.close();
    });

    it('should return values', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        await storage.set("key1", Buffer.from("value1"));
        await storage.set("key2", Buffer.from("value2"));
        const values = storage.values();
        expect(values).toEqual([Buffer.from("value1"), Buffer.from("value2")]);
        await storage.close();
    });

    it('should return entries', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        await storage.set("key1", Buffer.from("value1"));
        await storage.set("key2", Buffer.from("value2"));
        const entries = storage.entries();
        expect(entries).toEqual([["key1", Buffer.from("value1")], ["key2", Buffer.from("value2")]]);
        await storage.close();
    });

    it('should clear all data', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        await storage.set("key1", Buffer.from("value1"));
        await storage.set("key2", Buffer.from("value2"));
        storage.clear();
        const keys = storage.keys();
        expect(keys).toEqual([]);
        await storage.close();
    });

    it('should return size of storage', async () => {
        const storage = new InMemoryStorage();
        await storage.open();
        expect(storage.size()).toBe(0);
        await storage.set("key1", Buffer.from("value1"));
        expect(storage.size()).toBe(1);
        await storage.set("key2", Buffer.from("value2"));
        expect(storage.size()).toBe(2);
        await storage.delete("key1");
        expect(storage.size()).toBe(1);
        await storage.close();
    });
});

describe('Storage.ts, StorageCodec', () => {
    it('should encode and decode numbers', () => {
        const number = 12345;
        const encoded = StorageCodec.encodeNumber(number);
        const decoded = StorageCodec.decodeNumber(encoded);
        expect(decoded).toBe(number);
    });

    it('should throw when encoding a non-safe integer', () => {
        const unsafeNumber = Number.MAX_SAFE_INTEGER + 1;
        expect(() => StorageCodec.encodeNumber(unsafeNumber)).toThrow("Number 9007199254740992 must be a safe integer");
    });

    it('should throw when decoding a buffer of incorrect length', () => {
        const invalidBuffer = Buffer.alloc(4);
        expect(() => StorageCodec.decodeNumber(invalidBuffer)).toThrow("Buffer length must be 8 bytes");
    });

    it('should throw when decoded number is not a safe integer', () => {
        const invalidBuffer = Buffer.alloc(8);
        invalidBuffer.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1), 0);
        expect(() => StorageCodec.decodeNumber(invalidBuffer)).toThrow("Decoded number 9007199254740992 is not a safe integer");
    });

    it('should encode and decode zero', () => {
        const number = 0;
        const encoded = StorageCodec.encodeNumber(number);
        const decoded = StorageCodec.decodeNumber(encoded);
        expect(decoded).toBe(number);
    });

    it('should encode and decode negative numbers', () => {
        const number = -12345;
        const encoded = StorageCodec.encodeNumber(number);
        const decoded = StorageCodec.decodeNumber(encoded);
        expect(decoded).toBe(number);
    });

    it('should encode and decode the maximum safe integer', () => {
        const number = Number.MAX_SAFE_INTEGER;
        const encoded = StorageCodec.encodeNumber(number);
        const decoded = StorageCodec.decodeNumber(encoded);
        expect(decoded).toBe(number);
    });

    it('should encode and decode the minimum safe integer', () => {
        const number = Number.MIN_SAFE_INTEGER;
        const encoded = StorageCodec.encodeNumber(number);
        const decoded = StorageCodec.decodeNumber(encoded);
        expect(decoded).toBe(number);
    });

    it('should throw when encoding a non-integer number', () => {
        const nonInteger = 3.14;
        expect(() => StorageCodec.encodeNumber(nonInteger)).toThrow("must be a safe integer");
    });

    it('should encode and decode a string', () => {
        const str = "Hello, World!";
        const buffer = StorageCodec.encodeString(str);
        const decodedStr = StorageCodec.decodeString(buffer);
        expect(decodedStr).toBe(str);
    });

    it('should throw when encoding a non-string value', () => {
        const nonString = 12345 as any;
        expect(() => StorageCodec.encodeString(nonString)).toThrow("Value must be a string, got number");
    });

    it('should encode and decode an empty string', () => {
        const str = "";
        const buffer = StorageCodec.encodeString(str);
        const decodedStr = StorageCodec.decodeString(buffer);
        expect(decodedStr).toBe(str);
    });

    it('should encode and decode a string with special characters', () => {
        const str = "Hello, 世界! 👋";
        const buffer = StorageCodec.encodeString(str);
        const decodedStr = StorageCodec.decodeString(buffer);
        expect(decodedStr).toBe(str);
    });

    it('should throw when decoding a non-buffer value', () => {
        const nonBuffer = "Not a buffer" as any;
        expect(() => StorageCodec.decodeString(nonBuffer)).toThrow("Value must be a Buffer, got string");
    });

    it('should encode and decode objects', () => {
        const obj = { name: "Alice", age: 30 };
        const buffer = StorageCodec.encodeJSON(obj);
        const decodedObj = StorageCodec.decodeJSON<typeof obj>(buffer);
        expect(decodedObj).toEqual(obj);
    });

    it('should encode and deccode an empty object', () => {
        const obj = {};
        const buffer = StorageCodec.encodeJSON(obj);
        const decodedObj = StorageCodec.decodeJSON<typeof obj>(buffer);
        expect(decodedObj).toEqual(obj);
    });

    it('should throw when encoding a non-serializable object', () => {
        const circularObj: any = {};
        circularObj.self = circularObj;
        expect(() => StorageCodec.encodeJSON(circularObj)).toThrow("Failed to encode JSON");
    });

    it('should throw when decoding invalid JSON', () => {
        const invalidBuffer = Buffer.from("Not a valid JSON", StorageCodec.encoding);
        expect(() => StorageCodec.decodeJSON(invalidBuffer)).toThrow("Failed to decode JSON");
    });
});