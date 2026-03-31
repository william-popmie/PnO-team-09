// @author MaartenHaine, Jari Daemen
// @date 2025-11-22

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Collection, SimpleDBMS, type DocumentValue } from './simpledbms.mjs';
import { MockFile } from './file/mockfile.mjs';
import { FBNodeStorage } from './node-storage/fb-node-storage.mjs';
import {
  COMPRESSION_ALGORITHM_ZSTD_ID,
  COMPRESSION_ENVELOPE_HEADER_SIZE,
  CompressionService,
} from './compression/compression.mjs';

describe('Collection', () => {
  let db: SimpleDBMS;
  let collection: Collection;
  let dbFile: MockFile;
  let walFile: MockFile;
  let heapFile: MockFile;
  let heapWalFile: MockFile;

  const createIndexStorage = () =>
    new FBNodeStorage<string, number>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      (key) => key.length,
      db.getFreeBlockFile(),
      4096,
    );

  beforeEach(async () => {
    dbFile = new MockFile(512);
    walFile = new MockFile(512);
    heapFile = new MockFile(512);
    heapWalFile = new MockFile(512);
    db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    collection = await db.createCollection('users');
  });

  afterEach(async () => {
    await db.close();
  });

  it('should insert and find documents with generated id', async () => {
    const doc = await collection.insert({ name: 'maarten', age: 25 });
    expect(doc.id).toBeDefined();
    expect(doc['name']).toBe('maarten');
    expect(doc['age']).toBe(25);
    const found = await collection.findById(doc.id);
    expect(found).toEqual(doc);
  });

  it('should respect provided ids on insert', async () => {
    const doc = await collection.insert({ id: 'user-1', name: 'random' });
    expect(doc.id).toBe('user-1');
    const found = await collection.findById('user-1');
    expect(found).toEqual(doc);
  });

  it('should update documents and keep indexes in sync', async () => {
    const storage = createIndexStorage();
    await collection.createIndex('age', storage);

    const doc = await collection.insert({ name: 'random', age: 25 });
    expect(doc['age']).toBe(25);
    const updated = await collection.update(doc.id, { age: 26 });
    expect(updated).toBeDefined();
    expect(updated!['age']).toBe(26);
    expect(await collection.findById(doc.id)).toEqual(updated);

    const stale = await collection.find({ filterOps: { age: { $eq: 25 } } });
    const fresh = await collection.find({ filterOps: { age: { $eq: 26 } } });

    expect(stale).toHaveLength(0);
    expect(fresh).toHaveLength(1);
    expect(fresh[0]).toEqual(updated);
  });

  it('should isolate nested objects between insert and update (deep copy)', async () => {
    const doc = await collection.insert({
      name: 'alice',
      settings: { theme: 'dark', notifications: true },
    });

    const updated = await collection.update(doc.id, { name: 'Alice' });
    expect(updated).toBeDefined();

    const updatedSettings = updated!['settings'] as { [key: string]: DocumentValue };
    updatedSettings['theme'] = 'light';
    updatedSettings['notifications'] = false;

    const docSettings = doc['settings'] as { [key: string]: DocumentValue };
    expect(docSettings['theme']).toBe('dark');
    expect(docSettings['notifications']).toBe(true);

    const stored = await collection.findById(doc.id);
    const storedSettings = stored!['settings'] as { [key: string]: DocumentValue };
    expect(storedSettings['theme']).toBe('dark');
    expect(storedSettings['notifications']).toBe(true);
  });

  it('should delete documents and remove index entries', async () => {
    const storage = createIndexStorage();
    await collection.createIndex('age', storage);

    const doc = await collection.insert({ name: 'random', age: 25 });
    const deleted = await collection.delete(doc.id);
    expect(deleted).toBe(true);

    const found = await collection.findById(doc.id);
    expect(found).toBeNull();

    const indexed = await collection.find({ filterOps: { age: { $eq: 25 } } });
    expect(indexed).toHaveLength(0);
  });

  it('should reject indexing reserved fields', async () => {
    const storage = createIndexStorage();
    await expect(collection.createIndex('_private', storage)).rejects.toThrow();
    await expect(collection.createIndex('id', storage)).rejects.toThrow();
  });

  it('should create index on existing documents', async () => {
    await collection.insert({ id: 'u1', name: 'alice', age: 30 });
    await collection.insert({ id: 'u2', name: 'bob', age: 25 });
    await collection.insert({ id: 'u3', name: 'charlie', age: 30 });

    await collection.dropIndex('age');

    const storage = createIndexStorage();
    await collection.createIndex('age', storage);

    const results = await collection.find({ filterOps: { age: { $eq: 30 } } });
    expect(results).toHaveLength(2);
    expect(results.map((d) => d.id).sort()).toEqual(['u1', 'u3']);
  });

  it('should handle createIndex idempotency by throwing', async () => {
    const storage1 = createIndexStorage();
    const storage2 = createIndexStorage();

    await collection.createIndex('age', storage1);
    await expect(collection.createIndex('age', storage2)).rejects.toThrow('Index already exists');

    await collection.insert({ name: 'test', age: 40 });
    const results = await collection.find({ filterOps: { age: { $eq: 40 } } });
    expect(results).toHaveLength(1);
  });

  it('should maintain index when inserting new documents', async () => {
    const storage = createIndexStorage();
    await collection.createIndex('status', storage);

    await collection.insert({ id: 'd1', status: 'active' });
    await collection.insert({ id: 'd2', status: 'inactive' });
    await collection.insert({ id: 'd3', status: 'active' });

    const active = await collection.find({ filterOps: { status: { $eq: 'active' } } });
    const inactive = await collection.find({ filterOps: { status: { $eq: 'inactive' } } });

    expect(active).toHaveLength(2);
    expect(inactive).toHaveLength(1);
    expect(active.map((d) => d.id).sort()).toEqual(['d1', 'd3']);
  });

  it('should handle null and undefined values in indexed fields', async () => {
    const storage = createIndexStorage();
    await collection.createIndex('optionalField', storage);

    await collection.insert({ id: 'd1', optionalField: 'value1' });
    await collection.insert({ id: 'd2', optionalField: null });
    await collection.insert({ id: 'd3' });
    await collection.insert({ id: 'd4', optionalField: 'value2' });

    const withValue1 = await collection.find({ filterOps: { optionalField: { $eq: 'value1' } } });
    const withValue2 = await collection.find({ filterOps: { optionalField: { $eq: 'value2' } } });

    expect(withValue1).toHaveLength(1);
    expect(withValue1[0].id).toBe('d1');
    expect(withValue2).toHaveLength(1);
    expect(withValue2[0].id).toBe('d4');

    const allDocs = await collection.find({});
    expect(allDocs).toHaveLength(4);
  });

  it('should support multiple indexes on different fields', async () => {
    const ageStorage = createIndexStorage();
    const nameStorage = createIndexStorage();

    await collection.createIndex('age', ageStorage);
    await collection.createIndex('name', nameStorage);

    await collection.insert({ id: 'u1', name: 'alice', age: 30 });
    await collection.insert({ id: 'u2', name: 'bob', age: 25 });
    await collection.insert({ id: 'u3', name: 'charlie', age: 30 });

    const age30 = await collection.find({ filterOps: { age: { $eq: 30 } } });
    expect(age30).toHaveLength(2);

    const bob = await collection.find({ filterOps: { name: { $eq: 'bob' } } });
    expect(bob).toHaveLength(1);
    expect(bob[0].id).toBe('u2');
  });

  it('should apply projection, sorting, skip, and limit', async () => {
    await collection.insert({ id: 'a', name: 'alpha', score: 10 });
    await collection.insert({ id: 'b', name: 'bravo', score: 30 });
    await collection.insert({ id: 'c', name: 'charlie', score: 20 });
    await collection.insert({ id: 'd', name: 'delta', score: 40 });

    const results = await collection.find({
      sort: { field: 'score', order: 'desc' },
      projection: ['name'],
      skip: 1,
      limit: 2,
    });

    expect(results).toHaveLength(2);
    expect(results.map((d) => d.id)).toEqual(['b', 'c']);
    expect(results.map((d) => d['name'])).toEqual(['bravo', 'charlie']);
    expect(results.every((d) => d['score'] === undefined)).toBe(true);
  });

  it('should honor id ranges', async () => {
    await collection.insert({ id: 'a', value: 1 });
    await collection.insert({ id: 'b', value: 2 });
    await collection.insert({ id: 'c', value: 3 });
    await collection.insert({ id: 'd', value: 4 });

    const results = await collection.find({ idRange: { min: 'b', max: 'c' } });
    expect(results.map((d) => d.id)).toEqual(['b', 'c']);
  });

  it('should aggregate by group with count, sum, avg, and min', async () => {
    await collection.insert({ id: 'd1', team: 'red', points: 10 });
    await collection.insert({ id: 'd2', team: 'red', points: 20 });
    await collection.insert({ id: 'd3', team: 'blue', points: 7 });

    const aggregateResults = await collection.aggregate({
      groupBy: 'team',
      operations: {
        count: 'count',
        sum: [{ field: 'points', as: 'total' }],
        avg: [{ field: 'points', as: 'avgPoints' }],
        min: [{ field: 'points', as: 'minPoints' }],
      },
    });

    const byTeam = new Map(aggregateResults.map((r) => [r['team'], r]));
    expect(aggregateResults).toHaveLength(2);
    expect(byTeam.get('red')).toBeDefined();
    expect(byTeam.get('blue')).toBeDefined();
    expect(byTeam.get('red')!).toMatchObject({ count: 2, total: 30, avgPoints: 15, minPoints: 10 });
    expect(byTeam.get('blue')!).toMatchObject({ count: 1, total: 7, avgPoints: 7, minPoints: 7 });
  });
});

describe('SimpleDBMS', () => {
  let dbFile: MockFile;
  let walFile: MockFile;
  let heapFile: MockFile;
  let heapWalFile: MockFile;

  beforeEach(() => {
    dbFile = new MockFile(512);
    walFile = new MockFile(512);
    heapFile = new MockFile(512);
    heapWalFile = new MockFile(512);
  });

  it('create a new database and collection', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const collection = await db.createCollection('users');
    expect(collection).toBeDefined();
    await db.close();
  });

  it('insert and find documents', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const collection = await db.createCollection('users');

    const doc = await collection.insert({ name: 'maarten', age: 25 });
    expect(doc.id).toBeDefined();
    expect(doc['name']).toBe('maarten');

    const found = await collection.findById(doc.id);
    expect(found).toEqual(doc);

    const results = await collection.find({ filter: (d) => d['name'] === 'maarten' });
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(doc);

    await db.close();
  });

  it('update documents', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const collection = await db.createCollection('users');

    const doc = await collection.insert({ name: 'random', age: 25 });
    const updated = await collection.update(doc.id, { age: 26 });

    expect(updated).toBeDefined();
    expect(updated!['age']).toBe(26);

    const found = await collection.findById(doc.id);
    expect(found!['age']).toBe(26);

    await db.close();
  });

  it('ddelete documents', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const collection = await db.createCollection('users');

    const doc = await collection.insert({ name: 'random', age: 25 });
    const deleted = await collection.delete(doc.id);
    expect(deleted).toBe(true);

    const found = await collection.findById(doc.id);
    expect(found).toBeNull();

    await db.close();
  });

  it('should persist data across close/open', async () => {
    // Create and populate
    let db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    let collection = await db.createCollection('users');
    await collection.insert({ id: 'user1', name: 'random' });
    await db.close();

    // Reopen
    db = await SimpleDBMS.open(dbFile, walFile, heapFile, heapWalFile);
    collection = await db.getCollection('users');
    const found = await collection.findById('user1');
    expect(found).toBeDefined();
    expect(found!['name']).toBe('random');
    await db.close();
  });

  it('should handle multiple collections', async () => {
    let db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const users = await db.createCollection('users');
    const posts = await db.createCollection('posts');

    await users.insert({ id: 'u1', name: 'random' });
    await posts.insert({ id: 'p1', title: 'randomtitle' });
    await db.close();

    db = await SimpleDBMS.open(dbFile, walFile, heapFile, heapWalFile);
    const users2 = await db.getCollection('users');
    const posts2 = await db.getCollection('posts');

    expect(await users2.findById('u1')).toBeDefined();
    expect(await posts2.findById('p1')).toBeDefined();
    await db.close();
  });

  it('should persist and reopen with compressed db header', async () => {
      let db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);

    await db.getCollection('users');

    const freeBlockFile = db.getFreeBlockFile();
    const currentHeader = await freeBlockFile.readHeader();

    let parsedHeader: unknown;
    if (currentHeader.subarray(0, 4).equals(Buffer.from('DBH1', 'ascii'))) {
      const compressedSize = currentHeader.readUInt32LE(9);
      const compressedPayload = currentHeader.subarray(
        COMPRESSION_ENVELOPE_HEADER_SIZE,
        COMPRESSION_ENVELOPE_HEADER_SIZE + compressedSize,
      );
      const decoded = compressionService.decompress({
        algorithm: 'zstd',
        originalSize: currentHeader.readUInt32LE(5),
        compressedSize,
        payload: Buffer.from(compressedPayload),
      });
      parsedHeader = JSON.parse(decoded.toString());
    } else {
      parsedHeader = JSON.parse(currentHeader.toString());
    }

    const headerJson = Buffer.from(JSON.stringify(parsedHeader));
    const compressedHeader = compressionService.compress(headerJson);
    const metadata = Buffer.alloc(COMPRESSION_ENVELOPE_HEADER_SIZE);
    Buffer.from('DBH1', 'ascii').copy(metadata, 0);
    metadata.writeUInt8(COMPRESSION_ALGORITHM_ZSTD_ID, 4);
    metadata.writeUInt32LE(compressedHeader.originalSize, 5);
    metadata.writeUInt32LE(compressedHeader.compressedSize, 9);

    await freeBlockFile.writeHeader(Buffer.concat([metadata, compressedHeader.payload]));
    await freeBlockFile.commit();

    await db.close();

      db = await SimpleDBMS.open(dbFile, walFile, heapFile, heapWalFile);
    const collection = await db.getCollection('users');
    await collection.insert({ id: 'doc-header-test', value: 'ok' });
    const found = await collection.findById('doc-header-test');
    expect(found).toBeDefined();
    expect(found!['value']).toBe('ok');
    await db.close();
  });

  it('should reopen with legacy plain JSON header', async () => {
      let db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
    const users = await db.getCollection('users');
    await users.insert({ id: 'legacy-user', name: 'legacy' });

    const freeBlockFile = db.getFreeBlockFile();
    const header = await freeBlockFile.readHeader();

    let parsedHeader: unknown;
    if (header.subarray(0, 4).equals(Buffer.from('DBH1', 'ascii'))) {
      const compressedSize = header.readUInt32LE(9);
      const payload = header.subarray(
        COMPRESSION_ENVELOPE_HEADER_SIZE,
        COMPRESSION_ENVELOPE_HEADER_SIZE + compressedSize,
      );
      const service = new CompressionService({ algorithm: 'zstd' });
      const decoded = service.decompress({
        algorithm: 'zstd',
        originalSize: header.readUInt32LE(5),
        compressedSize,
        payload: Buffer.from(payload),
      });
      parsedHeader = JSON.parse(decoded.toString());
    } else {
      parsedHeader = JSON.parse(header.toString());
    }

    await freeBlockFile.writeHeader(Buffer.from(JSON.stringify(parsedHeader)));
    await freeBlockFile.commit();
    await db.close();

      db = await SimpleDBMS.open(dbFile, walFile, heapFile, heapWalFile);
    const reopenedUsers = await db.getCollection('users');
    const found = await reopenedUsers.findById('legacy-user');
    expect(found).toBeDefined();
    expect(found!['name']).toBe('legacy');
    await db.close();
  });
});
