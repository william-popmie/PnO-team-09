// @author MaartenHaine, Jari Daemen
// @date 2025-11-22

import { describe, it, expect, beforeEach } from 'vitest';
import { SimpleDBMS } from './simpledbms.mjs';
import { MockFile } from './file/mockfile.mjs';
import {
  COMPRESSION_ALGORITHM_GZIP_ID,
  COMPRESSION_ENVELOPE_HEADER_SIZE,
  CompressionService,
} from './compression/compression.mjs';

describe('SimpleDBMS', () => {
  let dbFile: MockFile;
  let walFile: MockFile;

  beforeEach(() => {
    dbFile = new MockFile(512);
    walFile = new MockFile(512);
  });

  it('create a new database and collection', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile);
    const collection = await db.getCollection('users');
    expect(collection).toBeDefined();
    await db.close();
  });

  it('insert and find documents', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile);
    const collection = await db.getCollection('users');

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
    const db = await SimpleDBMS.create(dbFile, walFile);
    const collection = await db.getCollection('users');

    const doc = await collection.insert({ name: 'random', age: 25 });
    const updated = await collection.update(doc.id, { age: 26 });

    expect(updated).toBeDefined();
    expect(updated!['age']).toBe(26);

    const found = await collection.findById(doc.id);
    expect(found!['age']).toBe(26);

    await db.close();
  });

  it('ddelete documents', async () => {
    const db = await SimpleDBMS.create(dbFile, walFile);
    const collection = await db.getCollection('users');

    const doc = await collection.insert({ name: 'random', age: 25 });
    const deleted = await collection.delete(doc.id);
    expect(deleted).toBe(true);

    const found = await collection.findById(doc.id);
    expect(found).toBeNull();

    await db.close();
  });

  it('should persist data across close/open', async () => {
    // Create and populate
    let db = await SimpleDBMS.create(dbFile, walFile);
    let collection = await db.getCollection('users');
    await collection.insert({ id: 'user1', name: 'random' });
    await db.close();

    // Reopen
    db = await SimpleDBMS.open(dbFile, walFile);
    collection = await db.getCollection('users');
    const found = await collection.findById('user1');
    expect(found).toBeDefined();
    expect(found!['name']).toBe('random');
    await db.close();
  });

  it('should handle multiple collections', async () => {
    let db = await SimpleDBMS.create(dbFile, walFile);
    const users = await db.getCollection('users');
    const posts = await db.getCollection('posts');

    await users.insert({ id: 'u1', name: 'random' });
    await posts.insert({ id: 'p1', title: 'randomtitle' });
    await db.close();

    db = await SimpleDBMS.open(dbFile, walFile);
    const users2 = await db.getCollection('users');
    const posts2 = await db.getCollection('posts');

    expect(await users2.findById('u1')).toBeDefined();
    expect(await posts2.findById('p1')).toBeDefined();
    await db.close();
  });

  it('should persist and reopen with compressed db header', async () => {
    let db = await SimpleDBMS.create(dbFile, walFile);
    const compressionService = new CompressionService({ algorithm: 'gzip' });

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
        algorithm: 'gzip',
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
    metadata.writeUInt8(COMPRESSION_ALGORITHM_GZIP_ID, 4);
    metadata.writeUInt32LE(compressedHeader.originalSize, 5);
    metadata.writeUInt32LE(compressedHeader.compressedSize, 9);

    await freeBlockFile.writeHeader(Buffer.concat([metadata, compressedHeader.payload]));
    await freeBlockFile.commit();

    await db.close();

    db = await SimpleDBMS.open(dbFile, walFile);
    const collection = await db.getCollection('users');
    await collection.insert({ id: 'doc-header-test', value: 'ok' });
    const found = await collection.findById('doc-header-test');
    expect(found).toBeDefined();
    expect(found!['value']).toBe('ok');
    await db.close();
  });

  it('should reopen with legacy plain JSON header', async () => {
    let db = await SimpleDBMS.create(dbFile, walFile);

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
      const service = new CompressionService({ algorithm: 'gzip' });
      const decoded = service.decompress({
        algorithm: 'gzip',
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

    db = await SimpleDBMS.open(dbFile, walFile);
    const reopenedUsers = await db.getCollection('users');
    const found = await reopenedUsers.findById('legacy-user');
    expect(found).toBeDefined();
    expect(found!['name']).toBe('legacy');
    await db.close();
  });
});
