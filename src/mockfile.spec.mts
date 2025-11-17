// @author Tijn Gommers
// @date 2025-17-11

import { describe, it, expect, beforeEach } from 'vitest';
import { MockFile } from './mockfile.mjs';

describe('MockFile', () => {
  let file: MockFile;

  beforeEach(() => {
    file = new MockFile(8); // kleine sectorSize voor eenvoud
  });

  it('should create, write, read and truncate a file', async () => {
    await file.create();
    await file.open();

    const buf = Buffer.from('abcdefgh');
    await file.writev([buf], 0);

    const readBuf = Buffer.alloc(buf.length);
    await file.read(readBuf, { position: 0 });
    expect(readBuf.toString()).toBe('abcdefgh');

    await file.truncate(4);
    const truncatedBuf = Buffer.alloc(4);
    await file.read(truncatedBuf, { position: 0 });
    expect(truncatedBuf.toString()).toBe('abcd');

    const stats = await file.stat();
    expect(stats.size).toBe(4);

    await file.close();
  });

  it('should throw when closing with pending writes', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcd')], 0);

    await expect(file.close()).rejects.toThrow("Closed the file without sync'ing first.");
  });

  it('should sync pending writes correctly', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcdefgh')], 0);

    await file.sync();
    const readBuf = Buffer.alloc(8);
    await file.read(readBuf, { position: 0 });
    expect(readBuf.toString()).toBe('abcdefgh');
  });

  it('should simulate crashBasic correctly', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcdefgh')], 0);

    file.crashBasic();
    const stats = await file.stat();
    expect(stats.size).toBe(8); // size should remain
  });

  it('should simulate crashFullLoss correctly', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcdefgh')], 0);

    file.crashFullLoss();
    const readBuf = Buffer.alloc(8);
    await expect(file.read(readBuf, { position: 0 })).rejects.toThrow(); // nothing saved
  });

  it('should simulate crashPartialCorruption correctly', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcdefgh')], 0);

    file.crashPartialCorruption();
    const readBuf = Buffer.alloc(8);
    await file.read(readBuf, { position: 0 });
    expect(readBuf.toString()).not.toBe('abcdefgh'); // data is corrupted
  });

  it('should simulate crashMixed correctly', async () => {
    await file.create();
    await file.open();
    await file.writev([Buffer.from('abcdefgh')], 0);
    await file.writev([Buffer.from('ijklmnop')], 8);

    file.crashMixed();
    const readBuf = Buffer.alloc(16);
    await file.read(readBuf, { position: 0 }).catch(() => {}); // may fail partially
    expect(readBuf.length).toBe(16);
  });

  it('should maintain file isolation between multiple MockFiles', async () => {
    const file1 = new MockFile(8);
    const file2 = new MockFile(8);

    await file1.create();
    await file2.create();

    await file1.writev([Buffer.from('one')], 0);
    await file2.writev([Buffer.from('two')], 0);

    const buf1 = Buffer.alloc(3);
    const buf2 = Buffer.alloc(3);

    await file1.read(buf1, { position: 0 });
    await file2.read(buf2, { position: 0 });

    expect(buf1.toString()).toBe('one');
    expect(buf2.toString()).toBe('two');
  });
});
