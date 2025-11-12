import { describe, it, expect } from 'vitest';
import { FreeBlockFile, NO_BLOCK } from '../src/freeblockfile.mjs';
import { MockFile } from '../src/mockfile.mjs';
import type { File as FileInterface } from '../src/mockfile.mjs';
import type { AtomicFile } from '../src/freeblockfile.mjs';

const DEFAULT_BLOCK_SIZE = 4096;
const NEXT_POINTER_SIZE = 4;
const HEADER_LENGTH_OFFSET = NEXT_POINTER_SIZE;
const HEADER_CLIENT_AREA_OFFSET = HEADER_LENGTH_OFFSET + NEXT_POINTER_SIZE;
const LENGTH_PREFIX_SIZE = 8;

class TestAtomicFile {
  private file: FileInterface;
  constructor(file: FileInterface) {
    this.file = file;
  }

  async open(): Promise<void> {
    if (typeof this.file.open === 'function') await this.file.open();
  }
  async close(): Promise<void> {
    if (typeof this.file.close === 'function') await this.file.close();
  }

  async atomicWrite(writes: { position: number; buffer: Buffer }[]): Promise<void> {
    for (const w of writes) {
      await this.file.writev([w.buffer], w.position);
    }
    if (typeof this.file.sync === 'function') await this.file.sync();
  }

  async sync(): Promise<void> {
    if (typeof this.file.sync === 'function') await this.file.sync();
  }
}

async function makeFreeBlockFile() {
  const mf = new MockFile(512);
  const atomic = new TestAtomicFile(mf as unknown as FileInterface);
  const fb = new FreeBlockFile(mf as unknown as FileInterface, atomic as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
  await fb.open();
  return { fb, mf, atomic };
}

describe('FreeBlockFile', () => {
  it('allocateWriteCommitRead', async () => {
    const { fb, mf } = await makeFreeBlockFile();
    const payload = Buffer.from('hello, freeblockfile!');
    const start = await fb.allocateAndWrite(payload);

    await fb.commit();

    const readBack = await fb.readBlob(start);
    expect(readBack.toString()).toEqual(payload.toString());

    await fb.close();

    const atomic2 = new TestAtomicFile(mf as unknown as FileInterface);
    const fb2 = new FreeBlockFile(mf as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await fb2.open();
    const readBack2 = await fb2.readBlob(start);
    expect(readBack2.toString()).toEqual(payload.toString());
    await fb2.close();
  });

  it('noCommitLosesData', async () => {
    const { fb, mf } = await makeFreeBlockFile();
    const payload = Buffer.from('transient data');
    const start = await fb.allocateAndWrite(payload);

    await fb.close();

    const atomic2 = new TestAtomicFile(mf as unknown as FileInterface);
    const fb2 = new FreeBlockFile(mf as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await fb2.open();

    const readBack = await fb2.readBlob(start);
    expect(readBack.length).toBe(0);

    await fb2.close();
  });

  it('freeAndReuse', async () => {
    const { fb } = await makeFreeBlockFile();
    const payload = Buffer.from('to be freed');
    const start = await fb.allocateAndWrite(payload);
    await fb.commit();

    const before = await fb.readBlob(start);
    expect(before.toString()).toEqual(payload.toString());

    await fb.freeBlob(start);
    await fb.commit();

    const freeHead = await fb.debug_getFreeListHead();
    expect(freeHead).toEqual(start);

    const newAlloc = await fb.allocateBlocks(1);
    expect(newAlloc).toEqual(start);
    await fb.close();
  });

  it('mixedReuseAndAppend (reuse some freed blocks + append new ones)', async () => {
    const { fb } = await makeFreeBlockFile();

    const payload3 = Buffer.alloc(fb['payloadSize'] * 3 - LENGTH_PREFIX_SIZE, 'a');
    const start3 = await fb.allocateAndWrite(payload3);
    await fb.commit();

    await fb.freeBlob(start3);
    await fb.commit();

    const payload5 = Buffer.alloc(fb['payloadSize'] * 5 - LENGTH_PREFIX_SIZE, 'b');
    const start5 = await fb.allocateAndWrite(payload5);
    await fb.commit();

    const readBack5 = await fb.readBlob(start5);
    expect(readBack5.length).toEqual(payload5.length);
    expect(readBack5.equals(payload5)).toBe(true);

    await fb.close();
  });

  it('stageRawBlock validation', async () => {
    const { fb } = await makeFreeBlockFile();
    const wrong = Buffer.alloc(16);
    await expect(async () => fb.stageRawBlock(2, wrong)).rejects.toThrow('raw block must have blockSize length');
    await fb.close();
  });

  it('constructor should throw for too-small blockSize', () => {
    const mf = new MockFile(512);
    const atomic = new TestAtomicFile(mf as unknown as FileInterface);
    expect(() => new FreeBlockFile(mf as unknown as FileInterface, atomic as unknown as AtomicFile, 16)).toThrow();
  });

  it('methods should throw when called before open', async () => {
    const mf = new MockFile(512);
    const atomic = new TestAtomicFile(mf as unknown as FileInterface);
    const fb = new FreeBlockFile(mf as unknown as FileInterface, atomic as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await expect(fb.allocateBlocks(1)).rejects.toThrow('FreeBlockFile is not open');
    await expect(fb.allocateAndWrite(Buffer.from('x'))).rejects.toThrow('FreeBlockFile is not open');
    await expect(fb.readBlob(1)).rejects.toThrow('FreeBlockFile is not open');
  });

  it('allocateBlocks should throw for non-positive count', async () => {
    const { fb } = await makeFreeBlockFile();
    await expect(fb.allocateBlocks(0)).rejects.toThrow('count must be positive');
    await expect(fb.allocateBlocks(-1)).rejects.toThrow('count must be positive');
    await fb.close();
  });

  it('writeHeader should throw if header too large and readHeader/writeHeader behavior', async () => {
    const { fb, mf } = await makeFreeBlockFile();

    const maxClientHeader = fb['blockSize'] - HEADER_CLIENT_AREA_OFFSET;
    await expect(fb.writeHeader(Buffer.alloc(maxClientHeader + 1))).rejects.toThrow('header too large');

    const hdr = Buffer.from('my-metadata');
    await fb.writeHeader(hdr);
    const r1 = await fb.readHeader();
    expect(r1.toString()).toEqual(hdr.toString());

    await fb.commit();
    await fb.close();

    const atomic2 = new TestAtomicFile(mf as unknown as FileInterface);
    const fb2 = new FreeBlockFile(mf as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await fb2.open();
    const r2 = await fb2.readHeader();
    expect(r2.toString()).toEqual(hdr.toString());
    await fb2.close();
  });

  it('stageRawBlock should update cached header/freeList when staging block 0', async () => {
    const { fb, mf } = await makeFreeBlockFile();

    const b = Buffer.alloc(fb['blockSize'], 0);
    const wantFreeHead = 7;
    const clientHdr = Buffer.from('xyz');
    b.writeUInt32LE(wantFreeHead >>> 0, 0);
    b.writeUInt32LE(clientHdr.length >>> 0, HEADER_LENGTH_OFFSET);
    clientHdr.copy(b, HEADER_CLIENT_AREA_OFFSET);

    await fb.stageRawBlock(0, b);

    const gotHead = await fb.debug_getFreeListHead();
    expect(gotHead).toEqual(wantFreeHead);

    const gotHdr = await fb.readHeader();
    expect(gotHdr.toString()).toEqual(clientHdr.toString());

    await fb.commit();
    await fb.close();

    const atomic2 = new TestAtomicFile(mf as unknown as FileInterface);
    const fb2 = new FreeBlockFile(mf as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await fb2.open();
    const gotHead2 = await fb2.debug_getFreeListHead();
    expect(gotHead2).toEqual(wantFreeHead);
    const gotHdr2 = await fb2.readHeader();
    expect(gotHdr2.toString()).toEqual(clientHdr.toString());
    await fb2.close();
  });

  it('freeBlob(NO_BLOCK) is a no-op and readBlob(NO_BLOCK) returns empty', async () => {
    const { fb } = await makeFreeBlockFile();
    await expect(fb.freeBlob(NO_BLOCK)).resolves.not.toThrow();
    const empty = await fb.readBlob(NO_BLOCK);
    expect(empty.length).toEqual(0);
    await fb.close();
  });

  it('commit should throw when called before open (ensureOpened)', async () => {
    const mf = new MockFile(512);
    const atomic = new TestAtomicFile(mf as unknown as FileInterface);
    const fb = new FreeBlockFile(mf as unknown as FileInterface, atomic as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
    await expect(fb.commit()).rejects.toThrow('FreeBlockFile is not open');
  });

  it('commit is a no-op when there are no staged writes (early return)', async () => {
    const { fb, mf } = await makeFreeBlockFile();

    await fb.commit();
    const st1 = await mf.stat();

    await fb.commit();
    const st2 = await mf.stat();

    expect(st2.size).toEqual(st1.size);

    await fb.close();
  });

  it('commit auto-stages header if missing from stagedWrites', async () => {
    const { fb, mf } = await makeFreeBlockFile();

    await fb.commit();

    const b1 = Buffer.alloc(fb['blockSize'], 0);
    b1.writeUInt32LE(0xdeadbeef >>> 0, 0);

    await fb.stageRawBlock(1, b1);

    const stBefore = await mf.stat();
    expect(stBefore.size).toBeLessThanOrEqual(fb['blockSize']);

    await fb.commit();

    const stAfter = await mf.stat();
    expect(stAfter.size).toBeGreaterThanOrEqual(fb['blockSize'] * 2);

    const headerBlock = Buffer.alloc(fb['blockSize']);
    await mf.read(headerBlock, { position: 0 });
    const headFromFile = headerBlock.readUInt32LE(0);
    const headCached = await fb.debug_getFreeListHead();
    expect(headFromFile).toEqual(headCached);

    const readBlock1 = Buffer.alloc(fb['blockSize']);
    await mf.read(readBlock1, { position: fb['blockSize'] });
    expect(readBlock1.equals(b1)).toBe(true);

    await fb.close();
  });
  it('readHeader returns empty Buffer when no header written yet', async () => {
    const { fb } = await makeFreeBlockFile();
    const hdr = await fb.readHeader();
    expect(hdr.length).toEqual(0);
    await fb.close();
  });

  it('readBlob returns empty when length prefix is zero', async () => {
    const { fb } = await makeFreeBlockFile();

    const b = Buffer.alloc(fb['blockSize'], 0);
    b.writeUInt32LE(NO_BLOCK >>> 0, 0);
    b.writeBigUInt64LE(0n, NEXT_POINTER_SIZE);
    await fb.stageRawBlock(10, b);

    const out = await fb.readBlob(10);
    expect(out.length).toEqual(0);

    await fb.close();
  });

  it('readBlob correctly decodes length-prefixed data from staged block', async () => {
    const { fb } = await makeFreeBlockFile();

    const data = Buffer.from('this-is-data');
    const full = Buffer.alloc(LENGTH_PREFIX_SIZE + data.length);
    full.writeBigUInt64LE(BigInt(data.length), 0);
    data.copy(full, LENGTH_PREFIX_SIZE);

    const b = Buffer.alloc(fb['blockSize'], 0);
    b.writeUInt32LE(NO_BLOCK >>> 0, 0);
    full.copy(b, NEXT_POINTER_SIZE);

    await fb.stageRawBlock(11, b);

    const out = await fb.readBlob(11);
    expect(out.equals(data)).toBe(true);

    await fb.close();
  });

  it('readBlob on out-of-range block returns empty via readRawBlock zero-buffer branch', async () => {
    const { fb, mf } = await makeFreeBlockFile();

    await fb.commit();
    const st = await mf.stat();
    expect(st.size).toBeGreaterThanOrEqual(0);

    const out = await fb.readBlob(99);
    expect(out.length).toEqual(0);

    await fb.close();
  });
});
