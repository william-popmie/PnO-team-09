// @author Tijn Gommers
// @author Wout Van Hemelrijck
// @date 2025-11-18

import { describe, expect, it } from 'vitest';
import { MockFile } from './mockfile.mjs';

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Returns a Buffer filled with a single repeating byte value. */
function filledBuffer(size: number, value: number): Buffer {
  return Buffer.alloc(size, value);
}

/** Reads the entire committed content of a MockFile (must be open, size known). */
async function readAll(file: MockFile, size: number): Promise<Buffer> {
  const buf = Buffer.alloc(size);
  await file.read(buf, { position: 0 });
  return buf;
}

// ─── Helpers for throw assertions ─────────────────────────────────────────────

/**
 * Asserts that `fn` throws (synchronously or via rejected Promise).
 * Wrapping in an async arrow ensures Vitest's `.rejects` catches both cases.
 */
async function expectThrows(fn: () => unknown, pattern: RegExp): Promise<void> {
  await expect( () => fn()).rejects.toThrow(pattern);
}

// ─── Constants ────────────────────────────────────────────────────────────────

const SECTOR = 8; // small sector for easy reasoning

// ─── Test suite ───────────────────────────────────────────────────────────────

describe('MockFile', () => {

  // ── Guard helpers ───────────────────────────────────────────────────────────

  describe('open/close/create guards', () => {

    it('create() succeeds on a fresh file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
    });

    it('create() throws if file is already open', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await expectThrows(() => f.create(), /already open/i);
    });

    it('open() succeeds after create+sync+close', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.sync();
      await f.close();
      await f.open();
    });

    it('open() throws if file is already open V2', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await expectThrows(() => f.open(), /already open/i);
    });

    it('close() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.close(), /not open/i);
    });

    it('close() throws when there are unsynced pending writes', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      await expectThrows(() => f.close(), /unsynced/i);
    });

    it('close() succeeds after sync()', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xbb)], 0);
      await f.sync();
      await f.close();
    });

    it('read() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.read(Buffer.alloc(4), { position: 0 }), /not open/i);
    });

    it('writev() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.writev([Buffer.alloc(4)], 0), /not open/i);
    });

    it('sync() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.sync(), /not open/i);
    });

    it('truncate() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.truncate(SECTOR), /not open/i);
    });

    it('stat() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await expectThrows(() => f.stat(), /not open/i);
    });

  });

  // ── stat / size ─────────────────────────────────────────────────────────────

  describe('stat()', () => {

    it('reports size 0 on a freshly created file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const { size } = await f.stat();
      expect(size).toBe(0);
    });

    it('reports correct size after truncate', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(3 * SECTOR);
      const { size } = await f.stat();
      expect(size).toBe(3 * SECTOR);
    });

    it('reports exact byte size (not rounded to sector boundary)', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR + 3);
      const { size } = await f.stat();
      expect(size).toBe(SECTOR + 3);
    });

  });

  // ── truncate ────────────────────────────────────────────────────────────────

  describe('truncate()', () => {

    it('extends the file with zero-filled sectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      const buf = await readAll(f, 2 * SECTOR);
      expect(buf).toEqual(Buffer.alloc(2 * SECTOR, 0));
    });

    it('shrinks the file and drops out-of-range sectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(2 * SECTOR, 0xcc)], 0);
      await f.sync();
      await f.truncate(SECTOR);
      const { size } = await f.stat();
      expect(size).toBe(SECTOR);
    });

    it('pending writes for removed sectors are discarded on shrink', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      await f.writev([filledBuffer(SECTOR, 0xff)], SECTOR);
      expect(f.getNewSectors().has(1)).toBe(true);
      await f.truncate(SECTOR);
      expect(f.getNewSectors().has(1)).toBe(false);
    });

    it('truncate to same size is a no-op', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.truncate(SECTOR);
      const { size } = await f.stat();
      expect(size).toBe(SECTOR);
    });

  });

  // ── writev / read ───────────────────────────────────────────────────────────

  describe('writev() and read()', () => {

    it('write a single full sector and read it back', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = filledBuffer(SECTOR, 0x42);
      await f.writev([data], 0);
      await f.sync();
      const result = await readAll(f, SECTOR);
      expect(result).toEqual(data);
    });

    it('write spanning two sectors and read back', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = filledBuffer(2 * SECTOR, 0x55);
      await f.writev([data], 0);
      await f.sync();
      const result = await readAll(f, 2 * SECTOR);
      expect(result).toEqual(data);
    });

    it('partial write within a sector preserves surrounding bytes', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      await f.sync();
      await f.writev([Buffer.from([0xff, 0xff])], 3);
      await f.sync();

      const expected = filledBuffer(SECTOR, 0xaa);
      expected[3] = 0xff;
      expected[4] = 0xff;

      const result = await readAll(f, SECTOR);
      expect(result).toEqual(expected);
    });

    it('write at a non-zero position auto-extends the file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = Buffer.from([0x01, 0x02, 0x03]);
      await f.writev([data], SECTOR);
      await f.sync();
      const { size } = await f.stat();
      expect(size).toBeGreaterThanOrEqual(SECTOR + data.length);
    });

    it('write with multiple buffers concatenates them correctly', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const a = Buffer.from([0x01, 0x02, 0x03, 0x04]);
      const b = Buffer.from([0x05, 0x06, 0x07, 0x08]);
      await f.writev([a, b], 0);
      await f.sync();
      const result = await readAll(f, SECTOR);
      expect(result).toEqual(Buffer.concat([a, b]));
    });

    it('read out of bounds throws an assertion error', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.sync();
      await expectThrows(
        () => f.read(Buffer.alloc(SECTOR + 1), { position: 0 }),
        /out of bounds|exceeds/i,
      );
    });

    it('read with negative position throws an assertion error', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.sync();
      await expectThrows(
        () => f.read(Buffer.alloc(1), { position: -1 }),
        /non-negative/i,
      );
    });

    it('pending write is visible before sync', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      // Do NOT sync — readSector should prefer newSectors
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      expect(buf).toEqual(filledBuffer(SECTOR, 0xaa));
    });

    it('second write to same sector (pre-sync) is visible immediately', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x11)], 0);
      await f.writev([filledBuffer(SECTOR, 0x22)], 0);
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      expect(buf).toEqual(filledBuffer(SECTOR, 0x22));
    });

    it('write at cross-sector boundary distributes bytes correctly', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      const payload = Buffer.from([0x10, 0x20, 0x30, 0x40]);
      await f.writev([payload], SECTOR - 2);
      await f.sync();

      const result = Buffer.alloc(2 * SECTOR);
      await f.read(result, { position: 0 });

      expect(result[SECTOR - 2]).toBe(0x10);
      expect(result[SECTOR - 1]).toBe(0x20);
      expect(result[SECTOR + 0]).toBe(0x30);
      expect(result[SECTOR + 1]).toBe(0x40);
    });

  });

  // ── sync ────────────────────────────────────────────────────────────────────

  describe('sync()', () => {

    it('clears newSectors after sync', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x01)], 0);
      expect(f.getNewSectors().size).toBe(1);
      await f.sync();
      expect(f.getNewSectors().size).toBe(0);
    });

    it('committed data survives a reopen', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = filledBuffer(SECTOR, 0xde);
      await f.writev([data], 0);
      await f.sync();
      await f.close();
      await f.open();
      const result = await readAll(f, SECTOR);
      expect(result).toEqual(data);
    });

    it('sync is idempotent when there is nothing pending', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.sync();
      await f.sync();
      expect(f.getNewSectors().size).toBe(0);
    });

  });

  // ── getNewSectors ───────────────────────────────────────────────────────────

  describe('getNewSectors()', () => {

    it('is empty on a freshly created file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      expect(f.getNewSectors().size).toBe(0);
    });

    it('accumulates multiple pending writes for the same sector', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x01)], 0);
      await f.writev([filledBuffer(SECTOR, 0x02)], 0);
      await f.writev([filledBuffer(SECTOR, 0x03)], 0);
      expect(f.getNewSectors().get(0)!.length).toBe(3);
    });

  });

  // ── create() reset behaviour ────────────────────────────────────────────────

  describe('create() resets state', () => {

    it('create() on a closed file resets size and sectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(2 * SECTOR, 0xff)], 0);
      await f.sync();
      await f.close();
      await f.create();
      const { size } = await f.stat();
      expect(size).toBe(0);
      expect(f.getNewSectors().size).toBe(0);
    });

  });

  // ── crash simulations ───────────────────────────────────────────────────────

  describe('crashBasic()', () => {

    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      f.crashBasic();
      expect(f.getNewSectors().size).toBe(0);
    });

    it('committed sector is one of the staged versions after crash', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      await f.sync();
      await f.writev([filledBuffer(SECTOR, 0xbb)], 0);
      f.crashBasic();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      const allAA = buf.every(b => b === 0xaa);
      const allBB = buf.every(b => b === 0xbb);
      expect(allAA || allBB).toBe(true);
    });

    it('file remains readable after crashBasic', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      await f.writev([filledBuffer(2 * SECTOR, 0x99)], 0);
      f.crashBasic();
      const buf = Buffer.alloc(2 * SECTOR);
      await f.read(buf, { position: 0 });
    });

  });

  describe('crashFullLoss()', () => {

    it('resets file to closed state (stat() throws)', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x11)], 0);
      await f.sync();
      f.crashFullLoss();
      await expectThrows(() => f.stat(), /not open/i);
    });

    it('after full loss a new create() works and starts fresh', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xcc)], 0);
      await f.sync();
      f.crashFullLoss();
      await f.create();
      const { size } = await f.stat();
      expect(size).toBe(0);
    });

    it('clears both committed sectors and newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xab)], 0);
      f.crashFullLoss();
      expect(f.getNewSectors().size).toBe(0);
    });

  });

  describe('crashPartialCorruption()', () => {

    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x55)], 0);
      f.crashPartialCorruption();
      expect(f.getNewSectors().size).toBe(0);
    });

    it('committed sector differs from original after crash with pending writes', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.sync();
      const original = filledBuffer(SECTOR, 0x00);
      await f.writev([original], 0);
      // Do NOT sync — corruption targets newSectors
      f.crashPartialCorruption();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      expect(buf).not.toEqual(original);
    });

    it('corrupts committed sectors when no pending writes exist', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const allZero = Buffer.alloc(SECTOR, 0x00);
      await f.writev([allZero], 0);
      await f.sync();
      f.crashPartialCorruption();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      expect(buf).not.toEqual(allZero);
    });

    it('exactly one byte differs after corruption (XOR 0xff)', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const allZero = Buffer.alloc(SECTOR, 0x00);
      await f.writev([allZero], 0);
      await f.sync();
      f.crashPartialCorruption();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      const diffCount = [...buf].filter(b => b !== 0x00).length;
      expect(diffCount).toBe(1);
    });

  });

  describe('crashMixed()', () => {

    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x77)], 0);
      f.crashMixed();
      expect(f.getNewSectors().size).toBe(0);
    });

    it('file remains readable after crashMixed', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      await f.writev([filledBuffer(2 * SECTOR, 0x44)], 0);
      f.crashMixed();
      const buf = Buffer.alloc(2 * SECTOR);
      await f.read(buf, { position: 0 });
    });

    it('file is always readable after crashMixed (probabilistic branches)', async () => {
      // Run many times to exercise all 50%/50% branches.
      for (let i = 0; i < 20; i++) {
        const f = new MockFile(SECTOR);
        await f.create();
        await f.truncate(SECTOR);
        await f.sync();
        await f.writev([filledBuffer(SECTOR, 0xff)], 0);
        f.crashMixed();
        const buf = Buffer.alloc(SECTOR);
        await f.read(buf, { position: 0 });
      }
    });

  });

  // ── sectorSize property ──────────────────────────────────────────────────────

  describe('sectorSize property', () => {

    it('reflects the value passed to the constructor', () => {
      const f = new MockFile(512);
      expect(f.sectorSize).toBe(512);
    });

    it('works with sector size of 1', async () => {
      const f = new MockFile(1);
      await f.create();
      await f.writev([Buffer.from([0x42])], 0);
      await f.sync();
      const buf = Buffer.alloc(1);
      await f.read(buf, { position: 0 });
      expect(buf[0]).toBe(0x42);
    });

  });

});
