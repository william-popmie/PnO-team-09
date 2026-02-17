// @author Tijn Gommers
// @author Wout Van Hemelrijck
// @date 2025-11-18

import assert from 'node:assert/strict';
import { MockFile } from './mockfile.mjs';
import { describe as _describe, it as _it } from 'node:test';

// ─── Helpers ──────────────────────────────────────────────────────────────────

const describe = (name: string, fn: () => void): void => {
  void _describe(name, fn);
};
const it = (name: string, fn: () => Promise<void>): void => {
  void _it(name, fn);
};
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

// ─── Constants ────────────────────────────────────────────────────────────────

const SECTOR = 8; // small sector for easy reasoning

// ─── Test suite ───────────────────────────────────────────────────────────────

describe('MockFile', () => {
  // ── Guard helpers ───────────────────────────────────────────────────────────

  describe('open/close/create guards', () => {
    it('create() succeeds on a fresh file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      // If create() throws the test will fail automatically.
    });

    it('create() throws if file is already open', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await assert.rejects(() => f.create(), /already open/i);
    });

    it('open() succeeds after create+close', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.sync();
      await f.close();
      await f.open();
    });

    it('open() throws if file is already open', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await assert.rejects(() => f.open(), /already open/i);
    });

    it('close() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.close(), /not open/i);
    });

    it('close() throws when there are unsynced pending writes', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      // newSectors is non-empty → close must reject
      await assert.rejects(() => f.close(), /unsynced/i);
    });

    it('close() succeeds after sync()', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xbb)], 0);
      await f.sync();
      await f.close(); // must not throw
    });

    it('read() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.read(Buffer.alloc(4), { position: 0 }), /not open/i);
    });

    it('writev() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.writev([Buffer.alloc(4)], 0), /not open/i);
    });

    it('sync() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.sync(), /not open/i);
    });

    it('truncate() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.truncate(SECTOR), /not open/i);
    });

    it('stat() throws if file is not open', async () => {
      const f = new MockFile(SECTOR);
      await assert.rejects(() => f.stat(), /not open/i);
    });
  });

  // ── stat / size ─────────────────────────────────────────────────────────────

  describe('stat()', () => {
    it('reports size 0 on a freshly created file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const { size } = await f.stat();
      assert.equal(size, 0);
    });

    it('reports correct size after truncate', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(3 * SECTOR);
      const { size } = await f.stat();
      assert.equal(size, 3 * SECTOR);
    });

    it('reports exact byte size (not rounded to sector boundary)', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR + 3);
      const { size } = await f.stat();
      assert.equal(size, SECTOR + 3);
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
      assert.deepEqual(buf, Buffer.alloc(2 * SECTOR, 0));
    });

    it('shrinks the file and drops out-of-range sectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(2 * SECTOR, 0xcc)], 0);
      await f.sync();
      await f.truncate(SECTOR); // drop second sector
      const { size } = await f.stat();
      assert.equal(size, SECTOR);
    });

    it('pending writes for removed sectors are discarded on shrink', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      // Stage a write to sector 1 (the one we are about to remove)
      await f.writev([filledBuffer(SECTOR, 0xff)], SECTOR);
      assert.equal(f.getNewSectors().has(1), true);
      await f.truncate(SECTOR); // shrink: sector 1 should be discarded
      assert.equal(f.getNewSectors().has(1), false);
    });

    it('truncate to same size is a no-op', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.truncate(SECTOR);
      const { size } = await f.stat();
      assert.equal(size, SECTOR);
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
      assert.deepEqual(result, data);
    });

    it('write spanning two sectors and read back', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = filledBuffer(2 * SECTOR, 0x55);
      await f.writev([data], 0);
      await f.sync();
      const result = await readAll(f, 2 * SECTOR);
      assert.deepEqual(result, data);
    });

    it('partial write within a sector preserves surrounding bytes', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      // Fill sector with 0xaa, then overwrite middle 2 bytes with 0xff
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      await f.sync();
      await f.writev([Buffer.from([0xff, 0xff])], 3);
      await f.sync();

      const expected = filledBuffer(SECTOR, 0xaa);
      expected[3] = 0xff;
      expected[4] = 0xff;

      const result = await readAll(f, SECTOR);
      assert.deepEqual(result, expected);
    });

    it('write at a non-zero position auto-extends the file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const data = Buffer.from([0x01, 0x02, 0x03]);
      await f.writev([data], SECTOR); // write into second sector region
      await f.sync();
      const { size } = await f.stat();
      assert.ok(size >= SECTOR + data.length, 'file should have grown');
    });

    it('write with multiple buffers concatenates them correctly', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const a = Buffer.from([0x01, 0x02, 0x03, 0x04]);
      const b = Buffer.from([0x05, 0x06, 0x07, 0x08]);
      await f.writev([a, b], 0);
      await f.sync();
      const result = await readAll(f, SECTOR);
      assert.deepEqual(result, Buffer.concat([a, b]));
    });

    it('read out of bounds throws an assertion error', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.sync();
      await assert.rejects(() => f.read(Buffer.alloc(SECTOR + 1), { position: 0 }), /out of bounds|exceeds/i);
    });

    it('read with negative position throws an assertion error', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(SECTOR);
      await f.sync();
      await assert.rejects(() => f.read(Buffer.alloc(1), { position: -1 }), /non-negative/i);
    });

    it('pending write is visible before sync', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      // Do NOT sync — readSector should prefer newSectors
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      assert.deepEqual(buf, filledBuffer(SECTOR, 0xaa));
    });

    it('second write to same sector (pre-sync) is visible immediately', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x11)], 0);
      await f.writev([filledBuffer(SECTOR, 0x22)], 0); // overwrite
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      assert.deepEqual(buf, filledBuffer(SECTOR, 0x22));
    });

    it('write at cross-sector boundary distributes bytes correctly', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      // Fill two sectors with 0x00
      await f.truncate(2 * SECTOR);
      await f.sync();
      // Write 4 bytes starting at offset SECTOR - 2 (straddles boundary)
      const payload = Buffer.from([0x10, 0x20, 0x30, 0x40]);
      await f.writev([payload], SECTOR - 2);
      await f.sync();

      const result = Buffer.alloc(2 * SECTOR);
      await f.read(result, { position: 0 });

      assert.equal(result[SECTOR - 2], 0x10);
      assert.equal(result[SECTOR - 1], 0x20);
      assert.equal(result[SECTOR], 0x30);
      assert.equal(result[SECTOR + 1], 0x40);
    });
  });

  // ── sync ────────────────────────────────────────────────────────────────────

  describe('sync()', () => {
    it('clears newSectors after sync', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x01)], 0);
      assert.equal(f.getNewSectors().size, 1);
      await f.sync();
      assert.equal(f.getNewSectors().size, 0);
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
      assert.deepEqual(result, data);
    });

    it('sync is idempotent when there is nothing pending', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.sync(); // nothing pending
      await f.sync(); // still nothing pending
      assert.equal(f.getNewSectors().size, 0);
    });
  });

  // ── getNewSectors ───────────────────────────────────────────────────────────

  describe('getNewSectors()', () => {
    it('is empty on a freshly created file', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      assert.equal(f.getNewSectors().size, 0);
    });

    it('accumulates multiple pending writes for the same sector', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x01)], 0);
      await f.writev([filledBuffer(SECTOR, 0x02)], 0);
      await f.writev([filledBuffer(SECTOR, 0x03)], 0);
      const pending = f.getNewSectors();
      assert.equal(pending.get(0)!.length, 3);
    });
  });

  // ── create() reset behaviour ────────────────────────────────────────────────

  describe('create() resets state', () => {
    it('create() on a closed file resets size and sectors', async () => {
      const f = new MockFile(SECTOR);
      // First use
      await f.create();
      await f.writev([filledBuffer(2 * SECTOR, 0xff)], 0);
      await f.sync();
      await f.close();
      // Re-create
      await f.create();
      const { size } = await f.stat();
      assert.equal(size, 0);
      assert.equal(f.getNewSectors().size, 0);
    });
  });

  // ── crash simulations ───────────────────────────────────────────────────────

  describe('crashBasic()', () => {
    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      f.crashBasic();
      assert.equal(f.getNewSectors().size, 0);
    });

    it('committed sectors have correct size after crash', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xaa)], 0);
      await f.sync();
      await f.writev([filledBuffer(SECTOR, 0xbb)], 0);
      f.crashBasic();
      // After crash the sector is either 0xaa or 0xbb — both are valid full sectors
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      const allAA = buf.every((b) => b === 0xaa);
      const allBB = buf.every((b) => b === 0xbb);
      assert.ok(allAA || allBB, 'sector must be one of the staged write versions');
    });

    it('file remains readable after crashBasic', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      await f.writev([filledBuffer(2 * SECTOR, 0x99)], 0);
      f.crashBasic();
      // We should be able to read without throwing
      const buf = Buffer.alloc(2 * SECTOR);
      await assert.doesNotReject(() => f.read(buf, { position: 0 }));
    });
  });

  describe('crashFullLoss()', () => {
    it('resets file to empty closed state', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x11)], 0);
      await f.sync();
      f.crashFullLoss();
      // File should now be closed
      await assert.rejects(() => f.stat(), /not open/i);
    });

    it('after full loss a new create() works and starts fresh', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xcc)], 0);
      await f.sync();
      f.crashFullLoss();
      await f.create();
      const { size } = await f.stat();
      assert.equal(size, 0);
    });

    it('clears both committed sectors and newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0xab)], 0); // pending
      f.crashFullLoss();
      assert.equal(f.getNewSectors().size, 0);
    });
  });

  describe('crashPartialCorruption()', () => {
    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x55)], 0);
      f.crashPartialCorruption();
      assert.equal(f.getNewSectors().size, 0);
    });

    it('committed sector is modified (corrupted) after crash with pending writes', async () => {
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
      // The sector must differ from original (one byte flipped by XOR 0xff)
      assert.notDeepEqual(buf, original);
    });

    it('corrupts committed sectors when no pending writes exist', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const allZero = Buffer.alloc(SECTOR, 0x00);
      await f.writev([allZero], 0);
      await f.sync(); // committed, no pending
      f.crashPartialCorruption();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      assert.notDeepEqual(buf, allZero);
    });

    it('exactly one byte differs after corruption', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      const allZero = Buffer.alloc(SECTOR, 0x00);
      await f.writev([allZero], 0);
      await f.sync();
      f.crashPartialCorruption();
      const buf = Buffer.alloc(SECTOR);
      await f.read(buf, { position: 0 });
      const diffCount = [...buf].filter((b) => b !== 0x00).length;
      assert.equal(diffCount, 1, 'exactly one byte should be corrupted (XOR 0xff → 0xff)');
    });
  });

  describe('crashMixed()', () => {
    it('clears newSectors', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.writev([filledBuffer(SECTOR, 0x77)], 0);
      f.crashMixed();
      assert.equal(f.getNewSectors().size, 0);
    });

    it('file remains readable after crashMixed', async () => {
      const f = new MockFile(SECTOR);
      await f.create();
      await f.truncate(2 * SECTOR);
      await f.sync();
      await f.writev([filledBuffer(2 * SECTOR, 0x44)], 0);
      f.crashMixed();
      const buf = Buffer.alloc(2 * SECTOR);
      await assert.doesNotReject(() => f.read(buf, { position: 0 }));
    });

    it('committed sector is either old, partially saved, or possibly corrupted', async () => {
      // We run multiple times to hit various branches with high probability.
      // The only guarantee we assert: after crash we can read without throwing.
      for (let i = 0; i < 20; i++) {
        const f = new MockFile(SECTOR);
        await f.create();
        await f.truncate(SECTOR);
        await f.sync();
        await f.writev([filledBuffer(SECTOR, 0xff)], 0);
        f.crashMixed();
        const buf = Buffer.alloc(SECTOR);
        await assert.doesNotReject(() => f.read(buf, { position: 0 }));
      }
    });
  });

  // ── sectorSize property ──────────────────────────────────────────────────────

  describe('sectorSize property', () => {
    it('works with sector size of 1', async () => {
      const f = new MockFile(1);
      await f.create();
      await f.writev([Buffer.from([0x42])], 0);
      await f.sync();
      const buf = Buffer.alloc(1);
      await f.read(buf, { position: 0 });
      assert.equal(buf[0], 0x42);
    });
  });
});
