// @author Tijn Gommers
// @date 2025-11-18
import assert from 'node:assert/strict';
import { MockFile } from './mockfile.mjs';
import { describe as _describe, it as _it } from 'vitest';

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

const describe = (name: string, fn: () => void): void => {
  void _describe(name, fn);
};
const it = (name: string, fn: () => Promise<void>): void => {
  void _it(name, fn);
};
/** Creates and opens a fresh MockFile with the given sector size (default 16). */
async function openedFile(sectorSize = 16): Promise<MockFile> {
  const file = new MockFile(sectorSize);
  await file.create();
  return file;
}

/**
 * Writes `data` at `position` and returns the file for chaining.
 * `data` is encoded as UTF-8.
 */
async function writeString(file: MockFile, data: string, position: number): Promise<MockFile> {
  await file.writev([Buffer.from(data, 'utf8')], position);
  return file;
}

/**
 * Reads `length` bytes from `position` and returns them as a UTF-8 string.
 */
async function readString(file: MockFile, length: number, position: number): Promise<string> {
  const buf = Buffer.alloc(length);
  await file.read(buf, { position });
  return buf.toString('utf8');
}

// ─────────────────────────────────────────────────────────────────────────────
// create()
// ─────────────────────────────────────────────────────────────────────────────

describe('create()', () => {
  it('opens a new file with size 0', async () => {
    const file = new MockFile(16);
    await file.create();
    const { size } = await file.stat();
    assert.equal(size, 0);
  });

  it('resets an existing file when called after close()', async () => {
    const file = new MockFile(16);
    await file.create();
    await writeString(file, 'hello', 0);
    await file.sync();
    await file.close();

    // Re-create: all data must be gone.
    await file.create();
    const { size } = await file.stat();
    assert.equal(size, 0);
  });

  it('throws when the file is already open', async () => {
    const file = new MockFile(16);
    await file.create();
    await assert.rejects(() => file.create(), /already open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// open()
// ─────────────────────────────────────────────────────────────────────────────

describe('open()', () => {
  it('opens a closed file without changing its contents', async () => {
    const file = new MockFile(16);
    await file.create();
    await writeString(file, 'hello', 0);
    await file.sync();
    await file.close();

    await file.open();
    const result = await readString(file, 5, 0);
    assert.equal(result, 'hello');
  });

  it('throws when the file is already open', async () => {
    const file = await openedFile();
    await assert.rejects(() => file.open(), /already open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// close()
// ─────────────────────────────────────────────────────────────────────────────

describe('close()', () => {
  it('closes an open file without error', async () => {
    const file = await openedFile();
    await file.sync();
    await assert.doesNotReject(() => file.close());
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    await assert.rejects(() => file.close(), /not open/i);
  });

  it('throws when there are unsynced pending writes', async () => {
    const file = await openedFile();
    await writeString(file, 'dirty', 0);
    await assert.rejects(() => file.close(), /unsynced/i);
  });

  it('allows reopening after close', async () => {
    const file = await openedFile();
    await file.sync();
    await file.close();
    await assert.doesNotReject(() => file.open());
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// sync()
// ─────────────────────────────────────────────────────────────────────────────

describe('sync()', () => {
  it('commits pending writes so they survive a re-read', async () => {
    const file = await openedFile();
    await writeString(file, 'synced', 0);
    await file.sync();
    const result = await readString(file, 6, 0);
    assert.equal(result, 'synced');
  });

  it('clears newSectors after sync', async () => {
    const file = await openedFile();
    await writeString(file, 'data', 0);
    await file.sync();
    assert.equal(file.getNewSectors().size, 0);
  });

  it('is idempotent: syncing twice does not throw', async () => {
    const file = await openedFile();
    await writeString(file, 'data', 0);
    await file.sync();
    await assert.doesNotReject(() => file.sync());
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    await assert.rejects(() => file.sync(), /not open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// writev()
// ─────────────────────────────────────────────────────────────────────────────

describe('writev()', () => {
  it('writes a single buffer at position 0', async () => {
    const file = await openedFile();
    await writeString(file, 'hello', 0);
    await file.sync();
    assert.equal(await readString(file, 5, 0), 'hello');
  });

  it('writes multiple buffers concatenated at position 0', async () => {
    const file = await openedFile();
    await file.writev([Buffer.from('hel', 'utf8'), Buffer.from('lo', 'utf8')], 0);
    await file.sync();
    assert.equal(await readString(file, 5, 0), 'hello');
  });

  it('writes at a non-zero position without corrupting preceding bytes', async () => {
    const file = await openedFile(16);
    await writeString(file, 'AAAAAAAAAA', 0); // 10 bytes
    await file.sync();
    await writeString(file, 'BBB', 4);
    await file.sync();
    // Bytes 0–3 must still be 'AAAA', bytes 4–6 must be 'BBB'.
    assert.equal(await readString(file, 4, 0), 'AAAA');
    assert.equal(await readString(file, 3, 4), 'BBB');
  });

  it('extends the file when writing beyond the current size', async () => {
    const file = await openedFile();
    await writeString(file, 'hi', 10);
    const { size } = await file.stat();
    assert.equal(size, 12);
  });

  it('stages writes in newSectors before sync', async () => {
    const file = await openedFile(16);
    await writeString(file, 'pending', 0);
    assert.ok(file.getNewSectors().size > 0, 'Expected pending writes in newSectors.');
  });

  it('writes spanning multiple sectors are read back correctly', async () => {
    // sectorSize = 8, write 20 bytes → spans 3 sectors.
    const file = await openedFile(8);
    const data = 'ABCDEFGHIJKLMNOPQRST'; // 20 bytes
    await writeString(file, data, 0);
    await file.sync();
    assert.equal(await readString(file, 20, 0), data);
  });

  it('overwrites previously written data', async () => {
    const file = await openedFile();
    await writeString(file, 'hello world', 0);
    await file.sync();
    await writeString(file, 'HELLO', 0);
    await file.sync();
    assert.equal(await readString(file, 11, 0), 'HELLO world');
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    await assert.rejects(() => file.writev([Buffer.from('x')], 0), /not open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// read()
// ─────────────────────────────────────────────────────────────────────────────

describe('read()', () => {
  it('reads back exactly what was written and synced', async () => {
    const file = await openedFile();
    await writeString(file, 'hello', 0);
    await file.sync();
    assert.equal(await readString(file, 5, 0), 'hello');
  });

  it('reads pending (unsynced) writes', async () => {
    const file = await openedFile();
    await writeString(file, 'pending', 0);
    // No sync — read should still return the latest pending value.
    assert.equal(await readString(file, 7, 0), 'pending');
  });

  it('reads a sub-range correctly', async () => {
    const file = await openedFile();
    await writeString(file, 'hello world', 0);
    await file.sync();
    assert.equal(await readString(file, 5, 6), 'world');
  });

  it('reads data spanning multiple sectors correctly', async () => {
    const file = await openedFile(8);
    const data = 'ABCDEFGHIJKLMNOPQRST'; // 20 bytes, 3 sectors of size 8
    await writeString(file, data, 0);
    await file.sync();
    assert.equal(await readString(file, 20, 0), data);
  });

  it('throws when reading out of bounds', async () => {
    const file = await openedFile();
    await writeString(file, 'hi', 0);
    await file.sync();
    const buf = Buffer.alloc(100);
    await assert.rejects(() => file.read(buf, { position: 0 }), /out of bounds/i);
  });

  it('throws when position is negative', async () => {
    const file = await openedFile();
    await writeString(file, 'hello', 0);
    await file.sync();
    const buf = Buffer.alloc(1);
    await assert.rejects(() => file.read(buf, { position: -1 }), /non-negative/i);
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    const buf = Buffer.alloc(4);
    await assert.rejects(() => file.read(buf, { position: 0 }), /not open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// truncate()
// ─────────────────────────────────────────────────────────────────────────────

describe('truncate()', () => {
  it('shrinks the file to the given length', async () => {
    const file = await openedFile();
    await writeString(file, 'hello world', 0);
    await file.sync();
    await file.truncate(5);
    const { size } = await file.stat();
    assert.equal(size, 5);
  });

  it('extends the file with zero bytes', async () => {
    const file = await openedFile();
    await file.truncate(32);
    const { size } = await file.stat();
    assert.equal(size, 32);
    // Extended region must be zero-filled.
    const buf = Buffer.alloc(32);
    await file.read(buf, { position: 0 });
    assert.ok(
      buf.every((b) => b === 0),
      'Expected extended region to be zero-filled.',
    );
  });

  it('discards pending writes for sectors beyond the new length', async () => {
    // sectorSize = 8: write into sector 1 (bytes 8–15), then truncate to 8 bytes.
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0); // sector 0
    await writeString(file, 'BBBBBBBB', 8); // sector 1
    await file.truncate(8); // remove sector 1
    assert.equal(file.getNewSectors().has(1), false);
  });

  it('truncating to current size is a no-op', async () => {
    const file = await openedFile();
    await writeString(file, 'hello', 0);
    await file.sync();
    const { size: before } = await file.stat();
    await file.truncate(before);
    const { size: after } = await file.stat();
    assert.equal(after, before);
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    await assert.rejects(() => file.truncate(0), /not open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// stat()
// ─────────────────────────────────────────────────────────────────────────────

describe('stat()', () => {
  it('returns size 0 for a freshly created file', async () => {
    const file = await openedFile();
    const { size } = await file.stat();
    assert.equal(size, 0);
  });

  it('reflects size after a write', async () => {
    const file = await openedFile();
    await writeString(file, 'hello', 0);
    const { size } = await file.stat();
    assert.equal(size, 5);
  });

  it('reflects size after truncate (shrink)', async () => {
    const file = await openedFile();
    await writeString(file, 'hello world', 0);
    await file.sync();
    await file.truncate(3);
    const { size } = await file.stat();
    assert.equal(size, 3);
  });

  it('reflects size after truncate (extend)', async () => {
    const file = await openedFile();
    await file.truncate(64);
    const { size } = await file.stat();
    assert.equal(size, 64);
  });

  it('throws when the file is not open', async () => {
    const file = new MockFile(16);
    await assert.rejects(() => file.stat(), /not open/i);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Crash simulations
// ─────────────────────────────────────────────────────────────────────────────

describe('crashBasic()', () => {
  it('clears all pending writes', async () => {
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    file.crashBasic();
    assert.equal(file.getNewSectors().size, 0);
  });

  it('leaves committed data intact or partially applied', async () => {
    // After a basic crash, committed sectors must contain either the
    // old value or a value from the pending writes — never garbage.
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    await file.sync();
    await writeString(file, 'BBBBBBBB', 0);
    file.crashBasic();
    // Re-open to read back.
    await file.open();
    const result = await readString(file, 8, 0);
    assert.ok(result === 'AAAAAAAA' || result === 'BBBBBBBB', `Unexpected data after crash: "${result}"`);
  });
});

describe('crashFullLoss()', () => {
  it('resets the file to empty and closed state', async () => {
    const file = await openedFile(8);
    await writeString(file, 'important data', 0);
    await file.sync();
    file.crashFullLoss();

    // File must be closed after a full-loss crash.
    await assert.rejects(() => file.stat(), /not open/i);

    // After re-opening, the file must be empty.
    await file.create();
    const { size } = await file.stat();
    assert.equal(size, 0);
  });

  it('clears all pending writes', async () => {
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    file.crashFullLoss();
    assert.equal(file.getNewSectors().size, 0);
  });
});

describe('crashPartialCorruption()', () => {
  it('clears all pending writes', async () => {
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    file.crashPartialCorruption();
    assert.equal(file.getNewSectors().size, 0);
  });

  it('corrupts data (result differs from original)', async () => {
    // Run multiple times to reduce the chance of a false negative from the
    // random byte flip landing on the same bit.
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    await file.sync();

    let corrupted = false;
    for (let attempt = 0; attempt < 20; attempt++) {
      await file.open();
      file.crashPartialCorruption();
      await file.open();
      const result = await readString(file, 8, 0);
      if (result !== 'AAAAAAAA') {
        corrupted = true;
        break;
      }
      await file.sync();
      await file.close();
    }
    assert.ok(corrupted, 'Expected at least one corruption in 20 attempts.');
  });
});

describe('crashMixed()', () => {
  it('clears all pending writes', async () => {
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    file.crashMixed();
    assert.equal(file.getNewSectors().size, 0);
  });

  it('leaves each sector in a valid state (old value, new value, or corrupted)', async () => {
    const file = await openedFile(8);
    await writeString(file, 'AAAAAAAA', 0);
    await file.sync();
    await writeString(file, 'BBBBBBBB', 0);
    file.crashMixed();

    // The file must still be readable after the crash — it must not throw.
    await file.open();
    const buf = Buffer.alloc(8);
    await assert.doesNotReject(() => file.read(buf, { position: 0 }));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end lifecycle
// ─────────────────────────────────────────────────────────────────────────────

describe('lifecycle', () => {
  it('full create → write → sync → close → open → read cycle works correctly', async () => {
    const file = new MockFile(16);

    await file.create();
    await writeString(file, 'lifecycle test', 0);
    await file.sync();
    await file.close();

    await file.open();
    const result = await readString(file, 14, 0);
    assert.equal(result, 'lifecycle test');

    await file.sync();
    await file.close();
  });

  it('multiple writes across sectors are all readable after sync', async () => {
    const file = await openedFile(8);
    // Write three sectors worth of data in separate calls.
    await writeString(file, 'SECTOR_1', 0); // sector 0
    await writeString(file, 'SECTOR_2', 8); // sector 1
    await writeString(file, 'SECTOR_3', 16); // sector 2
    await file.sync();

    assert.equal(await readString(file, 8, 0), 'SECTOR_1');
    assert.equal(await readString(file, 8, 8), 'SECTOR_2');
    assert.equal(await readString(file, 8, 16), 'SECTOR_3');
  });

  it('write without sync is visible before close', async () => {
    const file = await openedFile();
    await writeString(file, 'unsync', 0);
    // Must be readable even without sync.
    assert.equal(await readString(file, 6, 0), 'unsync');
  });
});
