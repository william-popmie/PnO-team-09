import { describe, it, expect } from 'vitest';
import { MockFile } from '../mockfile.mjs';
import { WALManagerImpl } from './wal-manager.mjs';
import { AtomicFileImpl } from './atomic-file.mjs';

describe('AtomicFile + WALManager integration (concurrency safe skeleton)', () => {
  it('journalCommit applies WAL writes to DB and clears WAL', async () => {
    const dbFile = new MockFile(512);
    const walFile = new MockFile(512);
    await dbFile.create();
    await walFile.create();

    const wal = new WALManagerImpl(walFile, dbFile);
    const atomic = new AtomicFileImpl(dbFile, wal);

    await atomic.begin();
    await atomic.journalWrite(0, Uint8Array.from([1, 2, 3]));
    await atomic.journalCommit();

    // read applied data from DB file
    const out = Buffer.alloc(3);
    await dbFile.read(out, { position: 0 });
    expect(Array.from(out)).toEqual([1, 2, 3]);

    // WAL should be cleared after commit
    const walStat = await walFile.stat();
    expect(walStat.size).toBe(0);

    await atomic.safeShutdown();
  });

  it('recover drops uncommitted WAL entries (no commit marker)', async () => {
    const dFile = new MockFile(512);
    const walFile = new MockFile(512);
    await dFile.create();
    await walFile.create();

    const wal = new WALManagerImpl(walFile, dFile);
    await wal.openWAL();

    // write a log entry but do NOT add commit marker
    await wal.logWrite(16, Uint8Array.from([9, 9, 9]));

    // call recover via an AtomicFile wrapper (recover will clear WAL if no COMMIT)
    const atomic = new AtomicFileImpl(dFile, wal);
    await atomic.recover();
    console.log('AtomicFile recover called');

    // WAL should be cleared and DB untouched
    const walStat = await walFile.stat();
    const dbStat = await dFile.stat();
    expect(walStat.size).toBe(0);
    expect(dbStat.size).toBe(0);

    await wal.closeWAL();
  });

  it('recover applies committed WAL entries', async () => {
    const dbFile = new MockFile(512);
    const walFile = new MockFile(512);
    await dbFile.create();
    await walFile.create();

    const wal = new WALManagerImpl(walFile, dbFile);
    await wal.openWAL();

    // write entry and commit marker (simulate a flushed WAL)
    await wal.logWrite(8, Uint8Array.from([7, 8]));
    await wal.addCommitMarker();
    await wal.sync(); // ensure commit marker is durable for recover path

    const atomic = new AtomicFileImpl(dbFile, wal);
    await atomic.recover();

    // committed writes should be applied to DB
    const out = Buffer.alloc(2);
    await dbFile.read(out, { position: 8 });
    expect(Array.from(out)).toEqual([7, 8]);

    // WAL should be cleared after successful recovery
    const walStat = await walFile.stat();
    expect(walStat.size).toBe(0);

    await wal.closeWAL();
    await atomic.safeShutdown();
  });
});
