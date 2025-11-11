// @author Frederick Hillen
// @date 2025-11-11

import type { File } from '../mockfile.mjs';
import type { WALManager } from './wal-manager.mjs';

/**
 * AtomicFile is the orchestration layer for transactional writes.
 * - Logs writes via WALManager
 * - Stages pending writes in memory during a transaction
 * - Replays pending/wal records to the DB file on commit/recover
 *
 * This is a minimal skeleton: real durability guarantees require WAL fsyncs,
 * checksums, and robust recovery handling.
 */
export interface AtomicFile {
  begin(): void;
  write(offset: number, data: Uint8Array): Promise<void>;
  read(offset: number, length: number): Promise<Uint8Array>;
  commit(): Promise<void>;
  recover(): Promise<void>;
  abort(): Promise<void>;
}

export class AtomicFileImpl implements AtomicFile {
  private dbFile: File;
  private wal: WALManager;
  private inTransaction = false;
  private pendingWrites: { offset: number; data: Uint8Array }[] = [];

  public constructor(dbFile: File, walManager: WALManager) {
    this.dbFile = dbFile;
    this.wal = walManager;
  }

  public begin(): void {
    if (this.inTransaction) throw new Error('transaction already in progress');
    this.pendingWrites.length = 0;
    this.inTransaction = true;
  }

  public async write(offset: number, data: Uint8Array): Promise<void> {
    if (!this.inTransaction) throw new Error('no active transaction');
    // 1) log to WAL (skeleton: WALManagerImpl keeps in-memory records)
    await this.wal.logWrite(offset, data);
    // 2) stage write for commit
    this.pendingWrites.push({ offset, data: data.slice() });
  }

  public async read(offset: number, length: number): Promise<Uint8Array> {
    // Simple pass-through read from dbFile. For full correctness should
    // consider staged pendingWrites that overlap the read range.
    const buf = Buffer.alloc(length);
    await this.dbFile.read(buf, { position: offset });
    return new Uint8Array(buf);
  }

  public async commit(): Promise<void> {
    if (!this.inTransaction) throw new Error('no active transaction');

    // Apply staged writes to DB file in order. Real impl must ensure durability
    // (fsync, ordering guarantees) before clearing WAL.
    for (const w of this.pendingWrites) {
      await this.dbFile.writev([Buffer.from(w.data)], w.offset);
    }

    // Clear WAL (skeleton)
    await this.wal.clearLog();

    this.pendingWrites.length = 0;
    this.inTransaction = false;
  }

  public async recover(): Promise<void> {
    // Ask WAL for pending records and apply them to DB file.
    const records = await this.wal.recover();
    for (const r of records) {
      await this.dbFile.writev([Buffer.from(r.data)], r.offset);
    }

    // After replay, clear WAL
    await this.wal.clearLog();
  }

  public async abort(): Promise<void> {
    if (!this.inTransaction) throw new Error('no active transaction');
    // Discard staged writes, clear WAL (skeleton assumes WAL contains only this tx)
    this.pendingWrites.length = 0;
    await this.wal.clearLog();
    this.inTransaction = false;
  }
}
