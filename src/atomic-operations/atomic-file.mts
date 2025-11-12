// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-12

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
 *
 * SEE db-writer.mts FOR SEQUENCE OF USE OF FUNCTIONS
 */
export interface AtomicFile {
  begin(): void;
  journalWrite(offset: number, data: Uint8Array): Promise<void>;
  read(offset: number, length: number): Promise<Uint8Array>;
  journalCommit(): Promise<void>;
  checkpoint(): Promise<void>;
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

  /**
   * Writes to WAL
   */
  public async journalWrite(offset: number, data: Uint8Array): Promise<void> {
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

  /**
   * Ensures that WAL is not dirty
   */
  public async journalCommit(): Promise<void> {
    if (!this.inTransaction) throw new Error('no active transaction');

    // Dubbele sync() om CommitMarker durable te maken zonder atomiciteit te verliezen bij crash tijdens addCommitMarker()
    await this.wal.sync();
    await this.wal.addCommitMarker();
    await this.wal.sync();

    this.pendingWrites.length = 0;
    this.inTransaction = false;
  }

  /**
   * Writes to database
   */
  public async checkpoint(): Promise<void> {
    await this.wal.checkpoint();
    await this.dbFile.sync();
  }

  /**
   * Checks for a crash and recovers committed data in WAL, run on startup.
   */
  public async recover(): Promise<void> {
    await this.wal.recover();
  }

  public async abort(): Promise<void> {
    if (!this.inTransaction) throw new Error('no active transaction');
    // Discard staged writes, clear WAL (skeleton assumes WAL contains only this tx)
    this.pendingWrites.length = 0;
    await this.wal.clearLog();
    this.inTransaction = false;
  }
}
