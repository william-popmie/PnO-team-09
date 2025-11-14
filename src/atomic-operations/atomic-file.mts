// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-12

import type { File } from '../mockfile.mjs';
import type { WALManager } from './wal-manager.mjs';

/* simple async mutex to serialize async critical sections */
class Mutex {
  private locked = false;
  private waiters: (() => void)[] = [];

  async lock(): Promise<() => void> {
    return new Promise((resolve) => {
      const take = () => {
        this.locked = true;
        resolve(() => {
          this.locked = false;
          const next = this.waiters.shift();
          if (next) next();
        });
      };
      if (!this.locked) take();
      else this.waiters.push(take);
    });
  }

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const unlock = await this.lock();
    try {
      return await fn();
    } finally {
      unlock();
    }
  }
}

export interface AtomicFile {
  begin(): Promise<void>;
  journalWrite(offset: number, data: Uint8Array): Promise<void>;
  read(offset: number, length: number): Promise<Uint8Array>;
  journalCommit(): Promise<void>;
  checkpoint(): Promise<void>;
  recover(): Promise<void>;
  abort(): Promise<void>;
  safeShutdown(): Promise<void>;
}

export class AtomicFileImpl implements AtomicFile {
  private dbFile: File;
  private wal: WALManager;
  private inTransaction = false;
  private pendingWrites: { offset: number; data: Uint8Array }[] = [];
  private opened = false;
  private mutex = new Mutex();

  public constructor(dbFile: File, walManager: WALManager) {
    this.dbFile = dbFile;
    this.wal = walManager;
  }

  private async ensureOpen(): Promise<void> {
    if (this.opened) return;
    await this.dbFile.open();
    await this.wal.openWAL();
    this.opened = true;
  }

  public async begin(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (this.inTransaction) throw new Error('transaction already in progress');
      await this.ensureOpen();
      this.pendingWrites.length = 0;
      this.inTransaction = true;
    });
  }

  /**
   * Writes to WAL
   */
  public async journalWrite(offset: number, data: Uint8Array): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (!this.inTransaction) throw new Error('no active transaction');
      await this.wal.logWrite(offset, data);
      this.pendingWrites.push({ offset, data: data.slice() });
    });
  }

  public async read(offset: number, length: number): Promise<Uint8Array> {
    return this.mutex.runExclusive(async () => {
      await this.ensureOpen();
      const buf = Buffer.alloc(length);
      await this.dbFile.read(buf, { position: offset });
      return new Uint8Array(buf);
    });
  }

  /**
   * Ensures that WAL is not dirty and finalizes the transaction.
   */
  public async journalCommit(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (!this.inTransaction) throw new Error('no active transaction');

      await this.wal.addCommitMarker();
      await this.wal.sync();

      await this.wal.checkpoint();

      await this.dbFile.sync();

      await this.wal.clearLog();
      await this.wal.sync();
      this.pendingWrites.length = 0;
      this.inTransaction = false;
    });
  }

  /**
   * Writes to database (trigger checkpoint). Serialized to avoid races.
   */
  public async checkpoint(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.ensureOpen();
      await this.wal.checkpoint();
      await this.dbFile.sync();
      await this.wal.clearLog();
      await this.wal.sync();
    });
  }

  /**
   * Checks for a crash and recovers committed data in WAL, run on startup.
   */
  public async recover(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.ensureOpen();
      await this.wal.recover();
      await this.dbFile.sync();
      await this.wal.sync();
    });
  }

  public async abort(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (!this.inTransaction) throw new Error('no active transaction');
      this.pendingWrites.length = 0;
      await this.wal.clearLog();
      this.inTransaction = false;
      await this.safeShutdown();
    });
  }

  public async safeShutdown(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.dbFile.sync();
      await this.wal.sync();
      await this.dbFile.close();
      await this.wal.closeWAL();
      this.opened = false;
    });
  }
}
