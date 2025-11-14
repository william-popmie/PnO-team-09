// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-14

import type { File } from '../mockfile.mjs';
import type { WALManager } from './wal-manager.mjs';

/**
 * Simple async mutex to serialize async critical sections
 * to avoid races (used in every function of atomic-file).
 */
class Mutex {
  private locked = false;
  private waiters: (() => void)[] = [];

  /**
   * Acquires the mutex lock.
   *
   * If the mutex is free, this call resolves immediately and returns an
   * `unlock` function.
   * If the mutex is already locked, the caller is queued and the Promise
   * resolves only when the mutex becomes available.
   *
   * @returns A function that releases the lock.
   */
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

  /**
   * Runs the given asynchronous function with exclusive access to the mutex.
   *
   * This helper acquires the lock, executes the provided function fn,
   * and guarantees that the lock is released afterwards.
   *
   * @param fn A function representing the critical section.
   * @returns The return value of fn.
   */
  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const unlock = await this.lock();
    try {
      return await fn();
    } finally {
      unlock();
    }
  }
}

/**
 * Interface to interact with atomic-file.
 */
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

/**
 * Implementation of atomicfile to ensure crash consistency.
 */
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

  /**
   * Open the WAL and database files.
   */
  private async ensureOpen(): Promise<void> {
    if (this.opened) return;
    await this.dbFile.open();
    await this.wal.openWAL();
    this.opened = true;
  }

  /**
   * Commence a transaction to modify the database.
   */
  public async begin(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (this.inTransaction) throw new Error('transaction already in progress');
      await this.ensureOpen();
      this.pendingWrites.length = 0;
      this.inTransaction = true;
    });
  }

  /**
   * Writes data to the WAL.
   * @param offset at which the data needs to be written in the database
   * @param data the data to be written to the database
   */
  public async journalWrite(offset: number, data: Uint8Array): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (!this.inTransaction) throw new Error('no active transaction');
      await this.wal.logWrite(offset, data);
      this.pendingWrites.push({ offset, data: data.slice() });
    });
  }

  /**
   * Reads a sequence of bytes from the database file.
   *
   * @param offset The byte position in the database file from which reading
   *               should begin.
   * @param length The number of bytes to read.
   * @returns A Uint8Array containing the read data from the database file.
   */
  public async read(offset: number, length: number): Promise<Uint8Array> {
    return this.mutex.runExclusive(async () => {
      await this.ensureOpen();
      const buf: Buffer = Buffer.alloc(length);
      await this.dbFile.read(buf, { position: offset });
      return new Uint8Array(buf);
    });
  }

  /**
   * Commits data to the WAL by adding a marker.
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
   * Writes to database (trigger checkpoint).
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
    });
  }

  /**
   * Aborts the active transaction and clears the WAL.
   */
  public async abort(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      if (!this.inTransaction) throw new Error('no active transaction');
      this.pendingWrites.length = 0;
      await this.wal.clearLog();
      this.inTransaction = false;
      await this.safeShutdown();
    });
  }

  /**
   * Shuts down atomic-file safely.
   */
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
