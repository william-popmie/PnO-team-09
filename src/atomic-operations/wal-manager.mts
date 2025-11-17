// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-17

import type { File } from '../mockfile.mjs';

/**
 * Interface to manage the write-ahead log.
 */
export interface WALManager {
  openWAL(): Promise<void>;
  closeWAL(): Promise<void>;
  logWrite(offset: number, data: Uint8Array): Promise<void>;
  addCommitMarker(): Promise<void>;
  sync(): Promise<void>;
  checkpoint(): Promise<void>;
  recover(): Promise<void>;
  clearLog(): Promise<void>;
}

/**
 *  Small async mutex used to serialize WAL file operations
 *  to avoid races (used in every function of wal-manager).
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
   * @param {() => Promise<T>} fn A function representing the critical section.
   * @returns {T} The return value of fn.
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
 * Implementation of the write-ahead log manager,
 * that executes operations on the wal.
 */
export class WALManagerImpl implements WALManager {
  private walFile: File;
  private dbFile: File;
  private mutex: Mutex = new Mutex();
  private opened: boolean = false;

  /**
   * Constructor
   * @param {File} walFile the write-ahead log file
   * @param {File} dbFile the database file
   */
  public constructor(walFile: File, dbFile: File) {
    this.walFile = walFile;
    this.dbFile = dbFile;
  }

  /**
   * Opens the WAL and database files.
   */
  public async openWAL(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.walFile.open();
      await this.dbFile.open();
      this.opened = true;
    });
  }

  /**
   * Closes the WAL and database files.
   */
  public async closeWAL(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.walFile.close();
      await this.dbFile.close();
      this.opened = false;
    });
  }

  /**
   * Writes data to the WAL.
   * @param {number} offset a number >= 0 at which the data needs to be written in the database
   * @param {Uint8Array} data a Uint8Array at which the data to be written to the database
   */
  public async logWrite(offset: number, data: Uint8Array): Promise<void> {
    return this.mutex.runExclusive(async () => {
      const checksum: number = this.checksumCalculator(data);
      const header: Uint32Array = new Uint32Array([offset, data.length, checksum]);
      const pos: number = await this.walFile.stat().then((stat) => stat.size);
      await this.walFile.writev([Buffer.from(header.buffer), Buffer.from(data)], pos);
    });
  }

  /**
   * Commits the data to the WAL to ensure it is complete
   * before writing to database.
   */
  public async addCommitMarker(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      const marker: Buffer = Buffer.from('COMMIT\n');
      const pos: number = await this.walFile.stat().then((stat) => stat.size);
      await this.walFile.writev([marker], pos);
    });
  }

  /**
   * Syncs the WAL-file
   */
  public async sync(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.walFile.sync();
    });
  }

  /**
   * Writes all committed data in the WAL to the database.
   */
  public async checkpoint(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.checkpointInternal();
    });
  }

  /**
   * internal checkpoint implementation that does not acquire the mutex.
   */
  private async checkpointInternal(): Promise<void> {
    const walSize: number = await this.walFile.stat().then((stat) => stat.size);
    if (walSize === 0) return;

    const buffer = Buffer.alloc(walSize);
    await this.walFile.read(buffer, { position: 0 });

    const committedData = this.getCommittedData(walSize, buffer);
    for (const w of committedData.writes) {
      await this.dbFile.writev([Buffer.from(w.data)], w.offset);
    }
  }

  /**
   * Helper function that extracts all committed data from the WAL.
   * @param {number} walSize a number > 0, the size of the WAL
   * @param {Buffer} buffer contents of WAL
   * @returns {{ writes: { offset: number; data: Buffer }[] }} An object containing an array writes, of which each element represents a fully committed write.
   */
  private getCommittedData(walSize: number, buffer: Buffer): { writes: { offset: number; data: Buffer }[] } {
    let pos: number = 0;
    const marker: Buffer = Buffer.from('COMMIT\n');
    let writes: { offset: number; data: Buffer }[] = [];
    let tempWrites: { offset: number; data: Buffer }[] = [];
    while (pos + 12 <= walSize) {
      const headerSize: number = 12;
      const size: number = headerSize + buffer.readUInt32LE(pos + 4);
      const offset: number = buffer.readUInt32LE(pos);
      const checksum = buffer.readUInt32LE(pos + 8);
      const data: Buffer = buffer.subarray(pos + headerSize, pos + size);

      if (pos + size + marker.length > walSize) break;
      const check: number = this.checksumCalculator(data);
      if (check !== checksum) {
        console.log('All previous commits to the WAL were corrupted, sorry... :(( \nFlusing...');
        writes = [];
        break;
      }
      if (
        pos + size + marker.length <= walSize &&
        buffer.subarray(pos + size, pos + size + marker.length).equals(marker)
      ) {
        for (const w of tempWrites) {
          writes.push(w);
        }
        writes.push({ offset, data });
        tempWrites = [];
        pos += size + marker.length;
      } else {
        tempWrites.push({ offset, data });
        pos += size;
      }
    }
    return { writes };
  }

  /**
   * Checks for a crash and reruns checkpoint if the WAL contains
   * any committed data to ensure crash consistency.
   */
  public async recover(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      const journalSize: number = await this.walFile.stat().then((stat) => stat.size);
      if (!(journalSize > 0)) {
        return;
      }

      console.log('A crash has been detected. Trying to recover WAL...');

      const journalBuffer: Buffer = Buffer.alloc(journalSize);
      await this.walFile.read(journalBuffer, { position: 0 });
      const journalContents: string = journalBuffer.toString();

      if (!journalContents.includes('COMMIT')) {
        console.log('No committed changes detected. Flushing...');
        await this.walFile.truncate(0);
        return;
      }

      console.log('Recovering committed WAL...');
      await this.checkpointInternal();
      console.log('Committed changes succesfully recovered.');
      await this.walFile.truncate(0);
    });
  }

  public async clearLog(): Promise<void> {
    return this.mutex.runExclusive(async () => {
      await this.walFile.truncate(0);
    });
  }

  private checksumCalculator(buf: Uint8Array | Buffer): number {
    let hash: number = 0x811c9dc5;
    for (const i of buf) {
      hash ^= buf[i];
      hash = (hash * 0x01000193) >>> 0;
    }
    return hash >>> 0;
  }

  // Helper functions for testing

  public getOpen(): boolean {
    return this.opened;
  }
}
