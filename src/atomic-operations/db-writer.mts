// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-12

import type { AtomicFile } from './atomic-file.mjs';

export interface DBWriter {
  beginTransaction(): void;
  writeData(offset: number, data: Uint8Array): Promise<void>;
  commitToJournal(): Promise<void>;
  commitToDatabase(): Promise<void>;
  recoverFromWAL(): Promise<void>;
}

/**
 * Lightweight wrapper that uses AtomicFile for transactional operations.
 * Keeps DB logic out of the AtomicFile orchestration layer.
 * 
 * It should be used like this:
 * At startup:
 * 
   await atomic-file.recoverFromWall();

 * To interact with database:

   await beginTransacion();
   await writeData(offset: number, data: Uint8Array);
   await commitToJournal();
   await commitToDatabase();
   
 */
export class DBWriterImpl implements DBWriter {
  private atomicFile: AtomicFile;

  public constructor(atomicFile: AtomicFile) {
    this.atomicFile = atomicFile;
  }

  public beginTransaction(): void {
    this.atomicFile.begin();
  }

  public async writeData(offset: number, data: Uint8Array): Promise<void> {
    await this.atomicFile.journalWrite(offset, data);
  }

  public async commitToJournal(): Promise<void> {
    await this.atomicFile.journalCommit();
  }

  public async commitToDatabase(): Promise<void> {
    await this.atomicFile.checkpoint();
  }

  public async recoverFromWAL(): Promise<void> {
    await this.atomicFile.recover();
  }
}
