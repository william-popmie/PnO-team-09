// @author Frederick Hillen
// @date 2025-11-10

import type { File } from './mockfile.mjs';

export interface AtomicFile {
  begin(): Promise<void>;
  write(offset: number, data: Uint8Array): Promise<void>;
  read(offset: number, length: number): Promise<Uint8Array>;
  commit(): Promise<void>;
  recover(): Promise<void>;
  abort(): Promise<void>;
}

export class AtomicFileImpl implements AtomicFile {
  private dbFile: File;
  private WALFile: File;
  private inTransaction: boolean = false;

  public constructor(dbFile: File, WALFile: File) {
    this.dbFile = dbFile;
    this.WALFile = WALFile;
  }

  public async begin(): Promise<void> {
    if (this.inTransaction) {
      throw new Error('Transaction already in progress');
    }
    this.inTransaction = true;
    void (await Promise.resolve());
  }

  public async write(offset: number, data: Uint8Array): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No active transaction');
    }
    // await the WAL write so callers observe completion/errors
    await this.WALFile.writev([Buffer.from(data)], offset);
  }

  public async read(offset: number, length: number): Promise<Uint8Array> {
    // Read from dbFile
    const buffer = Buffer.alloc(length);
    await this.dbFile.read(buffer, { position: offset });
    return new Uint8Array(buffer);
  }

  public async commit(): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No active transaction');
    }
    // Commit logic here
    this.inTransaction = false;
    void (await Promise.resolve());
  }

  public async recover(): Promise<void> {
    // Recovery logic here
  }

  public async abort(): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No active transaction');
    }
    // Abort logic here
    this.inTransaction = false;
    void (await Promise.resolve());
  }
}
