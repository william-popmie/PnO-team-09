// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-12

import type { File } from '../mockfile.mjs';

export interface WALManager {
  logWrite(offset: number, data: Uint8Array): Promise<void>;
  addCommitMarker(): Promise<void>;
  sync(): Promise<void>;
  checkpoint(): Promise<void>;
  recover(): Promise<void>;
  clearLog(): Promise<void>;
}

export class WALManagerImpl implements WALManager {
  private walFile: File;
  private dbFile: File;

  public constructor(walFile: File, dbFile: File) {
    this.walFile = walFile;
    this.dbFile = dbFile;
  }

  public async logWrite(offset: number, data: Uint8Array): Promise<void> {
    const header: Uint32Array = new Uint32Array([offset, data.length]);
    const pos: number = await this.walFile.stat().then((stat) => stat.size);
    await this.walFile.writev([Buffer.from(header.buffer), Buffer.from(data)], pos);
  }

  public async addCommitMarker(): Promise<void> {
    const marker: Buffer = Buffer.from('COMMIT\n');
    const pos: number = await this.walFile.stat().then((stat) => stat.size);
    await this.walFile.writev([marker], pos);
  }

  public async sync(): Promise<void> {
    await this.walFile.sync();
  }

  public async checkpoint() {
    const walSize: number = await this.walFile.stat().then((stat) => stat.size);
    if (walSize === 0) return;
    let pos: number = 0;

    const buffer = Buffer.alloc(walSize);
    await this.walFile.read(buffer, { position: 0 });

    while (pos < walSize) {
      const valid: { valid: boolean; nextPos: number } = this.checkValidityWalData(walSize, buffer, pos);
      if (!valid.valid) {
        return;
      }

      pos = valid.nextPos;
    }

    const marker = Buffer.from('COMMIT\n');
    pos = 0;
    const writes: { offset: number; data: Buffer }[] = [];
    while (pos + 8 <= walSize) {
      const offset = buffer.readUInt32LE(pos);
      const length = buffer.readUInt32LE(pos + 4);
      const dataStart = pos + 8;
      const dataEnd = dataStart + length;
      const commitEnd = dataEnd + marker.length;
      const dataBuf = buffer.slice(dataStart, dataEnd);
      writes.push({ offset, data: dataBuf });
      pos = commitEnd;
    }

    for (const w of writes) {
      await this.dbFile.writev([Buffer.from(w.data)], w.offset);
    }

    await this.clearLog();
  }

  private checkValidityWalData(walSize: number, buffer: Buffer, pos: number): { valid: boolean; nextPos: number } {
    const marker: Buffer = Buffer.from('COMMIT\n');
    while (pos + 8 <= walSize) {
      const dataCommitLength: number = buffer.readUInt32LE(pos + 4);
      const end = pos + 8 + dataCommitLength + marker.length;
      if (end <= walSize && buffer.subarray(end - marker.length, end).equals(marker)) {
        return { valid: true, nextPos: end };
      }
      pos = end - marker.length;
    }
    console.log('Uncommitted changes detected. Flushing...');
    return { valid: false, nextPos: 0 };
  }

  public async recover(): Promise<void> {
    const journalSize: number = await this.walFile.stat().then((stat) => stat.size);
    if (!(journalSize > 0)) {
      return;
    }

    console.log('A crash has been detected. Trying to recover WAL...');

    const journalBuffer: Buffer = Buffer.alloc(journalSize);
    await this.walFile.read(journalBuffer, { position: 0 });
    const journalContents: string = journalBuffer.toString();

    if (!journalContents.includes('COMMIT')) {
      console.log('WAL is dirty. Flushing...');
      await this.clearLog();
      return;
    }

    console.log('Recovering committed WAL...');
    await this.checkpoint();
    return;
  }

  public async clearLog(): Promise<void> {
    await this.walFile.truncate(0);
  }
}
