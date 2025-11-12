// @author Frederick Hillen, Arwin Gorissen
// @date 2025-11-12

import type { File } from '../mockfile.mjs';

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

export class WALManagerImpl implements WALManager {
  private walFile: File;
  private dbFile: File;

  public constructor(walFile: File, dbFile: File) {
    this.walFile = walFile;
    this.dbFile = dbFile;
  }

  public async openWAL(): Promise<void> {
    await this.walFile.open();
  }

  public async closeWAL(): Promise<void> {
    await this.walFile.close();
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

    const buffer = Buffer.alloc(walSize);
    await this.walFile.read(buffer, { position: 0 });

    const committedData = this.getCommittedData(walSize, buffer);
    if (committedData.writes.length === 0) {
      console.log('No committed changes detected. Flushing...');
    } else {
      for (const w of committedData.writes) {
        await this.dbFile.writev([Buffer.from(w.data)], w.offset);
      }
    }
  }

  private getCommittedData(walSize: number, buffer: Buffer): { writes: { offset: number; data: Buffer }[] } {
    let pos: number = 0;
    const marker: Buffer = Buffer.from('COMMIT\n');
    const writes: { offset: number; data: Buffer }[] = [];
    let tempWrites: { offset: number; data: Buffer }[] = [];
    while (pos + 8 <= walSize) {
      const dataCommitLength: number = buffer.readUInt32LE(pos + 4);
      const end = pos + 8 + dataCommitLength + marker.length;
      const offset: number = buffer.readUInt32LE(pos);
      const data: Buffer = buffer.subarray(pos + 8, pos + 8 + dataCommitLength);
      if (pos + 8 + dataCommitLength + marker.length > walSize) break;
      if (end <= walSize && buffer.subarray(end - marker.length, end).equals(marker)) {
        for (const w of tempWrites) {
          writes.push(w);
        }
        writes.push({ offset, data });
        tempWrites = [];
        pos = end;
      } else {
        tempWrites.push({ offset, data });
        pos = end - marker.length;
      }
    }
    return { writes };
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
