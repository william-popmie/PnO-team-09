// @author Frederick Hillen
// @date 2025-11-11

import type { File } from '../mockfile.mjs';

export interface WALManager {
  logWrite(offset: number, data: Uint8Array): Promise<void>;
  recover(): Promise<{ offset: number; data: Uint8Array }[]>;
  clearLog(): Promise<void>;
}

export class WALManagerImpl implements WALManager {
  private walFile: File;

  public constructor(walFile: File) {
    this.walFile = walFile;
  }

  public async logWrite(offset: number, data: Uint8Array): Promise<void> {
    const buffer = Buffer.from(data);
    await this.walFile.writev([buffer], offset);
  }

  public async recover(): Promise<{ offset: number; data: Uint8Array }[]> {
    await this.walFile.stat();
    return [];
  }

  public async clearLog(): Promise<void> {}
}
