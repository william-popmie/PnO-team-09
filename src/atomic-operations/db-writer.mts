// @author Frederick Hillen
// @date 2025-11-11

import type { File } from '../mockfile.mjs';
import type { AtomicFile } from './atomic-file.mjs';

export interface DBWriter {
  beginTransaction(): void;
  writeData(offset: number, data: Uint8Array): Promise<void>;
  commitTransaction(): Promise<void>;
  recoverFromWAL(): Promise<void>;
}

/**
 * Lightweight wrapper that uses AtomicFile for transactional operations.
 * Keeps DB logic out of the AtomicFile orchestration layer.
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
    await this.atomicFile.write(offset, data);
  }

  public async commitTransaction(): Promise<void> {
    await this.atomicFile.commit();
  }

  public async recoverFromWAL(): Promise<void> {
    await this.atomicFile.recover();
  }
}
