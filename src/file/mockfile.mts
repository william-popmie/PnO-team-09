// @author Tijn Gommers
// @author Wout Van Hemelrijck
// @date 2025-11-18

import assert from 'node:assert/strict';
import random from 'random';
import type { File } from './file.mjs';

/**
 * Mock implementation of the File interface.
 * Stores data in-memory for testing purposes and supports various crash simulations.
 *
 * Sector model:
 *   - `sectors`    – committed data, survives a sync.
 *   - `newSectors` – pending writes not yet synced; may be lost on a simulated crash.
 *
 * Invariant: every Buffer stored in `newSectors` has a length equal to `sectorSize`.
 */
export class MockFile implements File {
  /** Sector size for atomic writes. */
  public readonly sectorSize: number;

  /** Current logical file size in bytes. */
  private size: number;

  /** Array of sectors representing committed (synced) data. */
  private sectors: Buffer[] = [];

  /**
   * Map of pending (unsynced) writes for each sector index.
   * Each entry is an ordered list of full-sector buffers; the last one is the most recent.
   * Invariant: every buffer in these arrays has length equal to sectorSize.
   */
  private newSectors: Map<number, Buffer[]> = new Map();

  /** Whether the file is currently open. */
  private openFlag: boolean = false;

  /**
   * Constructs a new MockFile with a given sector size.
   * @param {number} sectorSize - Size of each sector in bytes.
   */
  public constructor(sectorSize: number) {
    this.sectorSize = sectorSize;
    this.size = 0;
  }

  // =====================
  // Guard helpers
  // =====================

  /**
   * Throws if the file is not currently open.
   * @throws {Error} If the file is not open.
   */
  private ensureOpen(): void {
    if (!this.openFlag) throw new Error('File is not open.');
  }

  /**
   * Throws if the file is already open.
   * @throws {Error} If the file is already open.
   */
  private ensureClosed(): void {
    if (this.openFlag) throw new Error('File is already open.');
  }

  // =====================
  // Test helpers
  // =====================

  /**
   * Returns the map of pending (unsynced) writes, for inspection in tests.
   * @returns {Map<number, Buffer[]>} The pending writes per sector index.
   */
  public getNewSectors(): Map<number, Buffer[]> {
    return this.newSectors;
  }

  // =====================
  // File interface
  // =====================

  /**
   * Creates a new (empty) file.
   * Resets all internal state. Throws if the file is already open.
   * @param {boolean} [overwrite=false] - Accepted for interface compatibility; has no effect in-memory.
   * @returns {Promise<void>} Resolves immediately.
   * @throws {Error} If the file is already open.
   */
  public create(overwrite = false): Promise<void> {
    void overwrite;
    this.ensureClosed();
    this.openFlag = true;
    this.size = 0;
    this.sectors = [];
    this.newSectors.clear();
    return Promise.resolve();
  }

  /**
   * Opens the file for reading and writing.
   * @returns {Promise<void>} Resolves immediately.
   * @throws {Error} If the file is already open.
   */
  public open(): Promise<void> {
    this.ensureClosed();
    this.openFlag = true;
    return Promise.resolve();
  }

  /**
   * Closes the file.
   * @returns {Promise<void>} Resolves when the file is closed.
   * @throws {Error} If the file is not open, or if there are unsynced pending writes.
   */
  public close(): Promise<void> {
    this.ensureOpen();
    if (this.newSectors.size !== 0) {
      throw new Error('Cannot close file: there are unsynced pending writes. Call sync() first.');
    }
    this.openFlag = false;
    return Promise.resolve();
  }

  /**
   * Commits all pending writes to the main sectors array.
   * After this call, `newSectors` is empty and all data is in `sectors`.
   * @returns {Promise<void>} Resolves when sync is complete.
   * @throws {Error} If the file is not open.
   */
  public sync(): Promise<void> {
    this.ensureOpen();
    for (const [sectorOrdinal, writes] of this.newSectors.entries()) {
      const last = writes.at(-1);
      if (!last) {
        throw new Error(`Invariant violated: empty writes array for sector ${sectorOrdinal}.`);
      }
      this.sectors[sectorOrdinal] = last;
    }
    this.newSectors.clear();
    return Promise.resolve();
  }

  /**
   * Writes multiple buffers to the file at a specified position.
   * Extends the file via truncate if the write exceeds the current size.
   * @param {Buffer[]} buffers - Buffers to write.
   * @param {number} position - Byte offset in the file to start writing.
   * @returns {Promise<void>} Resolves when the write is complete.
   * @throws {Error} If the file is not open.
   */
  public async writev(buffers: Buffer[], position: number): Promise<void> {
    this.ensureOpen();
    let buffer = Buffer.concat(buffers);
    if (position + buffer.length > this.size) {
      await this.truncate(position + buffer.length);
    }
    let sectorOrdinal = Math.floor(position / this.sectorSize);
    let offsetInSector = position - sectorOrdinal * this.sectorSize;
    while (buffer.length > this.sectorSize - offsetInSector) {
      this.writeSector(offsetInSector, buffer.subarray(0, this.sectorSize - offsetInSector), sectorOrdinal);
      buffer = buffer.subarray(this.sectorSize - offsetInSector);
      sectorOrdinal++;
      offsetInSector = 0;
    }
    if (buffer.length > 0) {
      this.writeSector(offsetInSector, buffer, sectorOrdinal);
    }
  }

  /**
   * Reads data from the file into a buffer at the specified position.
   * @param {Buffer} buffer - Buffer to fill with read data.
   * @param {{position: number}} options - Object containing the byte offset to start reading.
   * @returns {Promise<void>} Resolves when the read is complete.
   * @throws {Error} If the file is not open.
   * @throws {AssertionError} If the read range exceeds file bounds.
   */
  public read(buffer: Buffer, options: { position: number }): Promise<void> {
    this.ensureOpen();
    assert(options.position >= 0, 'Read position must be non-negative.');
    assert(
      options.position + buffer.length <= this.sectors.length * this.sectorSize,
      `Read out of bounds: position ${options.position} + length ${buffer.length} exceeds file size.`,
    );
    let sectorOrdinal = Math.floor(options.position / this.sectorSize);
    let offsetInSector = options.position - sectorOrdinal * this.sectorSize;
    while (buffer.length > this.sectorSize - offsetInSector) {
      const sector = this.readSector(sectorOrdinal);
      sector.copy(buffer, 0, offsetInSector, this.sectorSize);
      buffer = buffer.subarray(this.sectorSize - offsetInSector);
      sectorOrdinal++;
      offsetInSector = 0;
    }
    const sector = this.readSector(sectorOrdinal);
    sector.copy(buffer, 0, offsetInSector, offsetInSector + buffer.length);
    return Promise.resolve();
  }

  /**
   * Truncates (or extends) the file to a given length.
   * New sectors added during extension are zero-filled.
   * Pending writes for removed sectors are discarded.
   * @param {number} length - New file length in bytes.
   * @returns {Promise<void>} Resolves when truncation is complete.
   * @throws {Error} If the file is not open.
   */
  public truncate(length: number): Promise<void> {
    this.ensureOpen();
    const sectorCount = Math.ceil(length / this.sectorSize);
    if (sectorCount < this.sectors.length) {
      this.sectors.length = sectorCount;
      for (const sectorOrdinal of this.newSectors.keys()) {
        if (sectorOrdinal >= sectorCount) this.newSectors.delete(sectorOrdinal);
      }
    } else if (sectorCount > this.sectors.length) {
      for (let i = this.sectors.length; i < sectorCount; i++) {
        this.sectors.push(Buffer.alloc(this.sectorSize));
      }
    }
    this.size = length;
    return Promise.resolve();
  }

  /**
   * Returns the current logical file size.
   * @returns {Promise<{size: number}>} Object containing the file size in bytes.
   * @throws {Error} If the file is not open.
   */
  public stat(): Promise<{ size: number }> {
    this.ensureOpen();
    return Promise.resolve({ size: this.size });
  }

  // =====================
  // Private sector helpers
  // =====================

  /**
   * Returns the most recent data for a given sector.
   * Prefers the latest pending write in `newSectors` over the committed value in `sectors`.
   * @param {number} sectorOrdinal - Index of the sector to read.
   * @returns {Buffer} The sector buffer.
   * @throws {AssertionError} If sectorOrdinal is out of bounds.
   * @throws {Error} If the pending writes array for the sector is unexpectedly empty.
   */
  private readSector(sectorOrdinal: number): Buffer {
    assert(
      sectorOrdinal >= 0 && sectorOrdinal < this.sectors.length,
      `Sector ${sectorOrdinal} is out of bounds (total: ${this.sectors.length}).`,
    );
    const pending = this.newSectors.get(sectorOrdinal);
    if (pending) {
      const last = pending.at(-1);
      if (!last) {
        throw new Error(`Invariant violated: empty writes array for sector ${sectorOrdinal}.`);
      }
      return last;
    }
    return this.sectors[sectorOrdinal];
  }

  /**
   * Writes a buffer into a sector, merging with existing data if the write is partial.
   * All writes — including full-sector writes — are staged in `newSectors` so that
   * crash simulations can selectively drop them.
   * @param {number} offsetInSector - Byte offset within the sector to start writing.
   * @param {Buffer} buffer - Data to write (must fit within the sector from the given offset).
   * @param {number} sectorOrdinal - Index of the target sector.
   * @throws {AssertionError} If the buffer does not fit within the sector at the given offset.
   */
  private writeSector(offsetInSector: number, buffer: Buffer, sectorOrdinal: number): void {
    assert(
      buffer.length <= this.sectorSize - offsetInSector,
      `Write of ${buffer.length} bytes at offset ${offsetInSector} exceeds sector size ${this.sectorSize}.`,
    );

    let fullSector: Buffer;

    if (offsetInSector === 0 && buffer.length === this.sectorSize) {
      // Full-sector overwrite: no need to read existing data.
      fullSector = Buffer.from(buffer);
    } else {
      // Partial write: copy into a snapshot of the current sector data.
      fullSector = Buffer.from(this.readSector(sectorOrdinal));
      buffer.copy(fullSector, offsetInSector);
    }

    // Always stage through newSectors so crash simulations can drop this write.
    if (!this.newSectors.has(sectorOrdinal)) {
      this.newSectors.set(sectorOrdinal, []);
    }
    this.newSectors.get(sectorOrdinal)!.push(fullSector);
  }

  // =====================
  // Crash simulations
  // =====================

  /**
   * Simulates a basic crash: for each sector with pending writes, a random prefix
   * of those writes (possibly none) is committed. The rest are lost.
   */
  public crashBasic(): void {
    for (const [sectorOrdinal, writes] of this.newSectors) {
      const nbWritesToSave = random.uniformInt(0, writes.length)();
      if (nbWritesToSave > 0) {
        this.sectors[sectorOrdinal] = writes[nbWritesToSave - 1];
      }
    }
    this.newSectors.clear();
  }

  /**
   * Simulates a total crash: all pending and committed data is lost.
   * The file is reset to an empty, closed state.
   */
  public crashFullLoss(): void {
    this.newSectors.clear();
    this.sectors = [];
    this.size = 0;
    this.openFlag = false;
  }

  /**
   * Simulates a partial corruption crash: the last pending write for each sector
   * is committed with one random byte flipped. If there are no pending writes,
   * committed sectors are corrupted instead.
   */
  public crashPartialCorruption(): void {
    if (this.newSectors.size > 0) {
      for (const [sectorOrdinal, writes] of this.newSectors) {
        const last = writes.at(-1);
        if (!last) continue;
        const corrupted = Buffer.from(last);
        corrupted[random.uniformInt(0, corrupted.length - 1)()] ^= 0xff;
        this.sectors[sectorOrdinal] = corrupted;
      }
    } else {
      for (let i = 0; i < this.sectors.length; i++) {
        const current = this.sectors[i];
        if (!current) continue;
        const corrupted = Buffer.from(current);
        corrupted[random.uniformInt(0, corrupted.length - 1)()] ^= 0xff;
        this.sectors[i] = corrupted;
      }
    }
    this.newSectors.clear();
  }

  /**
   * Simulates a mixed crash: each sector independently has a 50% chance of losing
   * all its pending writes. For sectors that do save writes, a random prefix is
   * committed and may additionally have one byte corrupted (50% chance).
   */
  public crashMixed(): void {
    for (const [sectorOrdinal, writes] of this.newSectors) {
      if (random.bool()) continue; // 50% chance: all writes for this sector are lost.
      const nbWritesToSave = random.uniformInt(0, writes.length)();
      if (nbWritesToSave > 0) {
        const saved = Buffer.from(writes[nbWritesToSave - 1]);
        if (random.bool()) {
          saved[random.uniformInt(0, saved.length - 1)()] ^= 0xff;
        }
        this.sectors[sectorOrdinal] = saved;
      }
    }
    this.newSectors.clear();
  }
}
