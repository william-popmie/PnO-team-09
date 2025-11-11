// @author Mathias Bouhon Keulen
// @date 2025-11-11

import { type File } from './mockfile.mjs';

export interface AtomicFile {
  open(): Promise<void>;
  close(): Promise<void>;
  atomicWrite(writes: { position: number; buffer: Buffer }[]): Promise<void>;
  sync?(): Promise<void>;
}

export const DEFAULT_BLOCK_SIZE = 4096;

export const NEXT_POINTER_SIZE = 4;

export const FREE_LIST_HEAD_OFFSET = 0;
export const HEADER_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + NEXT_POINTER_SIZE;
export const HEADER_CLIENT_AREA_OFFSET = HEADER_LENGTH_OFFSET + NEXT_POINTER_SIZE;

export const LENGTH_PREFIX_SIZE = 8;

export const NO_BLOCK = 0;

export const MIN_BLOCK_SIZE = HEADER_CLIENT_AREA_OFFSET + 16;

export class FreeBlockFile {
  readonly blockSize: number;
  readonly payloadSize: number;

  private file: File;
  private atomicFile: AtomicFile;

  private stagedWrites: Map<number, Buffer> = new Map();

  private cachedFreeListHead: number = NO_BLOCK;
  private cachedHeaderBuf: Buffer = Buffer.alloc(0);

  private opened = false;

  // Ensure the file has been opened before performing public operations.
  private ensureOpened(): void {
    if (!this.opened) throw new Error('FreeBlockFile is not open');
  }

  constructor(file: File, atomicFile: AtomicFile, blockSize = DEFAULT_BLOCK_SIZE) {
    if (blockSize < MIN_BLOCK_SIZE) {
      throw new Error(`blockSize too small; must be >= ${MIN_BLOCK_SIZE}`);
    }
    this.file = file;
    this.atomicFile = atomicFile;
    this.blockSize = blockSize;
    this.payloadSize = blockSize - NEXT_POINTER_SIZE;
  }

  async open(): Promise<void> {
    await this.file.open();
    if (this.atomicFile.open) await this.atomicFile.open();

    const st = await this.file.stat();
    if (st.size === 0) {
      this.cachedFreeListHead = NO_BLOCK;
      this.cachedHeaderBuf = Buffer.alloc(0);
      this.stageHeaderBlock();
    } else {
      const headerBlock = Buffer.alloc(this.blockSize);
      await this.file.read(headerBlock, { position: 0 });
      this.cachedFreeListHead = headerBlock.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      const headerLen = headerBlock.readUInt32LE(HEADER_LENGTH_OFFSET);
      if (headerLen > 0) {
        this.cachedHeaderBuf = Buffer.from(
          headerBlock.slice(HEADER_CLIENT_AREA_OFFSET, HEADER_CLIENT_AREA_OFFSET + headerLen),
        );
      } else {
        this.cachedHeaderBuf = Buffer.alloc(0);
      }
    }

    this.opened = true;
  }

  async close(): Promise<void> {
    if (this.atomicFile.close) await this.atomicFile.close();
    await this.file.close();
    this.opened = false;
  }

  async readHeader(): Promise<Buffer> {
    return Promise.resolve(Buffer.from(this.cachedHeaderBuf));
  }

  async writeHeader(buf: Buffer): Promise<void> {
    await Promise.resolve();
    const maxClientHeader = this.blockSize - HEADER_CLIENT_AREA_OFFSET;
    if (buf.length > maxClientHeader) {
      throw new Error(`header too large for block 0; max ${maxClientHeader} bytes`);
    }
    this.cachedHeaderBuf = Buffer.from(buf);
    this.stageHeaderBlock();
  }

  private stageHeaderBlock(): void {
    const b = Buffer.alloc(this.blockSize, 0);
    b.writeUInt32LE(this.cachedFreeListHead >>> 0, FREE_LIST_HEAD_OFFSET);
    b.writeUInt32LE(this.cachedHeaderBuf.length >>> 0, HEADER_LENGTH_OFFSET);
    if (this.cachedHeaderBuf.length > 0) {
      this.cachedHeaderBuf.copy(b, HEADER_CLIENT_AREA_OFFSET, 0, this.cachedHeaderBuf.length);
    }
    this.stagedWrites.set(0, b);
  }

  async allocateBlocks(count: number): Promise<number> {
    this.ensureOpened();
    if (count <= 0) throw new Error('count must be positive');

    const allocated: number[] = [];

    while (allocated.length < count && this.cachedFreeListHead !== NO_BLOCK) {
      const head = this.cachedFreeListHead;
      const headBuf = await this.readRawBlock(head);
      const next = headBuf.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      this.cachedFreeListHead = next;
      allocated.push(head);
    }

    if (allocated.length < count) {
      const st = await this.file.stat();
      const currentBlocks = Math.floor(st.size / this.blockSize);
      const need = count - allocated.length;

      const base = currentBlocks === 0 ? 1 : currentBlocks;
      for (let i = 0; i < need; i++) {
        const newId = base + i;
        const newBlock = Buffer.alloc(this.blockSize, 0);
        newBlock.writeUInt32LE(NO_BLOCK >>> 0, FREE_LIST_HEAD_OFFSET);
        this.stagedWrites.set(newId, newBlock);
        allocated.push(newId);
      }
    }

    this.stageHeaderBlock();

    return allocated[0];
  }

  async allocateAndWrite(data: Buffer): Promise<number> {
    this.ensureOpened();
    const lengthPrefix = Buffer.alloc(LENGTH_PREFIX_SIZE);
    lengthPrefix.writeBigUInt64LE(BigInt(data.length), 0);
    const full = Buffer.concat([lengthPrefix, data]);
    const needed = Math.ceil(full.length / this.payloadSize) || 1;
    const firstBlock = await this.allocateBlocks(needed);
    const blocks = await this.collectAllocatedChain(firstBlock, needed);

    for (let i = 0; i < blocks.length; i++) {
      const blockId = blocks[i];
      const next = i + 1 < blocks.length ? blocks[i + 1] : NO_BLOCK;
      const out = Buffer.alloc(this.blockSize, 0);
      out.writeUInt32LE(next >>> 0, FREE_LIST_HEAD_OFFSET);
      const start = i * this.payloadSize;
      const end = Math.min(start + this.payloadSize, full.length);
      full.copy(out, NEXT_POINTER_SIZE, start, end);
      this.stagedWrites.set(blockId, out);
    }

    return firstBlock;
  }

  async freeBlob(startBlockId: number): Promise<void> {
    this.ensureOpened();
    if (startBlockId === NO_BLOCK) return;
    let current = startBlockId;
    while (current !== NO_BLOCK) {
      const buf = await this.readRawBlock(current);
      const nextInChain = buf.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      const freeBuf = Buffer.alloc(this.blockSize, 0);
      freeBuf.writeUInt32LE(this.cachedFreeListHead >>> 0, FREE_LIST_HEAD_OFFSET);
      this.stagedWrites.set(current, freeBuf);
      this.cachedFreeListHead = current;
      current = nextInChain;
    }
    this.stageHeaderBlock();
  }

  async readBlob(startBlockId: number): Promise<Buffer> {
    this.ensureOpened();
    if (startBlockId === NO_BLOCK) return Buffer.alloc(0);

    const parts: Buffer[] = [];
    let current = startBlockId;
    while (current !== NO_BLOCK) {
      const blockBuf = await this.readRawBlock(current);
      const next = blockBuf.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      const payload = blockBuf.slice(NEXT_POINTER_SIZE);
      parts.push(Buffer.from(payload));
      current = next;
    }
    const full = Buffer.concat(parts);
    if (full.length < LENGTH_PREFIX_SIZE) return Buffer.alloc(0);
    const len = Number(full.readBigUInt64LE(0));
    const data = full.slice(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + len);
    return Buffer.from(data);
  }

  private async readRawBlock(blockId: number): Promise<Buffer> {
    if (this.stagedWrites.has(blockId)) {
      return Buffer.from(this.stagedWrites.get(blockId)!);
    }
    const pos = blockId * this.blockSize;
    const st = await this.file.stat();
    if (pos >= st.size) {
      const z = Buffer.alloc(this.blockSize, 0);
      z.writeUInt32LE(NO_BLOCK >>> 0, FREE_LIST_HEAD_OFFSET);
      return z;
    }
    const buf = Buffer.alloc(this.blockSize);
    await this.file.read(buf, { position: pos });
    return buf;
  }

  private async collectAllocatedChain(firstBlock: number, count: number): Promise<number[]> {
    const ids: number[] = [firstBlock];

    let optimistic = true;
    for (let i = 1; i < count; i++) {
      const candidate = firstBlock + i;
      if (!this.stagedWrites.has(candidate)) {
        optimistic = false;
        break;
      }
    }
    if (optimistic) {
      for (let i = 1; i < count; i++) ids.push(firstBlock + i);
      return ids;
    }

    while (ids.length < count) {
      const prev = ids[ids.length - 1];
      const prevBuf = await this.readRawBlock(prev);
      const next = prevBuf.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      if (next === NO_BLOCK) {
        throw new Error('unexpected end of block chain while collecting allocated blocks');
      }
      ids.push(next);
    }
    return ids;
  }

  async commit(): Promise<void> {
    this.ensureOpened();
    if (this.stagedWrites.size === 0) return;

    if (!this.stagedWrites.has(0)) this.stageHeaderBlock();

    const writes: { position: number; buffer: Buffer }[] = [];
    let maxBlockId = 0;
    for (const [blockId, buf] of this.stagedWrites) {
      writes.push({ position: blockId * this.blockSize, buffer: Buffer.from(buf) });
      if (blockId > maxBlockId) maxBlockId = blockId;
    }

    const desiredSize = (maxBlockId + 1) * this.blockSize;
    const st = await this.file.stat();
    if (st.size < desiredSize) {
      await this.file.truncate(desiredSize);
    }

    await this.atomicFile.atomicWrite(writes);

    if (typeof this.file.sync === 'function') await this.file.sync();

    this.stagedWrites.clear();

    const headerBlock = Buffer.alloc(this.blockSize);
    await this.file.read(headerBlock, { position: 0 });
    this.cachedFreeListHead = headerBlock.readUInt32LE(FREE_LIST_HEAD_OFFSET);
    const headerLen = headerBlock.readUInt32LE(HEADER_LENGTH_OFFSET);
    if (headerLen > 0) {
      this.cachedHeaderBuf = Buffer.from(
        headerBlock.slice(HEADER_CLIENT_AREA_OFFSET, HEADER_CLIENT_AREA_OFFSET + headerLen),
      );
    } else {
      this.cachedHeaderBuf = Buffer.alloc(0);
    }
  }

  async stageRawBlock(blockId: number, buf: Buffer): Promise<void> {
    this.ensureOpened();
    await Promise.resolve();
    if (buf.length !== this.blockSize) throw new Error('raw block must have blockSize length');
    this.stagedWrites.set(blockId, Buffer.from(buf));
    if (blockId === 0) {
      this.cachedFreeListHead = buf.readUInt32LE(FREE_LIST_HEAD_OFFSET);
      const headerLen = buf.readUInt32LE(HEADER_LENGTH_OFFSET);
      this.cachedHeaderBuf =
        headerLen > 0
          ? Buffer.from(buf.slice(HEADER_CLIENT_AREA_OFFSET, HEADER_CLIENT_AREA_OFFSET + headerLen))
          : Buffer.alloc(0);
      this.stageHeaderBlock();
    }
  }

  async debug_getFreeListHead(): Promise<number> {
    return Promise.resolve(this.cachedFreeListHead);
  }
}
