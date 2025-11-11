import assert from 'node:assert/strict';
import { FreeBlockFile } from './freeblockfile.mjs';
import type { AtomicFile } from './freeblockfile.mjs';
import type { File as FileInterface } from './mockfile.mts';
import { MockFile } from './mockfile.mjs';

export const DEFAULT_BLOCK_SIZE = 4096;
export const NEXT_POINTER_SIZE = 4;
export const FREE_LIST_HEAD_OFFSET = 0;
export const HEADER_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + NEXT_POINTER_SIZE;
export const HEADER_CLIENT_AREA_OFFSET = HEADER_LENGTH_OFFSET + NEXT_POINTER_SIZE;

export const LENGTH_PREFIX_SIZE = 8;

export const NO_BLOCK = 0;

export const MIN_BLOCK_SIZE = HEADER_CLIENT_AREA_OFFSET + 16;

class TestAtomicFile {
  private file: FileInterface;

  constructor(file: FileInterface) {
    this.file = file;
  }

  async open(): Promise<void> {
    if (typeof this.file.open === 'function') await this.file.open();
  }
  async close(): Promise<void> {
    if (typeof this.file.close === 'function') await this.file.close();
  }

  async atomicWrite(writes: { position: number; buffer: Buffer }[]): Promise<void> {
    for (const w of writes) {
      await this.file.writev([w.buffer], w.position);
    }
    if (typeof this.file.sync === 'function') {
      await this.file.sync();
    }
  }

  async sync(): Promise<void> {
    if (typeof this.file.sync === 'function') {
      await this.file.sync();
    }
  }
}

async function makeFreeBlockFile(): Promise<{ fb: FreeBlockFile; mf: MockFile; atomic: TestAtomicFile }> {
  const mf = new MockFile(512);
  const atomic = new TestAtomicFile(mf);
  const fb = new FreeBlockFile(mf as unknown as FileInterface, atomic as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
  await fb.open();
  return { fb, mf, atomic };
}

async function testAllocateWriteCommitRead() {
  console.log(`\n--- RUNNING: allocateWriteCommitRead ---`);

  const { fb, mf } = await makeFreeBlockFile();

  const payload = Buffer.from('hello, freeblockfile!');
  const start = await fb.allocateAndWrite(payload);

  await fb.commit();

  const readBack = await fb.readBlob(start);
  assert.strictEqual(readBack.toString(), payload.toString(), 'readBlob must return same payload after commit');

  await fb.close();

  const mf2 = mf;
  const atomic2 = new TestAtomicFile(mf2 as unknown as FileInterface);
  const fb2 = new FreeBlockFile(mf2 as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
  await fb2.open();

  const readBack2 = await fb2.readBlob(start);
  assert.strictEqual(readBack2.toString(), payload.toString(), 'readBlob after reopen must return same payload');

  await fb2.close();

  console.log('PASS: allocateWriteCommitRead');
}

async function testNoCommitLosesData() {
  console.log(`\n--- RUNNING: noCommitLosesData ---`);

  const { fb, mf } = await makeFreeBlockFile();

  const payload = Buffer.from('transient data');
  const start = await fb.allocateAndWrite(payload);

  await fb.close();

  const mf2 = mf;
  const atomic2 = new TestAtomicFile(mf2 as unknown as FileInterface);
  const fb2 = new FreeBlockFile(mf2 as unknown as FileInterface, atomic2 as unknown as AtomicFile, DEFAULT_BLOCK_SIZE);
  await fb2.open();

  const readBack = await fb2.readBlob(start);
  assert.strictEqual(readBack.length, 0, 'uncommitted blob must not be visible after reopen');

  await fb2.close();

  console.log('PASS: noCommitLosesData');
}

async function testFreeAndReuse() {
  console.log(`\n--- RUNNING: freeAndReuse ---`);

  const { fb } = await makeFreeBlockFile();

  const payload = Buffer.from('to be freed');
  const start = await fb.allocateAndWrite(payload);
  await fb.commit();

  const before = await fb.readBlob(start);
  assert.strictEqual(before.toString(), payload.toString(), 'blob readable before free');

  await fb.freeBlob(start);
  await fb.commit();

  const freeHead = await fb.debug_getFreeListHead();
  assert.strictEqual(freeHead, start, 'free list head should point to the recently freed block');

  const newAlloc = await fb.allocateBlocks(1);
  assert.strictEqual(newAlloc, start, 'allocation should reuse freed block');

  console.log('PASS: freeAndReuse');

  await fb.close();
}

async function main() {
  try {
    await testAllocateWriteCommitRead();
    await testNoCommitLosesData();
    await testFreeAndReuse();
    console.log('\nALL TESTS PASSED');
  } catch (err) {
    console.error('\nTEST FAILURE:', (err as Error).message);
    console.error(err);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error('UNEXPECTED ERROR:', err);
  process.exitCode = 2;
});
