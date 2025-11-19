import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FBNodeStorage } from './fb-node-storage.mjs';
import { FreeBlockFile, NO_BLOCK } from '../freeblockfile.mjs';
import { MockFile } from '../mockfile.mjs';

/**
 * Small TestAtomicFile wrapper used by FreeBlockFile in your test harness.
 * Mirrors the structure used in your manual test script.
 */
class TestAtomicFile {
  file: MockFile;
  constructor(file: MockFile) {
    this.file = file;
  }
  async open() {}
  async close() {}
  async atomicWrite(writes: { position: number; buffer: Buffer }[]) {
    for (const w of writes) {
      await this.file.writev([w.buffer], w.position);
    }
    await this.file.sync();
  }
  async sync() {
    await this.file.sync();
  }
}

function makeStorage() {
  const mf = new MockFile(512);
  const atomic = new TestAtomicFile(mf);
  const fb = new FreeBlockFile(mf, atomic, 4096);
  return { mf, atomic, fb };
}

describe('FBNodeStorage', () => {
  let fb: FreeBlockFile;
  let storage: FBNodeStorage<number, string>;
  // let mf: MockFile;

  beforeEach(async () => {
    const setup = makeStorage();
    // const mf = setup.mf;
    fb = setup.fb;
    await fb.open();
    storage = new FBNodeStorage<number, string>(
      (a, b) => a - b,
      () => 8,
      fb,
      64,
    );
  });

  afterEach(async () => {
    try {
      await fb.close();
    } catch {
      // ignore
    }
  });

  it('persists a leaf and loadNode returns the same keys/values', async () => {
    const leaf = await storage.createLeaf();
    const cur = leaf.getCursorBeforeFirst();
    await cur.insert(10, 'v10');
    await cur.insert(5, 'v5');
    await cur.insert(20, 'v20');

    await storage.commitAndReclaim();

    expect(typeof leaf.blockId).toBe('number');
    const persistedId = leaf.blockId!;
    expect(persistedId).toBeGreaterThan(0);

    const raw = await fb.readBlob(persistedId);
    expect(Buffer.isBuffer(raw)).toBeTruthy();
    const parsed = JSON.parse(raw.toString('utf8')) as unknown;
    if (typeof parsed !== 'object' || parsed === null) throw new Error('parsed payload is not an object');
    const p = parsed as Record<string, unknown>;
    expect(p['type']).toBe('leaf');
    expect(Array.isArray(p['keys'])).toBeTruthy();

    const loaded = await storage.loadNode(persistedId);
    expect(loaded.isLeaf).toBe(true);
    if (loaded.isLeaf) {
      expect(loaded.keys).toEqual([5, 10, 20]);
      expect(loaded.values).toEqual(['v5', 'v10', 'v20']);
    }
  });

  it('creates an internal node referencing two leaves and loads it', async () => {
    const leaf1 = await storage.createLeaf();
    const cur1 = leaf1.getCursorBeforeFirst();
    await cur1.insert(10, 'v10');

    const leaf2 = await storage.createLeaf();
    const cur2 = leaf2.getCursorBeforeFirst();
    await cur2.insert(30, 'v30');

    await storage.commitAndReclaim();

    const internal = await storage.allocateInternalNodeStorage([leaf1, leaf2], [30]);
    await storage.commitAndReclaim();

    expect(typeof internal.blockId).toBe('number');
    // Diagnostic check: ensure the persisted internal payload actually contains keys
    const rawInternal = await fb.readBlob(internal.blockId!);
    expect(Buffer.isBuffer(rawInternal)).toBeTruthy();
    const parsedInternal = JSON.parse(rawInternal.toString('utf8')) as unknown;
    if (typeof parsedInternal !== 'object' || parsedInternal === null)
      throw new Error('parsed internal payload is not an object');
    const pi = parsedInternal as Record<string, unknown>;
    expect(pi['type']).toBe('internal');
    expect(Array.isArray(pi['keys'])).toBeTruthy();
    expect((pi['keys'] as unknown[]).length).toBeGreaterThan(0);
    const loaded = await storage.loadNode(internal.blockId!);
    expect(loaded.isLeaf).toBe(false);
    if (!loaded.isLeaf) {
      expect(loaded.keys).toEqual([30]);
      expect(Array.isArray(loaded.childBlockIds)).toBeTruthy();
      expect(loaded.childBlockIds.length).toBe(2);
      const child0 = await storage.loadNode(loaded.childBlockIds[0]);
      const child1 = await storage.loadNode(loaded.childBlockIds[1]);
      expect(child0.isLeaf).toBe(true);
      expect(child1.isLeaf).toBe(true);
    }
  });

  it('enqueues old leaf blocks and reclaims on commitAndReclaim; freed id reused', async () => {
    const leaf = await storage.createLeaf();
    const cur = leaf.getCursorBeforeFirst();
    await cur.insert(10, 'v10');
    await cur.insert(5, 'v5');
    await storage.commitAndReclaim();

    const firstId = leaf.blockId!;
    expect(typeof firstId).toBe('number');
    expect(firstId).toBeGreaterThan(0);

    await cur.insert(15, 'v15');
    const newId = leaf.blockId!;
    expect(newId).not.toBe(firstId);

    const freeHeadBefore = await fb.debug_getFreeListHead();
    expect(freeHeadBefore).toBe(NO_BLOCK);

    await storage.commitAndReclaim();

    const freeHeadAfter = await fb.debug_getFreeListHead();
    expect(freeHeadAfter).toBe(firstId);

    const alloc = await fb.allocateBlocks(1);

    let allocatedId: number;
    if (Array.isArray(alloc)) {
      allocatedId = alloc[0];
    } else if (typeof alloc === 'number') {
      allocatedId = alloc;
    } else {
      throw new Error(`unexpected allocateBlocks return value: ${String(alloc)}`);
    }

    expect(allocatedId).toBe(firstId);
  });
  it('returns maxKeySize', () => {
    const maxKeySize = storage.getMaxKeySize();
    expect(maxKeySize).toBe(64);
  });

  it('leaf cursor getCursorBeforeKey and iteration works', async () => {
    const leaf = await storage.createLeaf();
    const cur = leaf.getCursorBeforeFirst();
    await cur.insert(10, 'v10');
    await cur.insert(5, 'v5');
    await cur.insert(20, 'v20');

    expect(leaf.keys).toEqual([5, 10, 20]);

    const { cursor, isAtKey } = leaf.getCursorBeforeKey(10);
    expect(isAtKey).toBe(true);
    cursor.moveNext();
    const kv = cursor.getKeyValuePairAfter();
    expect(kv.key).toBe(20);
    expect(kv.value).toBe('v20');
  });

  it('links leaves via nextBlockId and getNextLeaf returns persisted sibling', async () => {
    const leaf1 = await storage.createLeaf();
    await leaf1.getCursorBeforeFirst().insert(1, 'a');
    await storage.commitAndReclaim();

    const leaf2 = await storage.createLeaf();
    await leaf2.getCursorBeforeFirst().insert(2, 'b');
    await storage.commitAndReclaim();

    leaf1.nextBlockId = leaf2.blockId;
    await storage.persistLeaf(leaf1);
    await storage.commitAndReclaim();

    const loaded1 = await storage.loadNode(leaf1.blockId!);
    expect(loaded1.isLeaf).toBe(true);
    if (!loaded1.isLeaf) throw new Error('expected leaf node');
    const next = await loaded1.getNextLeaf();
    expect(next).not.toBeNull();
    if (next) {
      expect(next.keys).toEqual(leaf2.keys);
      expect(next.values).toEqual(leaf2.values);
    }
  });

  it('persists child when creating internal node', async () => {
    const ephemeral = await storage.createLeaf();
    expect(ephemeral.blockId).toBeUndefined();

    const internal = await storage.allocateInternalNodeStorage([ephemeral], []);
    await storage.commitAndReclaim();

    expect(typeof internal.blockId).toBe('number');
    expect(internal.blockId).toBeGreaterThan(0);

    expect(typeof ephemeral.blockId).toBe('number');
    expect(internal.childBlockIds.length).toBe(1);
    expect(internal.childBlockIds[0]).toBe(ephemeral.blockId);
  });

  it('leaf cursor removeKeyValuePairAfter updates keys/values', async () => {
    const leaf = await storage.createLeaf();
    const c = leaf.getCursorBeforeFirst();
    await c.insert(2, 'b');
    await c.insert(1, 'a');
    await c.insert(3, 'c');

    expect(leaf.keys).toEqual([1, 2, 3]);

    const cursor = leaf.getCursorBeforeFirst();
    await cursor.removeKeyValuePairAfter();
    expect(leaf.keys).toEqual([2, 3]);

    // console.log(storage.loadNode(leaf.blockId!));
  });
});
