// Benchmark for FBNodeStorage to test scalability improvements with overwriteBlock
// @author Tijn Gommers
// @date 2026-02-13

import { performance } from 'perf_hooks';
import { BPlusTree } from '../b-plus-tree.mjs';
import { FBNodeStorage, FBLeafNode, FBInternalNode } from '../node-storage/fb-node-storage.mjs';
import { FreeBlockFile } from '../freeblockfile.mjs';
import { MockFile } from '../file/mockfile.mjs';

/**
 * Simple test wrapper for AtomicFile interface
 */
class TestAtomicFile {
  constructor(private file: MockFile) {}

  async open(): Promise<void> {
    await this.file.open();
  }

  async close(): Promise<void> {
    await this.file.close();
  }

  async atomicWrite(writes: { position: number; buffer: Buffer }[]): Promise<void> {
    for (const { position, buffer } of writes) {
      await this.file.writev([buffer], position);
    }
  }

  async sync(): Promise<void> {
    // No-op for MockFile
  }
}

/**
 * Setup storage with proper initialization
 */
async function setupStorage() {
  const mf = new MockFile(512); // sectorSize = 512 bytes
  await mf.open();
  const atomic = new TestAtomicFile(mf);
  const fb = new FreeBlockFile(mf, atomic, 4096); // blockSize = 4096 bytes
  await fb.open();

  const compareKeys = (a: number, b: number) => (a < b ? -1 : a > b ? 1 : 0);
  const keySize = (_k: number) => 8;
  const storage = new FBNodeStorage<number, number>(compareKeys, keySize, fb, 32); // maxKeySize = 32

  return { storage, fb, mf };
}

/**
 * Cleanup storage
 */
async function cleanupStorage(fb: FreeBlockFile, mf: MockFile) {
  await fb.close();
  await mf.close();
}

/**
 * Time inserts with periodic commits
 */
async function timeInserts(
  tree: BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>,
  storage: FBNodeStorage<number, number>,
  keys: number[],
  commitEvery = 100,
) {
  const t0 = performance.now();
  let commitCount = 0;

  for (let i = 0; i < keys.length; i++) {
    await tree.insert(keys[i], keys[i]);

    if ((i + 1) % commitEvery === 0) {
      await storage.commitAndReclaim();
      commitCount++;
    }
  }

  // Final commit
  await storage.commitAndReclaim();
  commitCount++;

  const t1 = performance.now();
  return { totalMs: t1 - t0, commitCount };
}

/**
 * Time searches
 */
async function timeSearches(
  tree: BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>,
  keys: number[],
) {
  const t0 = performance.now();
  for (const key of keys) {
    const result = await tree.search(key);
    if (result === null) {
      throw new Error(`Key ${key} not found!`);
    }
  }
  const t1 = performance.now();
  return t1 - t0;
}

/**
 * Time deletes
 */
async function timeDeletes(
  tree: BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>,
  storage: FBNodeStorage<number, number>,
  keys: number[],
  commitEvery = 100,
) {
  const t0 = performance.now();
  let commitCount = 0;

  for (let i = 0; i < keys.length; i++) {
    await tree.delete(keys[i]);

    if ((i + 1) % commitEvery === 0) {
      await storage.commitAndReclaim();
      commitCount++;
    }
  }

  // Final commit
  await storage.commitAndReclaim();
  commitCount++;

  const t1 = performance.now();
  return { totalMs: t1 - t0, commitCount };
}

/**
 * Run a single benchmark iteration
 */
async function runBenchmark(n: number, commitEvery = 50) {
  console.log(`\nTesting with ${n} operations (commit every ${commitEvery})...`);

  const { storage, fb, mf } = await setupStorage();
  const tree = new BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>(
    storage,
    32, // order
  );
  await tree.init();

  // Generate sequential keys
  const keys = Array.from({ length: n }, (_, i) => i);

  // 1. Time inserts with commits
  console.log('  Phase 1: Inserting...');
  const insertResult = await timeInserts(tree, storage, keys, commitEvery);
  const insertMs = insertResult.totalMs;
  const insertPerOp = (insertMs / n) * 1000; // microseconds per op
  console.log(
    `     Done: ${insertMs.toFixed(2)}ms total, ${insertPerOp.toFixed(3)}us/op, ${insertResult.commitCount} commits`,
  );

  // 2. Time searches
  console.log('  Phase 2: Searching...');
  const searchMs = await timeSearches(tree, keys);
  const searchPerOp = (searchMs / n) * 1000;
  console.log(`     Done: ${searchMs.toFixed(2)}ms total, ${searchPerOp.toFixed(3)}us/op`);

  // 3. Update test: re-insert same keys (should trigger overwriteBlock!)
  console.log('  Phase 3: Updating (re-insert same keys)...');
  const updateResult = await timeInserts(tree, storage, keys, commitEvery);
  const updateMs = updateResult.totalMs;
  const updatePerOp = (updateMs / n) * 1000;
  console.log(
    `     Done: ${updateMs.toFixed(2)}ms total, ${updatePerOp.toFixed(3)}us/op, ${updateResult.commitCount} commits`,
  );

  // 4. Time deletes
  console.log('  Phase 4: Deleting...');
  const deleteResult = await timeDeletes(tree, storage, keys, commitEvery);
  const deleteMs = deleteResult.totalMs;
  const deletePerOp = (deleteMs / n) * 1000;
  console.log(
    `     Done: ${deleteMs.toFixed(2)}ms total, ${deletePerOp.toFixed(3)}us/op, ${deleteResult.commitCount} commits`,
  );

  // Get stats
  const freeListHead = await fb.debug_getFreeListHead();
  console.log(`  Stats: free list head = ${freeListHead}`);

  await cleanupStorage(fb, mf);

  return {
    n,
    insertMs,
    searchMs,
    updateMs,
    deleteMs,
    insertPerOp,
    searchPerOp,
    updatePerOp,
    deletePerOp,
    insertCommits: insertResult.commitCount,
    updateCommits: updateResult.commitCount,
    deleteCommits: deleteResult.commitCount,
  };
}

/**
 * Main benchmark runner
 */
async function compareScalability() {
  console.log('FBNodeStorage Scalability Benchmark');
  console.log('Testing with in-place updates (overwriteBlock implementation)');
  console.log('='.repeat(70));

  const sizes = [100, 500, 1000, 2000, 5000];
  const results = [];

  for (const size of sizes) {
    try {
      const result = await runBenchmark(size, 50);
      results.push(result);
    } catch (err) {
      console.error(`\nError testing size ${size}:`, err);
      if (err instanceof Error) {
        console.error('Stack:', err.stack);
      }
      break;
    }
  }

  console.log('\n' + '='.repeat(70));
  console.log('Summary Results:');
  console.log('='.repeat(70));
  console.log('n\tinsert(us)\tupdate(us)\tsearch(us)\tdelete(us)\tcommits');
  console.log('-'.repeat(70));
  for (const r of results) {
    console.log(
      `${r.n}\t${r.insertPerOp.toFixed(3)}\t\t${r.updatePerOp.toFixed(3)}\t\t${r.searchPerOp.toFixed(3)}\t\t${r.deletePerOp.toFixed(
        3,
      )}\t\t${r.insertCommits}`,
    );
  }

  console.log('\nAnalysis:');
  if (results.length >= 2) {
    const first = results[0];
    const last = results[results.length - 1];
    const scaleFactor = last.n / first.n;
    const insertScale = last.insertPerOp / first.insertPerOp;
    const updateScale = last.updatePerOp / first.updatePerOp;

    console.log(`   Data size increased ${scaleFactor}x (from ${first.n} to ${last.n})`);
    console.log(`   Insert time/op increased ${insertScale.toFixed(2)}x`);
    console.log(`   Update time/op increased ${updateScale.toFixed(2)}x`);

    if (updateScale < scaleFactor * 0.5) {
      console.log('   GOOD: Updates scale sub-linearly (in-place working!)');
    } else if (updateScale < scaleFactor * 1.5) {
      console.log('   OK: Updates scale roughly linearly');
    } else {
      console.log('   BAD: Updates scale super-linearly (possible problem)');
    }
  }

  console.log('\nBenchmark complete!');
}

// Run the benchmark
await compareScalability();
