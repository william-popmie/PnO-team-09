import * as fsp from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { setTimeout as sleep } from 'timers/promises';
import { BPlusTree } from '../b-plus-tree.mjs';
import { FBNodeStorage, FBLeafNode, FBInternalNode } from '../node-storage/fb-node-storage.mjs';
import { FreeBlockFile } from '../freeblockfile.mjs';
import { MockFile } from '../file/mockfile.mjs';
import { RealFile, type File as DbFile } from '../file/file.mjs';
import { AtomicFileImpl } from '../atomic-operations/atomic-file.mjs';
import { WALManagerImpl } from '../atomic-operations/wal-manager.mjs';

function xorshift32(seed: number) {
  let s = seed >>> 0;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return s >>> 0;
  };
}

function shuffle<T>(arr: T[], seed = 42) {
  const rnd = xorshift32(seed);
  for (let i = arr.length - 1; i > 0; i--) {
    const j = rnd() % (i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}

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

type FileBackend = 'real' | 'mock';

function nowNs(): bigint {
  return process.hrtime.bigint();
}

function nsToMs(ns: bigint): number {
  return Number(ns) / 1_000_000;
}

async function makeTree(order = 32, backend: FileBackend = 'real') {
  let dbFile: DbFile;
  let walFile: DbFile;
  let tmpDbPath: string | null = null;
  let tmpWalPath: string | null = null;

  if (backend === 'mock') {
    dbFile = new MockFile(512);
    walFile = new MockFile(512);
    await dbFile.open();
    await walFile.open();
  } else {
    tmpDbPath = join(tmpdir(), `fb-bench-${process.pid}-${randomUUID()}.db`);
    tmpWalPath = `${tmpDbPath}.wal`;
    dbFile = new RealFile(tmpDbPath);
    walFile = new RealFile(tmpWalPath);
    await dbFile.create();
    await dbFile.close();
    await walFile.create();
    await walFile.close();
  }

  const atomic =
    backend === 'mock'
      ? new TestAtomicFile(dbFile as MockFile)
      : new AtomicFileImpl(dbFile, new WALManagerImpl(walFile, dbFile));
  const fb = new FreeBlockFile(dbFile, atomic, 4096);
  await fb.open();

  const compareKeys = (a: number, b: number) => (a < b ? -1 : a > b ? 1 : 0);
  const keySize = (_k: number) => 8;
  const storage = new FBNodeStorage<number, number>(compareKeys, keySize, fb, 32);
  const tree = new BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>(
    storage,
    order,
  );

  const dispose = async () => {
    await fb.close();
    if (tmpDbPath) await fsp.unlink(tmpDbPath).catch(() => {});
    if (tmpWalPath) await fsp.unlink(tmpWalPath).catch(() => {});
  };

  return { tree, storage, dispose };
}

async function timePrefill(
  tree: BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>,
  storage: FBNodeStorage<number, number>,
  keys: number[],
  commitEvery: number,
) {
  const t0 = nowNs();
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    await tree.insert(key, key);
    if (commitEvery > 0 && (i + 1) % commitEvery === 0) {
      await storage.commitAndReclaim();
    }
  }
  if (commitEvery > 0) {
    await storage.commitAndReclaim();
  }
  const t1 = nowNs();
  return nsToMs(t1 - t0);
}

async function timeInsertSearchDelete(
  tree: BPlusTree<number, number, FBLeafNode<number, number>, FBInternalNode<number, number>>,
  storage: FBNodeStorage<number, number>,
  initialKeys: number[],
  opCount: number,
  commitEvery: number,
) {
  const activeKeys = initialKeys.slice();
  const rand = xorshift32(987654321);
  let maxKey = Number.NEGATIVE_INFINITY;
  for (const key of initialKeys) {
    if (key > maxKey) maxKey = key;
  }
  let nextKey = Number.isFinite(maxKey) ? maxKey + 1 : 0;

  let insertMs = 0;
  let searchMs = 0;
  let deleteMs = 0;

  for (let i = 0; i < opCount; i++) {
    const insertedKey = nextKey++;
    const insertT0 = nowNs();
    await tree.insert(insertedKey, insertedKey);
    const insertT1 = nowNs();
    insertMs += nsToMs(insertT1 - insertT0);
    activeKeys.push(insertedKey);

    const searchIndex = rand() % activeKeys.length;
    const searchKey = activeKeys[searchIndex];
    const searchT0 = nowNs();
    const found = await tree.search(searchKey);
    const searchT1 = nowNs();
    searchMs += nsToMs(searchT1 - searchT0);
    if (found === null) {
      throw new Error(`Searched key not found during benchmark: ${searchKey}`);
    }

    if (activeKeys.length === 0) {
      throw new Error('No key available to delete during insert/delete benchmark pair');
    }
    const deleteIndex = rand() % activeKeys.length;
    const keyToDelete = activeKeys[deleteIndex];

    const deleteT0 = nowNs();
    await tree.delete(keyToDelete);
    const deleteT1 = nowNs();
    deleteMs += nsToMs(deleteT1 - deleteT0);

    const lastIndex = activeKeys.length - 1;
    const lastKey = activeKeys[lastIndex];
    if (deleteIndex !== lastIndex) {
      activeKeys[deleteIndex] = lastKey;
    }
    activeKeys.pop();

    if (commitEvery > 0 && (i + 1) % commitEvery === 0) {
      await storage.commitAndReclaim();
    }
  }

  if (commitEvery > 0) {
    await storage.commitAndReclaim();
  }

  return {
    insertMs,
    searchMs,
    deleteMs,
  };
}

function printRowFromUs(n: number, insertUs: number, searchUs: number, deleteUs: number) {
  const log2n = Math.log2(n);
  console.log(
    `${n}\tlog2(n)=${log2n.toFixed(2)}\tinsert_us/op=${insertUs.toFixed(3)}\tsearch_us/op=${searchUs.toFixed(
      3,
    )}\tdelete_us/op=${deleteUs.toFixed(3)}`,
  );
}

async function runOnce(
  n: number,
  opts: { mode: 'random' | 'sequential'; order?: number; commitEvery?: number; backend?: FileBackend },
) {
  const { tree, storage, dispose } = await makeTree(opts.order ?? 32, opts.backend ?? 'real');
  const commitEvery = opts.commitEvery ?? 1000;

  try {
    await tree.init();

    const keys = Array.from({ length: n }, (_, i) => i);
    if (opts.mode !== 'sequential') {
      shuffle(keys, 12345);
    }

    await timePrefill(tree, storage, keys, commitEvery);
    const opCount = n;
    const { insertMs, searchMs, deleteMs } = await timeInsertSearchDelete(tree, storage, keys, opCount, commitEvery);

    await storage.commitAndReclaim();
    return { n, insertMs, searchMs, deleteMs, opCount };
  } finally {
    await dispose();
  }
}

function mean(values: number[]): number {
  if (values.length === 0) return NaN;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]): number {
  if (values.length === 0) return NaN;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function stdDev(values: number[]): number {
  if (values.length <= 1) return 0;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return NaN;
  const idx = (sortedValues.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  const w = idx - lo;
  return sortedValues[lo] * (1 - w) + sortedValues[hi] * w;
}

function iqrUpperFilter(values: number[]): { kept: number[]; removed: number } {
  if (values.length < 4) return { kept: values.slice(), removed: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const q1 = percentile(sorted, 0.25);
  const q3 = percentile(sorted, 0.75);
  const iqr = q3 - q1;
  const upperFence = q3 + 1.5 * iqr;
  const kept = values.filter((value) => value <= upperFence);
  return { kept, removed: values.length - kept.length };
}

async function runSizes(
  sizes: number[],
  mode: 'random' | 'sequential',
  opts?: {
    totalRuns?: number;
    discardRuns?: number;
    summary?: 'median' | 'average';
    commitEvery?: number;
    coolDownMs?: number;
    backend?: FileBackend;
  },
) {
  const totalRuns = opts?.totalRuns ?? 9;
  const discardRuns = opts?.discardRuns ?? 2;
  const summary = opts?.summary ?? 'median';
  const coolDownMs = opts?.coolDownMs ?? 200;

  if (discardRuns >= totalRuns) {
    throw new Error(`discardRuns (${discardRuns}) must be smaller than totalRuns (${totalRuns})`);
  }

  const summarize = summary === 'median' ? median : mean;

  console.log(
    `Using ${summary} of ${totalRuns - discardRuns} runs per size (discarding first ${discardRuns} warmup run(s))`,
  );
  console.log('n\tlog2(n)\tinsert_us/op\tsearch_us/op\tdelete_us/op');

  for (const n of sizes) {
    try {
      const insertArr: number[] = [];
      const searchArr: number[] = [];
      const deleteArr: number[] = [];
      const insertUsAll: number[] = [];
      const searchUsAll: number[] = [];
      const deleteUsAll: number[] = [];

      for (let r = 0; r < totalRuns; r++) {
        if (typeof global.gc === 'function') {
          global.gc();
        }

        const res = await runOnce(n, {
          mode,
          order: 32,
          commitEvery: opts?.commitEvery ?? 1000,
          backend: opts?.backend ?? 'real',
        });

        const insertUs = (res.insertMs / res.opCount) * 1000;
        const searchUs = (res.searchMs / res.opCount) * 1000;
        const deleteUs = (res.deleteMs / res.opCount) * 1000;
        insertUsAll.push(insertUs);
        searchUsAll.push(searchUs);
        deleteUsAll.push(deleteUs);

        if (r < discardRuns) continue;

        insertArr.push(insertUs);
        searchArr.push(searchUs);
        deleteArr.push(deleteUs);

        if (typeof global.gc === 'function') {
          global.gc();
        }
        await sleep(coolDownMs);
      }

      const fmtRuns = (values: number[]) =>
        values.map((value, idx) => `${idx + 1}${idx < discardRuns ? '*' : ''}:${value.toFixed(3)}`).join(' | ');

      console.log(`  runs insert_us/op: ${fmtRuns(insertUsAll)}`);
      console.log(`  runs search_us/op: ${fmtRuns(searchUsAll)}`);
      console.log(`  runs delete_us/op: ${fmtRuns(deleteUsAll)}`);

      const keptInsertUs = insertUsAll.slice(discardRuns);
      const keptSearchUs = searchUsAll.slice(discardRuns);
      const keptDeleteUs = deleteUsAll.slice(discardRuns);
      const filteredInsert = iqrUpperFilter(keptInsertUs);
      const filteredSearch = iqrUpperFilter(keptSearchUs);
      const filteredDelete = iqrUpperFilter(keptDeleteUs);
      const cv = (values: number[]) => {
        const avg = mean(values);
        return avg === 0 ? 0 : (stdDev(values) / avg) * 100;
      };

      console.log(
        `  spread kept-runs: insert[min=${Math.min(...keptInsertUs).toFixed(3)}, max=${Math.max(
          ...keptInsertUs,
        ).toFixed(3)}, std=${stdDev(keptInsertUs).toFixed(3)}, cv=${cv(keptInsertUs).toFixed(2)}%] ` +
          `search[min=${Math.min(...keptSearchUs).toFixed(3)}, max=${Math.max(...keptSearchUs).toFixed(3)}, std=${stdDev(
            keptSearchUs,
          ).toFixed(3)}, cv=${cv(keptSearchUs).toFixed(2)}%] ` +
          `delete[min=${Math.min(...keptDeleteUs).toFixed(3)}, max=${Math.max(...keptDeleteUs).toFixed(3)}, std=${stdDev(
            keptDeleteUs,
          ).toFixed(3)}, cv=${cv(keptDeleteUs).toFixed(2)}%]`,
      );

      console.log(
        `  iqr-filter removed: insert=${filteredInsert.removed}, search=${filteredSearch.removed}, delete=${filteredDelete.removed}`,
      );

      printRowFromUs(
        n,
        summarize(filteredInsert.kept.length > 0 ? filteredInsert.kept : keptInsertUs),
        summarize(filteredSearch.kept.length > 0 ? filteredSearch.kept : keptSearchUs),
        summarize(filteredDelete.kept.length > 0 ? filteredDelete.kept : keptDeleteUs),
      );
    } catch (err) {
      console.error('error running size', n, err);
      break;
    }
  }
}

async function main() {
  const sizes = [1000, 2500, 5000, 7500, 10000, 25000, 50000, 75000, 100000];
  const totalRuns = 11;
  const discardRuns = 3;
  const summary: 'median' | 'average' = 'median';
  const commitEvery = 1000;
  const coolDownMs = 200;
  const backend: FileBackend = process.argv.includes('--mock') ? 'mock' : 'real';

  console.log('Steady-state benchmark using FBNodeStorage (disk-based) + BPlusTree');
  console.log(`Each size tested ${totalRuns} times`);
  console.log(`Backend: ${backend} (real backend uses fresh temp files per run)`);
  console.log(`Commit checkpoint every ${commitEvery} operations`);
  console.log(`Cool-down between kept runs: ${coolDownMs}ms`);
  console.log('Recommended V8 flags: --expose-gc --max-old-space-size=4096 --initial-old-space-size=4096');
  const hasMaxOld = process.execArgv.some((arg) => arg.startsWith('--max-old-space-size='));
  const hasInitialOld = process.execArgv.some((arg) => arg.startsWith('--initial-old-space-size='));
  if (typeof global.gc !== 'function' || !hasMaxOld || !hasInitialOld) {
    console.warn('warning: recommended flags not fully enabled; variance may remain higher than desired.');
  }
  console.log(
    'Per run: prefill tree to n, then perform n times: 1 insert + 1 search(existing random) + 1 delete(existing random)',
  );

  console.log('Mode: random prefill order');
  await runSizes(sizes, 'random', { totalRuns, discardRuns, summary, commitEvery, coolDownMs, backend });

  console.log('\nMode: sequential prefill order');
  await runSizes(sizes, 'sequential', { totalRuns, discardRuns, summary, commitEvery, coolDownMs, backend });

  console.log('\nDone.');
}

await main();
