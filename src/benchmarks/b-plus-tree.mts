import { performance } from 'perf_hooks';
import { BPlusTree } from '../b-plus-tree.mjs';
import { TrivialNodeStorage, TrivialLeafNode, TrivialInternalNode } from '../trivial-node-storage.mjs';

function xorshift32(seed: number) {
  let s = seed >>> 0;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return s >>> 0;
  };
}

function makeRandomKeys(n: number, maxKey: number, seed = 1234567): number[] {
  const rand = xorshift32(seed);
  return Array.from({ length: n }, () => rand() % Math.max(1, maxKey));
}

function shuffle<T>(arr: T[], seed = 42) {
  const rnd = xorshift32(seed);
  for (let i = arr.length - 1; i > 0; i--) {
    const j = rnd() % (i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}

function makeTree(order = 32) {
  const compareKeys = (a: number, b: number) => (a < b ? -1 : a > b ? 1 : 0);
  const keySize = (_k: number) => 8;
  const storage = new TrivialNodeStorage<number, number>(compareKeys, keySize);
  const tree = new BPlusTree<number, number, TrivialLeafNode<number, number>, TrivialInternalNode<number, number>>(
    storage,
    order,
  );
  return { tree, storage };
}

async function timeInsert(
  tree: BPlusTree<number, number, TrivialLeafNode<number, number>, TrivialInternalNode<number, number>>,
  keys: number[],
) {
  const t0 = performance.now();
  for (const key of keys) {
    await tree.insert(key, key);
  }
  const t1 = performance.now();
  return t1 - t0;
}

async function timeSearch(
  tree: BPlusTree<number, number, TrivialLeafNode<number, number>, TrivialInternalNode<number, number>>,
  keys: number[],
) {
  const t0 = performance.now();
  for (const key of keys) {
    await tree.search(key);
  }
  const t1 = performance.now();
  return t1 - t0;
}

async function timeDelete(
  tree: BPlusTree<number, number, TrivialLeafNode<number, number>, TrivialInternalNode<number, number>>,
  keys: number[],
) {
  const t0 = performance.now();
  for (const key of keys) {
    await tree.delete(key);
  }
  const t1 = performance.now();
  return t1 - t0;
}

function printRow(n: number, insMs: number, srchMs: number, delMs: number) {
  const log2n = Math.log2(n);
  const insUs = (insMs / n) * 1000;
  const srchUs = (srchMs / n) * 1000;
  const delUs = (delMs / n) * 1000;
  console.log(
    `${n}\tlog2(n)=${log2n.toFixed(2)}\tinsert_total_ms=${insMs.toFixed(2)}\tinsert_us/op=${insUs.toFixed(
      3,
    )}\tsearch_total_ms=${srchMs.toFixed(2)}\tsearch_us/op=${srchUs.toFixed(3)}\tdelete_total_ms=${delMs.toFixed(
      2,
    )}\tdelete_us/op=${delUs.toFixed(3)}`,
  );
}

async function runOnce(n: number, opts: { mode: 'random' | 'sequential'; order?: number }) {
  const { tree } = makeTree(opts.order ?? 32);
  await tree.init();

  let keys: number[];
  if (opts.mode === 'sequential') {
    keys = Array.from({ length: n }, (_, i) => i);
  } else {
    keys = makeRandomKeys(n, Math.max(n * 2, 1000), 12345);
  }

  const warmup = Math.min(50, Math.floor(n * 0.01));
  for (let i = 0; i < warmup; i++) {
    await tree.insert(keys[i], keys[i]);
  }

  const insertKeys = keys.slice(warmup);

  const insertMs = await timeInsert(tree, insertKeys);
  const searchKeys = keys.slice();
  shuffle(searchKeys, 9999);
  const srchMs = await timeSearch(tree, searchKeys);

  const delMs = await timeDelete(tree, keys);

  return { n, insertMs, srchMs, delMs };
}

async function runSizes(sizes: number[], mode: 'random' | 'sequential') {
  console.log(
    'n\tlog2(n)\tinsert_total_ms\tinsert_us/op\tsearch_total_ms\tsearch_us/op\tdelete_total_ms\tdelete_us/op',
  );
  for (const n of sizes) {
    try {
      const repeats = 2;
      const insertArr: number[] = [];
      const searchArr: number[] = [];
      const deleteArr: number[] = [];
      for (let r = 0; r < repeats; r++) {
        const res = await runOnce(n, { mode, order: 32 });
        insertArr.push(res.insertMs);
        searchArr.push(res.srchMs);
        deleteArr.push(res.delMs);
      }
      const avg = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length;
      printRow(n, avg(insertArr), avg(searchArr), avg(deleteArr));
    } catch (err) {
      console.error('error running size', n, err);
      break;
    }
  }
}

async function main() {
  const sizes = [1000, 5000, 10000, 50000, 100000];
  console.log('Simple benchmark using TrivialNodeStorage + BPlusTree');
  console.log('Mode: random inserts/searches');
  await runSizes(sizes, 'random');

  console.log('\nMode: sequential inserts/searches');
  await runSizes(sizes, 'sequential');

  console.log('\nDone.');
}

await main();
