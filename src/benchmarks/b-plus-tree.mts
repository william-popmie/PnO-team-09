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

/*
Mode: random inserts/searches
n       log2(n)         insert_total_ms         insert_us/op            search_total_ms         search_us/op            delete_total_ms         delete_us/op
1000    log2(n)=9.97    insert_total_ms=4.86    insert_us/op=4.863      search_total_ms=1.41    search_us/op=1.413      delete_total_ms=2.28    delete_us/op=2.284
5000    log2(n)=12.29   insert_total_ms=30.67   insert_us/op=6.134      search_total_ms=5.42    search_us/op=1.085      delete_total_ms=18.05   delete_us/op=3.609
10000   log2(n)=13.29   insert_total_ms=93.21   insert_us/op=9.321      search_total_ms=9.03    search_us/op=0.903      delete_total_ms=55.06   delete_us/op=5.506
50000   log2(n)=15.61   insert_total_ms=1979.79 insert_us/op=39.596     search_total_ms=58.37   search_us/op=1.167      delete_total_ms=1205.82 delete_us/op=24.116
100000  log2(n)=16.61   insert_total_ms=7805.24 insert_us/op=78.052     search_total_ms=120.30  search_us/op=1.203      delete_total_ms=4860.59 delete_us/op=48.606

Mode: sequential inserts/searches
n       log2(n)         insert_total_ms         insert_us/op            search_total_ms         search_us/op            delete_total_ms         delete_us/op
1000    log2(n)=9.97    insert_total_ms=2.68    insert_us/op=2.681      search_total_ms=0.74    search_us/op=0.739      delete_total_ms=0.95    delete_us/op=0.947
5000    log2(n)=12.29   insert_total_ms=37.13   insert_us/op=7.427      search_total_ms=4.70    search_us/op=0.940      delete_total_ms=7.00    delete_us/op=1.400
10000   log2(n)=13.29   insert_total_ms=137.98  insert_us/op=13.798     search_total_ms=9.60    search_us/op=0.960      delete_total_ms=14.51   delete_us/op=1.451
50000   log2(n)=15.61   insert_total_ms=2954.60 insert_us/op=59.092     search_total_ms=60.78   search_us/op=1.216      delete_total_ms=79.91   delete_us/op=1.598
100000  log2(n)=16.61   insert_total_ms=11544.3 insert_us/op=115.443    search_total_ms=125.06  search_us/op=1.251      delete_total_ms=170.94  delete_us/op=1.709
*/
