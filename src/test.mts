// @author Mathias Bouhon Keulen
// @date 2025-11-08

import { BPlusTree } from './b-plus-tree.mjs';
import { TrivialNodeStorage, TrivialLeafNode, TrivialInternalNode } from './trivial-node-storage.mjs';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function testBPlusTree() {
  const storage = new TrivialNodeStorage<number, string>(
    (a, b) => a - b,
    (_key) => 8,
  );

  const tree = new BPlusTree<number, string, TrivialLeafNode<number, string>, TrivialInternalNode<number, string>>(
    storage,
    2,
  );

  await tree.init();

  const keysToInsert = [
    10, 20, 5, 15, 25, 30, 3, 8, 12, 18, 22, 28, 35, 1, 4, 6, 7, 9, 11, 13, 14, 16, 17, 19, 21, 23, 24, 26, 27, 29, 31,
    32, 33, 34,
  ];
  console.log('\n=== INSERTION PHASE ===');

  for (const key of keysToInsert) {
    console.log(`\n--- Inserting ${key} ---`);
    await tree.insert(key, `value-${key}`);
    console.log('Tree after insertion:');
    tree.ascii();
  }

  console.log('\n=== TRAVERSAL CHECKS AFTER INSERTS ===');

  console.log('\n-- Full iteration via for await (tree) --');
  for await (const { key, value } of tree) {
    console.log(`${key}:${value}`);
  }
  console.log('\n');

  console.log('\n-- entries() generator --');
  for await (const { key, value } of tree.entries()) {
    console.log(`${key}:${value}`);
  }
  console.log('\n');

  console.log('\n-- keys() generator --');
  for await (const k of tree.keys()) {
    console.log(k);
  }
  console.log('\n');

  console.log('\n-- values() generator (first 20 values shown) --');
  let i = 0;
  for await (const v of tree.values()) {
    console.log(v);
    if (++i >= 20) break;
  }
  console.log('\n');

  const startKey = 13;
  console.log(`\n-- entriesFrom(${startKey}) --`);
  for await (const { key, value } of tree.entriesFrom(startKey)) {
    console.log(`${key}:${value}`);
  }
  console.log('\n');

  console.log('\n-- range(8, 22) default options (inclusiveStart=true, inclusiveEnd=false in this test file) --');
  for await (const { key, value } of tree.range(8, 22)) {
    console.log(`${key}:${value}`);
  }
  console.log('\n');

  console.log('\n-- range(8, 22, { inclusiveStart: true, inclusiveEnd: true }) --');
  for await (const { key, value } of tree.range(8, 22, { inclusiveStart: true, inclusiveEnd: true })) {
    console.log(`${key}:${value}`);
  }
  console.log('\n');

  console.log('\n-- forEach (async callback; will sleep 5ms for each entry, showing first 10) --');
  let count = 0;
  await tree.forEach(async (k, v) => {
    if (count < 10) console.log(`${k}:${v}`);
    count++;
    await sleep(5);
  });
  console.log(`\n(forEach processed ${count} entries)\n`);

  console.log('\n-- Manual iteration with early exit when key === 17 --');
  for await (const { key, value } of tree.entries()) {
    console.log(`${key}:${value}`);
    if (key === 17) {
      console.log('\nEarly exit triggered at key 17');
      break;
    }
  }
  console.log('\n');

  console.log('\n=== SEARCH PHASE ===');
  for (const key of keysToInsert) {
    const result = await tree.search(key);
    console.log(`Search ${key}:`, result);
  }

  console.log('\n=== DELETION PHASE ===');
  const keysToDelete = [5, 10, 15, 20, 25];

  for (const key of keysToDelete) {
    console.log(`\n--- Deleting ${key} ---`);
    await tree.delete(key);
    console.log('Tree after deletion:');
    tree.ascii();

    const result = await tree.search(key);
    console.log(`Search ${key} after deletion:`, result);
  }

  console.log('\n=== FINAL TREE STRUCTURE ===');
  tree.ascii();
}

await testBPlusTree();
