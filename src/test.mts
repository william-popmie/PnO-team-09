// @author Mathias Bouhon Keulen
// @date 2025-11-08

import { BPlusTree } from './b-plus-tree.mjs';
import { TrivialNodeStorage, TrivialLeafNode, TrivialInternalNode } from './trivial-node-storage.mjs';

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
