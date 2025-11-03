import assert from 'node:assert';

/*

This file provides an outline and test suite for an implementation of a 2-3-tree algorithm. Completing this algorithm is a
good way to bootstrap your B-tree implementation. (A 2-3-tree is a special case of a B-tree.)

Note: this outline and test suite is for an implementation where the values in the interior nodes are duplicates of values
that also appear in leaf nodes. It therefore corresponds to a B+-tree rather than a B-tree. For this project, you may choose
to implement either a B-tree or a B+-tree (or any other data structure that offers the specified performance). In the material
we provide, we often use "B-tree" and "B+-tree" interchangeably. Both are perfectly balanced trees with a variable number of
children per interior node.

*/
type LeafNode = string;
/**
 * An interior node is an array of odd length, where the elements at even indices are either string (i.e. leaf nodes)
 * or child interior nodes, and each element at odd index i is a string value v such that the values in the subtree rooted
 * at index i - 1 are not greater than v, and the values in the subtree rooted at index i + 1 are greater than v.
 *
 * In a 2-3-tree, all interior nodes have either two or three children.
 */
type InteriorNode = (Node | string)[];
type Node = LeafNode | InteriorNode;
/**
 * `null` represents the empty tree.
 */
type Tree = Node | null;

export function isLeaf(node: Node): node is LeafNode {
  return typeof node === 'string';
}

export function isFull(node: InteriorNode): boolean {
  return node.length > 5;
}

/**
 * Returns an updated version of the interior node, which may be overfull (more than 3 children/5 elements).
 *
 * If the node's children are themselves interior nodes, this function calls itself recursively on the appropriate child.
 * Then, it replaces the child by the recursive call's result (after splitting it if it is overfull).
 * 
 * @param {InteriorNode} node - The interior node to insert into.
 * @param {string} value - The value to insert.
 * @returns {InteriorNode} - The updated interior node.
 */
function insertIntoSubtree(node: InteriorNode, value: string): InteriorNode {
  let idx = 0;
  while (idx < node.length) {
    if (idx % 2 === 1 && value < (node[idx] as string)) {
      break;
    }
    idx++;
  }

  const childIdx = idx % 2 === 0 ? idx : Math.max(idx - 1, 0);

  const child = node[childIdx];

  if (isLeaf(child)) {
    const newLeaf: LeafNode[] = [child, value].sort();
    if (newLeaf.length === 2) {
      node.splice(childIdx, 1, newLeaf[0], newLeaf[0], newLeaf[1]); 

    } else if (newLeaf.length === 3){
      node.splice(childIdx, 1, ...newLeaf);
    }
     else {
      node[childIdx] = newLeaf;
    }
  } else {
    const updatedChild = insertIntoSubtree(child, value); 
    node[childIdx] = updatedChild;

    if (isFull(updatedChild)) {
      const mid = Math.floor(updatedChild.length / 2);
      const left = updatedChild.slice(0, mid);
      const right = updatedChild.slice(mid + 1);
      const midValue = updatedChild[mid];

      node.splice(childIdx, 1, left, midValue, right);
    }
  }

  return node;
}

/**
 * If the tree is an interior node, calls insertIntoSubtree on it. Then, if the result is overfull, it splits it into multiple nodes.
 * 
 * @param {Tree} tree - The tree to insert into.
 * @param {string} value - The value to insert.
 * @returns {Tree} - The updated tree.
 */
function insert(tree: Tree, value: string): Tree {
  if (tree === null) {
      return value;
    }

  if (isLeaf(tree)) {
  const leaves = [tree, value].sort();
  if (leaves.length <= 2) return [leaves[0], leaves[0], leaves[1]]; // simple 2-element leaf
  // split leaf
  const left: LeafNode[] = [leaves[0], leaves[0]];
  const right: LeafNode[] = [leaves[2], leaves[2]];
  const middle = leaves[1];
  return [left, middle, right];
}

  const updatedRoot = insertIntoSubtree(tree, value);

  if (isFull(updatedRoot)) {
    const mid = Math.floor(updatedRoot.length / 2);
    const left = updatedRoot.slice(0, mid);
    const right = updatedRoot.slice(mid + 1);
    const midValue = updatedRoot[mid];
    return [left, midValue, right];
  }

  return updatedRoot;
}

/**
 * Returns an updated version of the interior node, which may be underfull (just 1 child/element).
 *
 * If the node's children are themselves interior nodes, this function calls itself recursively on the appropriate child.
 * Then, if the child is underfull:
 * - if it has a predecessor and the predecessor has size 2, it is merged with the predecessor
 * - if it has a predecessor and the predecessor has size 3, the predecessor's last child is split off and added to the front of the child.
 * - otherwise, it must have a successor. If it has size 2, it is merged with the successor.
 * - otherwise, the successor has size 3. The successor's first child is split off and added to the end of the child.
 */
function deleteFromSubtree(node: InteriorNode, value: string): InteriorNode {
  console.log(`deleteFromSubtree called with value=${value} and node=${JSON.stringify(node)}`);
  throw new Error('Not yet implemented');
}

/**
 * If the tree is an interior node, calls deleteFromSubtree on it. Then, if the result is underfull,
 * it uses the sole child node as the new root node.
 */
function delete_(tree: Tree, value: string): Tree {
  console.log(`delete called with value=${value} and tree=${JSON.stringify(tree)}`);
  throw new Error('Not yet implemented');
}

process.nextTick(() => {
  let tree: Tree = null;
  tree = insert(tree, 'hello');
  assert.deepEqual(tree, 'hello');
  tree = insert(tree, 'world');
  assert.deepEqual(tree, ['hello', 'hello', 'world']);
  tree = insert(tree, 'zzz');
  assert.deepEqual(tree, ['hello', 'hello', 'world', 'world', 'zzz']);
  tree = insert(tree, 'lazy');
  assert.deepEqual(tree, [['hello', 'hello', 'lazy'], 'lazy', ['world', 'world', 'zzz']]);
  tree = insert(tree, 'my');
  assert.deepEqual(tree, [['hello', 'hello', 'lazy'], 'lazy', ['my', 'my', 'world', 'world', 'zzz']]);
  tree = insert(tree, 'sweet');
  assert.deepEqual(tree, [
    ['hello', 'hello', 'lazy'],
    'lazy',
    ['my', 'my', 'sweet'],
    'sweet',
    ['world', 'world', 'zzz'],
  ]);
  tree = insert(tree, 'so');
  assert.deepEqual(tree, [
    ['hello', 'hello', 'lazy'],
    'lazy',
    ['my', 'my', 'so', 'so', 'sweet'],
    'sweet',
    ['world', 'world', 'zzz'],
  ]);
  tree = insert(tree, 'oh');
  assert.deepEqual(tree, [
    [['hello', 'hello', 'lazy'], 'lazy', ['my', 'my', 'oh']],
    'oh',
    [['so', 'so', 'sweet'], 'sweet', ['world', 'world', 'zzz']],
  ]);

  tree = delete_(tree, 'oh');
  assert.deepEqual(tree, [
    ['hello', 'hello', 'lazy', 'lazy', 'my'],
    'oh',
    ['so', 'so', 'sweet'],
    'sweet',
    ['world', 'world', 'zzz'],
  ]);
  tree = delete_(tree, 'so');
  assert.deepEqual(tree, [
    ['hello', 'hello', 'lazy'],
    'lazy',
    ['my', 'oh', 'sweet'],
    'sweet',
    ['world', 'world', 'zzz'],
  ]);
  if (Array.isArray(tree)) {
    insertIntoSubtree(tree, 'zzz');
    deleteFromSubtree(tree, 'hello');
  }
});
