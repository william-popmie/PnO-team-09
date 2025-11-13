// @author Mathias Bouhon Keulen, Frederick Hillen
// @date 2025-11-10

import type { NodeStorage, LeafNodeStorage, InternalNodeStorage } from './node-storage/node-storage.mjs';

/**
 * A B+ tree implementation.
 *
 * @template KeysType - The type of the keys in the B+ tree.
 * @template ValuesType - The type of the values in the B+ tree.
 * @template LeafNodeStorageType - The type of the leaf node storage.
 * @template InternalNodeStorageType - The type of the internal node storage.
 *
 */
export class BPlusTree<
  KeysType,
  ValuesType,
  LeafNodeStorageType extends LeafNodeStorage<KeysType, ValuesType, LeafNodeStorageType, InternalNodeStorageType>,
  InternalNodeStorageType extends InternalNodeStorage<
    KeysType,
    ValuesType,
    LeafNodeStorageType,
    InternalNodeStorageType
  >,
> {
  private root!: LeafNodeStorageType | InternalNodeStorageType;
  private readonly storage: NodeStorage<KeysType, ValuesType, LeafNodeStorageType, InternalNodeStorageType>;
  private readonly order: number;

  /**
   * Creates a new B+ tree.
   *
   * @param {NodeStorage} storage - The node storage to use.
   * @param {number} order - The order of the B+ tree (maximum number of keys per node).
   * @throws {Error} If the order is less than 1.
   */
  constructor(storage: NodeStorage<KeysType, ValuesType, LeafNodeStorageType, InternalNodeStorageType>, order: number) {
    if (order < 1) throw new Error('order must be >= 1');
    this.storage = storage;
    this.order = order;
  }

  /**
   * Initializes the B+ tree.
   *
   * @returns {Promise<void>} A promise that resolves when the tree is initialized.
   */
  async init(): Promise<void> {
    this.root = await this.storage.createTree();
  }

  /**
   * Inserts a key-value pair into the B+ tree.
   *
   * @param {KeysType} key - The key to insert.
   * @param {ValuesType} value - The value to insert.
   * @returns {Promise<void>} A promise that resolves when the insertion is complete.
   */
  async insert(key: KeysType, value: ValuesType): Promise<void> {
    const leaf = await this.findLeaf(key);
    await this.insertInLeaf(leaf, key, value);

    if (leaf.keys.length > this.order) {
      const redistributed = await this.tryRedistributeLeafBeforeSplit(leaf);
      if (!redistributed) {
        await this.splitLeaf(leaf);
      }
    }
  }

  /**
   * Searches for a key in the B+ tree and returns its associated value.
   *
   * @param {KeysType} key - The key to search for.
   * @returns {Promise<ValuesType | null>} A promise that resolves to the value associated with the key, or null if not found.
   */
  async search(key: KeysType): Promise<ValuesType | null> {
    const leaf = await this.findLeaf(key);
    const result = await leaf.getCursorBeforeKey(key);
    const { cursor, isAtKey } = result;

    if (!isAtKey) return null;
    const pair = cursor.getKeyValuePairAfter();
    return pair ? pair.value : null;
  }

  /**
   * Deletes a key-value pair from the B+ tree.
   *
   * @param {KeysType} key - The key to delete.
   * @returns {Promise<void>} A promise that resolves when the deletion is complete.
   */
  async delete(key: KeysType): Promise<void> {
    const leaf = await this.findLeaf(key);
    const { cursor, isAtKey } = await leaf.getCursorBeforeKey(key);
    if (!isAtKey) return;

    await cursor.removeKeyValuePairAfter();

    const minKeys = Math.ceil(this.order / 2);

    if (leaf === this.root) {
      if (leaf.keys.length === 0) {
        this.root = await this.storage.createLeaf();
      }
      return;
    }

    if (leaf.keys.length < minKeys) {
      await this.handleUnderflow(leaf);
    }
  }

  /**
   * Prints a simple text representation of the B+ tree to the console.
   *
   * @returns {void}
   */
  printTree(): void {
    if (!this.root) {
      console.log('<empty>');
      return;
    }

    const q: (LeafNodeStorageType | InternalNodeStorageType)[] = [this.root];
    while (q.length) {
      const levelCount = q.length;
      const parts: string[] = [];
      for (let i = 0; i < levelCount; i++) {
        const n = q.shift()!;
        if (n.isLeaf) {
          parts.push(`Leaf(${n.keys.map((k) => String(k)).join(',')})`);
        } else {
          const inNode = n;
          parts.push(`Internal(keys:${inNode.keys.map((k) => String(k)).join(',')})`);
          for (const c of inNode.children) q.push(c);
        }
      }
      console.log(parts.join(' | '));
    }
  }

  /**
   * Prints an ASCII representation of the B+ tree to the console.
   *
   * @returns {void}
   *
   * @example
   *         [15,20]
   *    --------|-------
   *    |       |      |
   * [5,10]   [15]   [20]
   */
  ascii(): void {
    if (!this.root) {
      console.log('<empty>');
      return;
    }

    const nodeText = (n: LeafNodeStorageType | InternalNodeStorageType) =>
      `[${n.keys.map((k) => String(k)).join(',')}]`;

    function layout(node: LeafNodeStorageType | InternalNodeStorageType): {
      lines: string[];
      width: number;
      middle: number;
    } {
      const text = nodeText(node);
      const textWidth = Math.max(1, text.length);

      if (node.isLeaf) {
        return { lines: [text], width: textWidth, middle: Math.floor(textWidth / 2) };
      }

      const internal = node;
      const childrenLayouts = internal.children.map((c) => layout(c));

      const gap = 3;
      const childrenWidth = childrenLayouts.reduce((s, cl) => s + cl.width, 0);
      const totalChildrenWidth = childrenWidth + Math.max(0, childrenLayouts.length - 1) * gap;
      let width = Math.max(textWidth, totalChildrenWidth);

      let childStarts: number[] = [];
      const computeChildStarts = () => {
        childStarts = [];
        let cur = Math.floor((width - totalChildrenWidth) / 2);
        for (const cl of childrenLayouts) {
          childStarts.push(cur);
          cur += cl.width + gap;
        }
      };
      computeChildStarts();

      let parentMiddle: number;
      if (childrenLayouts.length % 2 === 1) {
        const midIndex = Math.floor(childrenLayouts.length / 2);
        parentMiddle = childStarts[midIndex] + childrenLayouts[midIndex].middle;
        const leftNeeded = Math.max(0, Math.floor(textWidth / 2) - parentMiddle);
        const rightNeeded = Math.max(0, parentMiddle + Math.ceil(textWidth / 2) - (width - 1));
        if (leftNeeded > 0 || rightNeeded > 0) {
          width += leftNeeded + rightNeeded;
          computeChildStarts();
          parentMiddle = childStarts[midIndex] + childrenLayouts[midIndex].middle;
        }
      } else {
        parentMiddle = Math.floor(width / 2);
        const leftNeeded = Math.max(0, Math.floor(textWidth / 2) - parentMiddle);
        const rightNeeded = Math.max(0, parentMiddle + Math.ceil(textWidth / 2) - (width - 1));
        if (leftNeeded > 0 || rightNeeded > 0) {
          width += leftNeeded + rightNeeded;
          computeChildStarts();
          parentMiddle = Math.floor(width / 2);
        }
      }

      const parentStart = parentMiddle - Math.floor(text.length / 2);
      const parentLine =
        ' '.repeat(Math.max(0, parentStart)) + text + ' '.repeat(Math.max(0, width - parentStart - text.length));

      const childMiddles = childrenLayouts.map((cl, i) => childStarts[i] + cl.middle);
      const bridgeLeft = Math.min(...childMiddles);
      const bridgeRight = Math.max(...childMiddles);

      const bridgeRowArr = new Array(width).fill(' ');
      for (let c = bridgeLeft; c <= bridgeRight; c++) bridgeRowArr[c] = '-';
      bridgeRowArr[parentMiddle] = '|';

      const dropRowArr = new Array(width).fill(' ');
      for (const cm of childMiddles) dropRowArr[cm] = '|';

      const connectorLines = [bridgeRowArr.join(''), dropRowArr.join('')];

      const maxChildHeight = Math.max(...childrenLayouts.map((c) => c.lines.length));
      const childLines: string[] = [];
      for (let row = 0; row < maxChildHeight; row++) {
        let line = '';
        for (let i = 0; i < childrenLayouts.length; i++) {
          const cl = childrenLayouts[i];
          const clLine = cl.lines[row] ?? ' '.repeat(cl.width);
          const needPad = childStarts[i] - line.length;
          if (needPad > 0) line += ' '.repeat(needPad);
          line += clLine;
        }
        if (line.length < width) line += ' '.repeat(width - line.length);
        childLines.push(line);
      }

      const lines = [parentLine, ...connectorLines, ...childLines];
      return { lines, width, middle: parentMiddle };
    }

    const picture = layout(this.root);
    for (const l of picture.lines) console.log(l);
  }

  /**
   * Returns an async generator that yields all key-value pairs in the B+ tree in order.
   *
   * @returns {AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown>} An async generator yielding key-value pairs.
   */
  public async *entries(): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    if (!this.root) return;
    let leaf = await this.getLeftmostLeaf();
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        yield { key: leaf.keys[i], value: leaf.values[i] };
      }
      if (leaf.nextLeaf) {
        leaf = leaf.nextLeaf;
      } else {
        break;
      }
    }
  }

  /**
   * Returns an async iterator that yields all key-value pairs in the B+ tree in order.
   *
   * @returns {AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown>} An async generator yielding key-value pairs.
   */
  public async *[Symbol.asyncIterator](): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry;
    }
  }

  /**
   * Returns an async generator that yields all keys in the B+ tree in order.
   *
   * @returns {AsyncGenerator<KeysType, void, unknown>} An async generator yielding keys.
   */
  public async *keys(): AsyncGenerator<KeysType, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry.key;
    }
  }

  /**
   * Returns an async generator that yields all values in the B+ tree in order.
   *
   * @returns {AsyncGenerator<ValuesType, void, unknown>} An async generator yielding values.
   */
  public async *values(): AsyncGenerator<ValuesType, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry.value;
    }
  }

  /**
   * Returns an async generator that yields all key-value pairs starting from the specified key.
   *
   * @param {KeysType} startKey - The key to start from.
   * @returns {AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown>} An async generator yielding key-value pairs.
   */
  public async *entriesFrom(startKey: KeysType): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    if (!this.root) return;
    let leaf: LeafNodeStorageType | null = await this.findLeaf(startKey);

    const cmp = (a: KeysType, b: KeysType): number => (a < b ? -1 : a > b ? 1 : 0);

    while (leaf) {
      const startIdx = leaf.keys.findIndex((k) => cmp(k, startKey) >= 0);

      if (startIdx === -1) {
        leaf = leaf.nextLeaf ?? null;
        continue;
      }

      for (let i = startIdx; i < leaf.keys.length; i++) {
        yield { key: leaf.keys[i], value: leaf.values[i] };
      }
      leaf = leaf.nextLeaf ?? null;
    }
  }

  /**
   * Returns an async generator that yields all key-value pairs within the specified key range.
   *
   * @param {KeysType} startKey - The start key of the range.
   * @param {KeysType} endKey - The end key of the range.
   * @param {Object} [options] - Options for the range query.
   * @param {boolean} [options.inclusiveStart=true] - Whether to include the start key.
   * @param {boolean} [options.inclusiveEnd=false] - Whether to include the end key.
   * @param {(a: KeysType, b: KeysType) => number} [options.comparator] - A custom comparator function for keys.
   * @returns {AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown>} An async generator yielding key-value pairs within the range.
   */
  public async *range(
    startKey: KeysType,
    endKey: KeysType,
    options?: { inclusiveStart?: boolean; inclusiveEnd?: boolean; comparator?: (a: KeysType, b: KeysType) => number },
  ): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    const inclusiveStart = options?.inclusiveStart ?? true;
    const inclusiveEnd = options?.inclusiveEnd ?? false;
    const comparator = options?.comparator ?? ((a: KeysType, b: KeysType) => (a < b ? -1 : a > b ? 1 : 0));

    const startLeafGenerator = this.entriesFrom(startKey);
    for await (const { key, value } of startLeafGenerator) {
      const cmpStart = comparator(key, startKey);
      if (!inclusiveStart && cmpStart === 0) {
        continue;
      }
      const cmpEnd = comparator(key, endKey);
      if (cmpEnd > 0 || (!inclusiveEnd && cmpEnd === 0)) {
        break;
      }
      yield { key: key, value: value };
    }
  }

  /**
   * Executes a provided function once for each key-value pair in the B+ tree.
   *
   * @param {(key: KeysType, value: ValuesType) => void | Promise<void>} callback - The function to execute for each key-value pair.
   * @returns {Promise<void>} A promise that resolves when all key-value pairs have been processed.
   */
  public async forEach(callback: (key: KeysType, value: ValuesType) => void | Promise<void>): Promise<void> {
    for await (const { key, value } of this.entries()) {
      await callback(key, value);
    }
  }

  /**
   * Clears the B+ tree, removing all key-value pairs.
   *
   * @returns {Promise<void>} A promise that resolves when the tree is cleared.
   */
  public async clear(): Promise<void> {
    this.root = await this.storage.createTree();
  }

  /**
   * Retrieves the leftmost leaf node of the B+ tree.
   *
   * @returns {Promise<LeafNodeStorageType>} A promise that resolves to the leftmost leaf node.
   * @throws {Error} If the tree is not initialized.
   */
  private async getLeftmostLeaf(): Promise<LeafNodeStorageType> {
    if (!this.root) throw new Error('Tree is not initialized');
    let node: LeafNodeStorageType | InternalNodeStorageType = this.root;
    while (!node.isLeaf) {
      const internal = node;
      node = internal.children[0];
    }
    return Promise.resolve(node);
  }

  /**
   * Handles underflow in a node by borrowing or merging with siblings.
   *
   * @param {LeafNodeStorageType | InternalNodeStorageType} node - The node that is underflowing.
   * @returns {Promise<void>} A promise that resolves when the underflow is handled.
   * @throws {Error} If the parent does not contain the node in its children.
   */
  private async handleUnderflow(node: LeafNodeStorageType | InternalNodeStorageType): Promise<void> {
    const parent = await this.findParent(node);
    if (!parent) {
      return;
    }

    const index = parent.children.indexOf(node);
    if (index === -1) throw new Error('Parent does not contain node in children');

    const leftSibling = index > 0 ? parent.children[index - 1] : null;
    const rightSibling = index < parent.children.length - 1 ? parent.children[index + 1] : null;

    const minKeys = Math.ceil(this.order / 2);

    if (leftSibling && leftSibling.keys.length > minKeys) {
      if (node.isLeaf) {
        const ls = leftSibling as LeafNodeStorageType;
        const borrowedKey = ls.keys.pop() as KeysType;
        const borrowedValue = ls.values.pop() as ValuesType;
        node.keys.unshift(borrowedKey);
        node.values.unshift(borrowedValue);
        parent.keys[index - 1] = node.keys[0];
      } else {
        const ls = leftSibling as InternalNodeStorageType;
        const borrowedKey = ls.keys.pop() as KeysType;
        const borrowedChild = ls.children.pop() as LeafNodeStorageType | InternalNodeStorageType;
        const sep = parent.keys[index - 1];
        node.keys.unshift(sep);
        node.children.unshift(borrowedChild);
        parent.keys[index - 1] = borrowedKey;
      }
      return;
    }

    if (rightSibling && rightSibling.keys.length > minKeys) {
      if (node.isLeaf) {
        const rs = rightSibling as LeafNodeStorageType;
        const borrowedKey = rs.keys.shift() as KeysType;
        const borrowedValue = rs.values.shift() as ValuesType;
        node.keys.push(borrowedKey);
        node.values.push(borrowedValue);
        parent.keys[index] = rs.keys[0];
      } else {
        const rs = rightSibling as InternalNodeStorageType;
        const borrowedKey = rs.keys.shift() as KeysType;
        const borrowedChild = rs.children.shift() as LeafNodeStorageType | InternalNodeStorageType;
        const sep = parent.keys[index];
        node.keys.push(sep);
        node.children.push(borrowedChild);
        parent.keys[index] = borrowedKey;
      }
      return;
    }

    if (leftSibling) {
      const separatorKey = parent.keys[index - 1];
      await leftSibling.mergeWithNext(separatorKey, node);
      parent.keys.splice(index - 1, 1);
      parent.children.splice(index, 1);
    } else if (rightSibling) {
      const separatorKey = parent.keys[index];
      await node.mergeWithNext(separatorKey, rightSibling);
      parent.keys.splice(index, 1);
      parent.children.splice(index + 1, 1);
    }

    if (parent === this.root) {
      if (!parent.isLeaf && parent.keys.length === 0) {
        this.root = parent.children[0];
      }
      return;
    }

    if (parent.keys.length < minKeys) {
      await this.handleUnderflow(parent);
    }
  }

  /**
   * Finds the leaf node that should contain the specified key.
   *
   * @param {KeysType} key - The key to find the leaf for.
   * @returns {Promise<LeafNodeStorageType>} A promise that resolves to the leaf node.
   * @throws {Error} If the child node cannot be found while descending.
   */
  private async findLeaf(key: KeysType): Promise<LeafNodeStorageType> {
    let node: LeafNodeStorageType | InternalNodeStorageType = this.root;
    while (!node.isLeaf) {
      const internal = node;
      const { cursor } = await internal.getChildCursorAtKey(key);
      const child = await cursor.getChild();
      if (!child) throw new Error('Child not found while descending');
      node = child;
    }
    return node;
  }

  /**
   * Finds the parent of a given node.
   *
   * @param {LeafNodeStorageType | InternalNodeStorageType} child - The child node whose parent is to be found.
   * @returns {Promise<InternalNodeStorageType | null>} A promise that resolves to the parent node, or null if the child is the root.
   */
  private async findParent(
    child: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<InternalNodeStorageType | null> {
    if (child === this.root) return null;
    if (this.root.isLeaf) return null;
    return this.findParentRecursive(this.root, child);
  }

  /**
   * Recursively finds the parent of a given child node starting from a specified node.
   *
   * @param {InternalNodeStorageType} node - The current node to search from.
   * @param {LeafNodeStorageType | InternalNodeStorageType} child - The child node whose parent is to be found.
   * @returns {Promise<InternalNodeStorageType | null>} A promise that resolves to the parent node, or null if not found.
   */
  private async findParentRecursive(
    node: InternalNodeStorageType,
    child: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<InternalNodeStorageType | null> {
    const cursor = await node.getChildCursorAtFirstChild();

    // eslint-disable-next-line @typescript-eslint/prefer-for-of
    for (let i = 0; i < node.children.length; i++) {
      const currentChild = await cursor.getChild();
      if (currentChild === child) {
        return node;
      }
      if (!currentChild.isLeaf) {
        const parent = await this.findParentRecursive(currentChild, child);
        if (parent) return parent;
      }
      cursor.moveNext();
    }
    return null;
  }

  /**
   * Inserts a key-value pair into a leaf node.
   *
   * @param {LeafNodeStorageType} leaf - The leaf node to insert into.
   * @param {KeysType} key - The key to insert.
   * @param {ValuesType} value - The value to insert.
   * @returns {Promise<void>} A promise that resolves when the insertion is complete.
   */
  private async insertInLeaf(leaf: LeafNodeStorageType, key: KeysType, value: ValuesType): Promise<void> {
    const result = await leaf.getCursorBeforeKey(key);
    const { cursor } = result;
    await cursor.insert(key, value);
  }

  /**
   * Attempt to redistribute keys from an overfull leaf into an adjacent sibling
   * so we can avoid splitting the leaf. Returns true when redistribution happened.
   *
   * @param {LeafNodeStorageType} leaf - The leaf node to redistribute from.
   * @returns {Promise<boolean>} A promise that resolves to true if redistribution occurred, false otherwise.
   */
  private async tryRedistributeLeafBeforeSplit(leaf: LeafNodeStorageType): Promise<boolean> {
    const parent = await this.findParent(leaf);
    if (!parent) return false;

    const idx = parent.children.indexOf(leaf);
    if (idx === -1) throw new Error('Parent does not contain leaf in children');

    const left = idx > 0 ? (parent.children[idx - 1] as LeafNodeStorageType) : null;
    const right = idx < parent.children.length - 1 ? (parent.children[idx + 1] as LeafNodeStorageType) : null;

    if (left && left.keys.length < this.order) {
      const movedKey = leaf.keys.shift() as KeysType;
      const movedVal = leaf.values.shift() as ValuesType;
      left.keys.push(movedKey);
      left.values.push(movedVal);
      parent.keys[idx - 1] = leaf.keys[0];
      return true;
    }

    if (right && right.keys.length < this.order) {
      const movedKey = leaf.keys.pop() as KeysType;
      const movedVal = leaf.values.pop() as ValuesType;
      right.keys.unshift(movedKey);
      right.values.unshift(movedVal);
      parent.keys[idx] = right.keys[0];
      return true;
    }

    return false;
  }

  /**
   * Splits a leaf node that has exceeded the maximum number of keys.
   *
   * @param {LeafNodeStorageType} leaf - The leaf node to split.
   * @returns {Promise<void>} A promise that resolves when the split is complete.
   */
  private async splitLeaf(leaf: LeafNodeStorageType): Promise<void> {
    const mid = Math.ceil(leaf.keys.length / 2);
    const newLeaf = await this.storage.createLeaf();

    newLeaf.keys = leaf.keys.splice(mid);
    newLeaf.values = leaf.values.splice(mid);

    newLeaf.nextLeaf = leaf.nextLeaf;
    leaf.nextLeaf = newLeaf;

    const promotedKey = newLeaf.keys[0];
    await this.insertInParent(leaf, promotedKey, newLeaf);
  }

  /**
   * Attempt to redistribute children from an overfull internal node into an adjacent sibling
   * so we can avoid splitting the internal node. Returns true when redistribution happened.
   *
   * @param {InternalNodeStorageType} internal - The internal node to redistribute from.
   * @returns {Promise<boolean>} A promise that resolves to true if redistribution occurred, false otherwise.
   */
  private async tryRedistributeInternalBeforeSplit(internal: InternalNodeStorageType): Promise<boolean> {
    const parent = await this.findParent(internal);
    if (!parent) return false;

    const idx = parent.children.indexOf(internal);
    if (idx === -1) throw new Error('Parent does not contain internal node in children');

    const left = idx > 0 ? (parent.children[idx - 1] as InternalNodeStorageType) : null;
    const right = idx < parent.children.length - 1 ? (parent.children[idx + 1] as InternalNodeStorageType) : null;

    if (left && left.keys.length < this.order) {
      const sepFromParent = parent.keys[idx - 1];
      const movedChild = internal.children.shift() as LeafNodeStorageType | InternalNodeStorageType;
      const newParentSep = internal.keys.shift() as KeysType;
      left.keys.push(sepFromParent);
      left.children.push(movedChild);
      parent.keys[idx - 1] = newParentSep;
      return true;
    }

    if (right && right.keys.length < this.order) {
      const sepFromParent = parent.keys[idx];
      const movedChild = internal.children.pop() as LeafNodeStorageType | InternalNodeStorageType;
      const newParentSep = internal.keys.pop() as KeysType;
      right.keys.unshift(sepFromParent);
      right.children.unshift(movedChild);
      parent.keys[idx] = newParentSep;
      return true;
    }

    return false;
  }

  /**
   * Splits an internal node that has exceeded the maximum number of keys.
   *
   * @param {InternalNodeStorageType} internal - The internal node to split.
   * @returns {Promise<void>} A promise that resolves when the split is complete.
   */
  private async splitInternalNode(internal: InternalNodeStorageType): Promise<void> {
    const redistributed = await this.tryRedistributeInternalBeforeSplit(internal);
    if (redistributed) return;

    const mid = Math.floor(internal.keys.length / 2);
    const promotedKey = internal.keys[mid];

    const leftKeys = internal.keys.slice(0, mid);
    const rightKeys = internal.keys.slice(mid + 1);

    const leftChildren = internal.children.slice(0, mid + 1);
    const rightChildren = internal.children.slice(mid + 1);

    internal.keys = leftKeys;
    internal.children = leftChildren;

    const newInternal = await this.storage.createInternalNode(rightChildren, rightKeys);

    await this.insertInParent(internal, promotedKey, newInternal);
  }

  /**
   * Inserts a promoted key and new node into the parent of a given node.
   *
   * @param {LeafNodeStorageType | InternalNodeStorageType} node - The node whose parent will receive the promoted key and new node.
   * @param {KeysType} promotedKey - The key to promote to the parent.
   * @param {LeafNodeStorageType | InternalNodeStorageType} newNode - The new node to insert into the parent.
   * @returns {Promise<void>} A promise that resolves when the insertion is complete.
   * @throws {Error} If the parent node cannot be found.
   * @throws {Error} If the parent does not contain the node as a child.
   */
  private async insertInParent(
    node: LeafNodeStorageType | InternalNodeStorageType,
    promotedKey: KeysType,
    newNode: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<void> {
    if (node === this.root) {
      const newRoot = await this.storage.createInternalNode([node, newNode], [promotedKey]);
      this.root = newRoot;
      return;
    }

    const parent = await this.findParent(node);
    if (!parent) throw new Error('Parent not found while inserting in parent');

    const idx = parent.children.indexOf(node);
    if (idx === -1) throw new Error('Parent does not contain node as child');

    parent.keys.splice(idx, 0, promotedKey);
    parent.children.splice(idx + 1, 0, newNode);

    if (parent.keys.length > this.order) {
      await this.splitInternalNode(parent);
    }
  }
}
