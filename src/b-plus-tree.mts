// @author Mathias Bouhon Keulen
// @date 2025-11-09

import type { NodeStorage, LeafNodeStorage, InternalNodeStorage } from './node-storage.mjs';

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

  constructor(storage: NodeStorage<KeysType, ValuesType, LeafNodeStorageType, InternalNodeStorageType>, order: number) {
    if (order < 1) throw new Error('order must be >= 1');
    this.storage = storage;
    this.order = order;
  }

  async init(): Promise<void> {
    this.root = await this.storage.createTree();
  }

  async insert(key: KeysType, value: ValuesType): Promise<void> {
    const leaf = await this.findLeaf(key);
    await this.insertInLeaf(leaf, key, value);

    if (leaf.keys.length > this.order) {
      await this.splitLeaf(leaf);
    }
  }

  async search(key: KeysType): Promise<ValuesType | null> {
    const leaf = await this.findLeaf(key);
    const result = await leaf.getCursorBeforeKey(key);
    const { cursor, isAtKey } = result;

    if (!isAtKey) return null;
    const pair = cursor.getKeyValuePairAfter();
    return pair ? pair.value : null;
  }

  async delete(key: KeysType): Promise<void> {
    const leaf = await this.findLeaf(key);
    const { cursor, isAtKey } = await leaf.getCursorBeforeKey(key);
    if (!isAtKey) return;

    await cursor.removeKeyValuePairAfter();

    const minKeys = Math.ceil(this.order / 2);

    if (leaf === this.root) {
      if (leaf.keys.length === 0) {
        this.root = this.storage.createLeaf();
      }
      return;
    }

    if (leaf.keys.length < minKeys) {
      await this.handleUnderflow(leaf);
    }
  }

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

  public async *[Symbol.asyncIterator](): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry;
    }
  }

  public async *keys(): AsyncGenerator<KeysType, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry.key;
    }
  }

  public async *values(): AsyncGenerator<ValuesType, void, unknown> {
    for await (const entry of this.entries()) {
      yield entry.value;
    }
  }

  public async *entriesFrom(startKey: KeysType): AsyncGenerator<{ key: KeysType; value: ValuesType }, void, unknown> {
    if (!this.root) return;
    let leaf = await this.findLeaf(startKey);

    const { cursor } = await leaf.getCursorBeforeKey(startKey);
    const pair = cursor.getKeyValuePairAfter();

    let startIdx = 0;
    if (pair) {
      const idx = leaf.keys.findIndex((k) => k === pair.key);
      startIdx = idx >= 0 ? idx : 0;
    } else {
      startIdx = leaf.keys.length;
    }

    while (leaf) {
      for (let i = startIdx; i < leaf.keys.length; i++) {
        yield { key: leaf.keys[i], value: leaf.values[i] };
      }
      if (!leaf.nextLeaf) break;
      leaf = leaf.nextLeaf;
      startIdx = 0;
    }
  }

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

  public async forEach(callback: (key: KeysType, value: ValuesType) => void | Promise<void>): Promise<void> {
    for await (const { key, value } of this.entries()) {
      await callback(key, value);
    }
  }

  public async clear(): Promise<void> {
    this.root = await this.storage.createTree();
  }

  private async getLeftmostLeaf(): Promise<LeafNodeStorageType> {
    if (!this.root) throw new Error('Tree is not initialized');
    let node: LeafNodeStorageType | InternalNodeStorageType = this.root;
    while (!node.isLeaf) {
      const internal = node;
      node = internal.children[0];
    }
    return Promise.resolve(node);
  }

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
      leftSibling.mergeWithNext(separatorKey, node);
      parent.keys.splice(index - 1, 1);
      parent.children.splice(index, 1);
    } else if (rightSibling) {
      const separatorKey = parent.keys[index];
      node.mergeWithNext(separatorKey, rightSibling);
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

  private async findParent(
    child: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<InternalNodeStorageType | null> {
    if (child === this.root) return null;
    if (this.root.isLeaf) return null;
    return this.findParentRecursive(this.root, child);
  }

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

  private async insertInLeaf(leaf: LeafNodeStorageType, key: KeysType, value: ValuesType): Promise<void> {
    const result = await leaf.getCursorBeforeKey(key);
    const { cursor } = result;
    await cursor.insert(key, value);
  }

  private async splitLeaf(leaf: LeafNodeStorageType): Promise<void> {
    const mid = Math.ceil(leaf.keys.length / 2);
    const newLeaf = this.storage.createLeaf();

    newLeaf.keys = leaf.keys.splice(mid);
    newLeaf.values = leaf.values.splice(mid);

    newLeaf.nextLeaf = leaf.nextLeaf;
    leaf.nextLeaf = newLeaf;

    const promotedKey = newLeaf.keys[0];
    await this.insertInParent(leaf, promotedKey, newLeaf);
  }

  private async splitInternalNode(internal: InternalNodeStorageType): Promise<void> {
    const mid = Math.floor(internal.keys.length / 2);
    const promotedKey = internal.keys[mid];

    const leftKeys = internal.keys.slice(0, mid);
    const rightKeys = internal.keys.slice(mid + 1);

    const leftChildren = internal.children.slice(0, mid + 1);
    const rightChildren = internal.children.slice(mid + 1);

    internal.keys = leftKeys;
    internal.children = leftChildren;

    const newInternal = this.storage.createInternalNode(rightChildren, rightKeys);

    await this.insertInParent(internal, promotedKey, newInternal);
  }

  private async insertInParent(
    node: LeafNodeStorageType | InternalNodeStorageType,
    promotedKey: KeysType,
    newNode: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<void> {
    if (node === this.root) {
      const newRoot = this.storage.createInternalNode([node, newNode], [promotedKey]);
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
