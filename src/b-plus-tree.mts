// @author Mathias Bouhon Keulen
// @date 2025-11-06

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
    this.storage = storage;
    this.order = order;
  }

  async init() {
    this.root = await this.storage.createTree();
  }

  async insert(key: KeysType, value: ValuesType): Promise<void> {
    const leaf = await this.findLeaf(key);
    await this.insertInLeaf(leaf, key, value);

    if (leaf.keys.length > this.order) {
      await this.splitLeaf(leaf);
    }
  }

  async search(_key: KeysType): Promise<ValuesType | null> {
    // Search logic to be implemented
    return Promise.resolve(null);
  }

  async delete(_key: KeysType): Promise<void> {
    // Deletion logic to be implemented
  }

  async printTree(): Promise<void> {
    // Tree printing logic to be implemented
  }

  private async findLeaf(key: KeysType): Promise<LeafNodeStorageType> {
    let node: LeafNodeStorageType | InternalNodeStorageType = this.root;
    console.log('Starting findLeaf at root:', node);

    while (!node.isLeaf) {
      console.log('Descending into internal node:', node);
      const internalNode = node;
      const { cursor } = await internalNode.getChildCursorAtKey(key);
      node = await cursor.getChild();
      console.log('Moved to child node:', node);
      if (!node) {
        throw new Error('Child node not found');
      }
    }

    return node;
  }

  private async findParent(
    child: LeafNodeStorageType | InternalNodeStorageType,
  ): Promise<InternalNodeStorageType | null> {
    if (child === this.root) return null;

    return this.findParentRecursive(this.root as InternalNodeStorageType, child);
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
    const { cursor } = await leaf.getCursorBeforeKey(key);
    await cursor.insert(key, value);
  }

  private async splitLeaf(leaf: LeafNodeStorageType): Promise<void> {
    const mid = Math.floor(leaf.keys.length / 2);
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
    const newInternal = this.storage.createInternalNode([], []);
    const promotedKey = internal.keys[mid];

    newInternal.keys = internal.keys.splice(mid + 1);
    newInternal.children = internal.children.splice(mid + 1);
    internal.keys.splice(mid, 1);
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
    if (!parent) {
      throw new Error('Parent not found');
    }

    const { cursor } = await parent.getChildCursorAtKey(promotedKey);

    await cursor.replaceKeysAndChildrenAfterBy(0, [promotedKey], [newNode]);

    if (parent.keys.length > this.order) {
      await this.splitInternalNode(parent);
    }
  }
}
