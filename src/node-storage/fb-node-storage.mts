// @author Mathias Bouhon Keulen
// @date 2025-11-13

import type {
  NodeStorage,
  NodeBaseStorage,
  LeafNodeStorage,
  InternalNodeStorage,
  LeafCursor,
  ChildCursor,
} from './node-storage.mjs';
import { FreeBlockFile, NO_BLOCK } from '../freeblockfile.mjs';

type SerializedKey =
  | { type: 'string'; value: string }
  | { type: 'number'; value: number }
  | { type: 'boolean'; value: boolean }
  | { type: 'buffer'; value: string };

function serializeKey(key: unknown): SerializedKey {
  if (Buffer.isBuffer(key) || key instanceof Uint8Array) {
    return { type: 'buffer', value: Buffer.from(key as Uint8Array).toString('base64') };
  }
  const t = typeof key;
  if (t === 'string') return { type: 'string', value: key as string };
  if (t === 'number') return { type: 'number', value: key as number };
  if (t === 'boolean') return { type: 'boolean', value: key as boolean };
  return { type: 'string', value: String(key) };
}

function deserializeKey(serializedKey: SerializedKey): string | number | boolean | Uint8Array {
  if (serializedKey.type === 'buffer') {
    return Buffer.from(serializedKey.value, 'base64');
  }
  return serializedKey.value;
}

type SerializedValue =
  | { t: 'buffer'; value: string }
  | { t: 'json'; value: string }
  | { t: 'string'; value: string }
  | { t: 'number'; value: number }
  | { t: 'boolean'; value: boolean }
  | { t: 'null'; value: null };

function serializeValue(v: unknown): SerializedValue {
  if (Buffer.isBuffer(v) || v instanceof Uint8Array)
    return { t: 'buffer', value: Buffer.from(v as Uint8Array).toString('base64') };
  if (v === null) return { t: 'null', value: null };
  if (typeof v === 'string') return { t: 'string', value: v };
  if (typeof v === 'number') return { t: 'number', value: v };
  if (typeof v === 'boolean') return { t: 'boolean', value: v };
  return { t: 'json', value: JSON.stringify(v) };
}

function deserializeValue(serializedValue: unknown): unknown {
  if (!serializedValue || typeof serializedValue !== 'object') return serializedValue;
  const obj = serializedValue as Record<string, unknown>;
  const t = obj['t'] as string | undefined;
  if (t === 'buffer') return Buffer.from(String(obj['value']), 'base64');
  if (t === 'json') return JSON.parse(String(obj['value']));
  return obj['value'];
}

export class FBNodeStorage<Keystype, ValuesType>
  implements NodeStorage<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  private cache = new Map<number, FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>>();

  constructor(
    public compareKeys: (a: Keystype, b: Keystype) => number,
    public keySize: (key: Keystype) => number,
    public FBfile: FreeBlockFile,
    private maxKeySize: number,
  ) {}

  getMaxKeySize(): number {
    return this.maxKeySize;
  }

  async createTree(): Promise<FBLeafNode<Keystype, ValuesType>> {
    return this.createLeaf();
  }

  createLeaf(): Promise<FBLeafNode<Keystype, ValuesType>> {
    const node = new FBLeafNode<Keystype, ValuesType>(this);
    return Promise.resolve(node);
  }

  createInternalNode(
    children: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[],
    keys: Keystype[],
  ): Promise<FBInternalNode<Keystype, ValuesType>> {
    const childIds = children.map((child) => child.blockId ?? NO_BLOCK);
    const node = new FBInternalNode<Keystype, ValuesType>(this, childIds, keys.slice());
    node.children = children.slice();
    return Promise.resolve(node);
  }

  async allocateInternalNodeStorage(
    children: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[],
    keys: Keystype[],
  ): Promise<FBInternalNode<Keystype, ValuesType>> {
    for (const child of children) {
      if (child.blockId === undefined || child.blockId === NO_BLOCK) {
        if (child.isLeaf) {
          await this.persistLeaf(child);
        } else {
          await this.persistInternal(child);
        }
      }
    }

    const node = await this.createInternalNode(children, keys);
    if (node.blockId === undefined || node.blockId === NO_BLOCK) {
      await this.persistInternal(node);
    }
    return node;
  }

  async persistLeaf(node: FBLeafNode<Keystype, ValuesType>): Promise<void> {
    const payload = {
      type: 'leaf',
      keys: node.keys.map((key) => serializeKey(key)),
      values: node.values.map((value) => serializeValue(value)),
      nextBlockId: node.nextBlockId ?? NO_BLOCK,
      version: 1,
    };
    const buffer = Buffer.from(JSON.stringify(payload), 'utf-8');
    const newBlockId = await this.FBfile.allocateAndWrite(buffer);
    const oldBlockId = node.blockId;
    node.blockId = newBlockId;
    this.cache.set(newBlockId, node);
    if (typeof oldBlockId === 'number' && oldBlockId !== NO_BLOCK && oldBlockId !== newBlockId) {
      try {
        await this.FBfile.freeBlob(oldBlockId);
      } catch (e) {
        console.warn(`failed to free old block id ${oldBlockId}:`, e);
      } finally {
        this.cache.delete(oldBlockId);
      }
    }
  }

  async persistInternal(node: FBInternalNode<Keystype, ValuesType>): Promise<void> {
    if (node.childBlockIds.some((id) => id === NO_BLOCK || id === undefined)) {
      throw new Error('Cannot persist internal node with unpersisted children');
    }

    const payload = {
      type: 'internal',
      keys: node.keys.map((key) => serializeKey(key)),
      childBlockIds: node.childBlockIds.slice(),
      version: 1,
    };
    const buffer = Buffer.from(JSON.stringify(payload), 'utf-8');
    const newBlockId = await this.FBfile.allocateAndWrite(buffer);
    const oldBlockId = node.blockId;
    node.blockId = newBlockId;
    this.cache.set(newBlockId, node);
    if (typeof oldBlockId === 'number' && oldBlockId !== NO_BLOCK && oldBlockId !== newBlockId) {
      try {
        await this.FBfile.freeBlob(oldBlockId);
      } catch (e) {
        console.warn(`failed to free old block id ${oldBlockId}:`, e);
      } finally {
        this.cache.delete(oldBlockId);
      }
    }
  }

  async loadNode(blockId: number): Promise<FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>> {
    if (blockId === NO_BLOCK || blockId === undefined || blockId === null) {
      throw new Error('Cannot load node with NO_BLOCK id');
    }
    if (this.cache.has(blockId)) {
      return this.cache.get(blockId)!;
    }

    const buffer = await this.FBfile.readBlob(blockId);
    if (!buffer) {
      throw new Error(`Block with id ${blockId} not found`);
    }

    const raw = JSON.parse(buffer.toString('utf-8')) as unknown;

    type LeafPayload = { type: 'leaf'; keys: SerializedKey[]; values: SerializedValue[]; nextBlockId?: number };
    type InternalPayload = { type: 'internal'; keys?: SerializedKey[]; childBlockIds?: number[] };

    function isLeafPayload(x: unknown): x is LeafPayload {
      if (typeof x !== 'object' || x === null) return false;
      const obj = x as Record<string, unknown>;
      return obj['type'] === 'leaf' && Array.isArray(obj['keys']) && Array.isArray(obj['values']);
    }

    function isInternalPayload(x: unknown): x is InternalPayload {
      if (typeof x !== 'object' || x === null) return false;
      const obj = x as Record<string, unknown>;
      return obj['type'] === 'internal';
    }

    if (isLeafPayload(raw)) {
      const node = new FBLeafNode<Keystype, ValuesType>(this);
      node.keys = raw.keys.map((serializedKey) => deserializeKey(serializedKey) as Keystype);
      node.values = raw.values.map((sv) => deserializeValue(sv) as ValuesType);
      node.nextBlockId =
        typeof raw.nextBlockId === 'number' && raw.nextBlockId !== NO_BLOCK ? raw.nextBlockId : undefined;
      node.blockId = blockId;
      this.cache.set(blockId, node);
      return node;
    } else if (isInternalPayload(raw)) {
      const rawInternal = raw;
      const childIds = Array.isArray(rawInternal.childBlockIds) ? rawInternal.childBlockIds.slice() : [];
      const keys = Array.isArray(rawInternal.keys) ? rawInternal.keys.map((k) => deserializeKey(k) as Keystype) : [];
      const node = new FBInternalNode<Keystype, ValuesType>(this, childIds, keys);
      node.blockId = blockId;
      node.children = [];
      this.cache.set(blockId, node);

      for (const childId of childIds) {
        if (typeof childId === 'number' && childId !== NO_BLOCK) {
          const childNode = await this.loadNode(childId);
          node.children.push(childNode);
        } else {
          continue;
        }
      }
      return node;
    } else {
      throw new Error('Unknown node type in payload');
    }
  }

  debug_clearCache(): void {
    this.cache.clear();
  }

  deleteCachedBlock(blockId?: number): void {
    if (typeof blockId === 'number' && blockId !== NO_BLOCK) {
      this.cache.delete(blockId);
    }
  }
}

export class FBNodeBase<Keystype, ValuesType>
  implements
    NodeBaseStorage<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  readonly isLeaf!: boolean;

  constructor(protected storage: FBNodeStorage<Keystype, ValuesType>) {}

  public getStorage(): FBNodeStorage<Keystype, ValuesType> {
    return this.storage;
  }

  canMergeWithNext(
    _key: Keystype,
    _nextNode: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>,
  ): boolean {
    return false;
  }

  async mergeWithNext(
    _key: Keystype,
    _nextNode: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>,
  ): Promise<void> {
    return Promise.resolve();
  }
}

export class FBLeafNode<Keystype, ValuesType>
  implements
    LeafNodeStorage<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  readonly isLeaf = true;

  keys: Keystype[] = [];
  values: ValuesType[] = [];
  nextBlockId?: number;
  nextLeaf: FBLeafNode<Keystype, ValuesType> | null = null;

  blockId?: number;

  constructor(private storage: FBNodeStorage<Keystype, ValuesType>) {}

  getStorage(): FBNodeStorage<Keystype, ValuesType> {
    return this.storage;
  }

  getCursorBeforeFirst(): LeafCursor<
    Keystype,
    ValuesType,
    FBLeafNode<Keystype, ValuesType>,
    FBInternalNode<Keystype, ValuesType>
  > {
    return new FBLeafCursor<Keystype, ValuesType>(this, -1);
  }

  getCursorBeforeKey(key: Keystype): {
    cursor: LeafCursor<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>;
    isAtKey: boolean;
  } {
    let index = -1;
    for (let i = 0; i < this.keys.length; i++) {
      if (this.storage.compareKeys(this.keys[i], key) >= 0) {
        index = i;
        break;
      }
    }
    const cursor = new FBLeafCursor<Keystype, ValuesType>(this, index - 1);
    const isAtKey = index >= 0 && this.storage.compareKeys(this.keys[index], key) === 0;
    return { cursor, isAtKey };
  }

  async getNextLeaf(): Promise<FBLeafNode<Keystype, ValuesType> | null> {
    if (this.nextLeaf) return this.nextLeaf;
    if (this.nextBlockId === undefined || this.nextBlockId === NO_BLOCK) return null;
    const nextLeaf = await this.storage.loadNode(this.nextBlockId);
    if (!nextLeaf.isLeaf) throw new Error('Next leaf node is not a leaf');
    this.nextLeaf = nextLeaf;
    return this.nextLeaf;
  }

  canMergeWithNext(
    _key: Keystype,
    nextNode: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>,
  ): boolean {
    if (!nextNode.isLeaf) return false;
    const totalKeys = this.keys.length + (nextNode.keys ? nextNode.keys.length : 0);
    return totalKeys <= this.storage.getMaxKeySize();
  }

  async mergeWithNext(
    _key: Keystype,
    nextNode: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>,
  ): Promise<void> {
    if (!nextNode.isLeaf) {
      throw new Error('Cannot merge with non-leaf node');
    }
    const nextLeaf = nextNode;
    this.keys.push(...nextLeaf.keys);
    this.values.push(...nextLeaf.values);
    this.nextBlockId = nextLeaf.nextBlockId ?? NO_BLOCK;
    this.nextLeaf = nextLeaf.nextLeaf ?? null;

    await this.getStorage().persistLeaf(this);

    if (typeof nextLeaf.blockId === 'number' && nextLeaf.blockId !== NO_BLOCK) {
      try {
        await this.getStorage().FBfile.freeBlob(nextLeaf.blockId);
      } catch (e) {
        console.warn(`failed to free freed next leaf block id ${nextLeaf.blockId}:`, e);
      } finally {
        this.getStorage().deleteCachedBlock(nextLeaf.blockId);
      }
    }
  }
}

export class FBLeafCursor<Keystype, ValuesType>
  implements LeafCursor<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  private position: number;

  constructor(
    public readonly leaf: FBLeafNode<Keystype, ValuesType>,
    position: number = -1,
  ) {
    this.position = position;
  }

  reset(): void {
    this.position = -1;
  }

  isAfterLast(): boolean {
    return this.position + 1 >= this.leaf.keys.length;
  }

  getKeyValuePairAfter(): { key: Keystype; value: ValuesType } {
    const nextIndex = this.position + 1;
    if (nextIndex < 0 || nextIndex >= this.leaf.keys.length) {
      throw new Error('No key/value pair after cursor');
    }
    return { key: this.leaf.keys[nextIndex], value: this.leaf.values[nextIndex] };
  }

  moveNext(): void {
    this.position++;
  }

  movePrev(): void {
    this.position--;
  }

  async insert(
    key: Keystype,
    value: ValuesType,
  ): Promise<{ nodes: FBLeafNode<Keystype, ValuesType>[]; keys: Keystype[] }> {
    let index = -1;
    for (let i = 0; i < this.leaf.keys.length; i++) {
      if (this.leaf.getStorage().compareKeys(this.leaf.keys[i], key) >= 0) {
        index = i;
        break;
      }
    }
    const insertPosition = index === -1 ? this.leaf.keys.length : index;
    this.leaf.keys.splice(insertPosition, 0, key);
    this.leaf.values.splice(insertPosition, 0, value);

    await this.leaf.getStorage().persistLeaf(this.leaf);

    return { nodes: [this.leaf], keys: this.leaf.keys.slice() };
  }

  async removeKeyValuePairAfter(): Promise<void> {
    const removeIndex = this.position + 1;
    if (removeIndex < 0 || removeIndex >= this.leaf.keys.length) {
      throw new Error('No key/value pair to remove after cursor');
    }
    this.leaf.keys.splice(removeIndex, 1);
    this.leaf.values.splice(removeIndex, 1);

    await this.leaf.getStorage().persistLeaf(this.leaf);
  }
}

export class FBInternalNode<Keystype, ValuesType>
  implements
    InternalNodeStorage<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  readonly isLeaf = false;

  keys: Keystype[] = [];
  childBlockIds: number[] = [];
  children: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[] = [];
  blockId?: number;

  constructor(
    private storage: FBNodeStorage<Keystype, ValuesType>,
    childBlockIds: number[] = [],
    keys: Keystype[] = [],
  ) {
    this.childBlockIds = childBlockIds.slice();
    this.keys = keys.slice();
  }

  getStorage(): FBNodeStorage<Keystype, ValuesType> {
    return this.storage;
  }

  async getChildCursorAtFirstChild(): Promise<
    ChildCursor<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
  > {
    const child = new FBChildCursor<Keystype, ValuesType>(this);
    child.setPosition(0);
    return Promise.resolve(child);
  }

  getChildCursorAtKey(key: Keystype): Promise<{
    cursor: ChildCursor<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>;
    isAtKey: boolean;
  }> {
    let index = 0;
    while (index < this.keys.length && this.storage.compareKeys(key, this.keys[index]) >= 0) {
      index++;
    }
    const cursor = new FBChildCursor<Keystype, ValuesType>(this);
    cursor.setPosition(index);
    const isAtKey = index > 0 && this.storage.compareKeys(key, this.keys[index - 1]) === 0;
    return Promise.resolve({ cursor, isAtKey });
  }

  isUnderfull(): boolean {
    const minKeys = Math.floor(this.storage.getMaxKeySize() / 2);
    return this.keys.length < minKeys;
  }

  async deallocateUnderfull(): Promise<FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>> {
    return Promise.resolve(this);
  }

  async moveLastChildTo(separatorKey: Keystype, nextNode: FBInternalNode<Keystype, ValuesType>): Promise<Keystype> {
    const lastChildBlockId = this.childBlockIds.pop()!;
    const lastKey = this.keys.pop()!;
    nextNode.childBlockIds.unshift(lastChildBlockId);
    nextNode.keys.unshift(separatorKey);

    let movedChild: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>;
    if (this.children.length > 0) {
      movedChild = this.children.pop()!;
      nextNode.children.unshift(movedChild);
    }

    await this.storage.persistInternal(this);
    await this.storage.persistInternal(nextNode);
    return lastKey;
  }

  async moveFirstChildTo(
    previousNode: FBInternalNode<Keystype, ValuesType>,
    separatorKey: Keystype,
  ): Promise<Keystype> {
    const firstChildBlockId = this.childBlockIds.shift()!;
    const firstKey = this.keys.shift()!;
    previousNode.childBlockIds.push(firstChildBlockId);
    previousNode.keys.push(separatorKey);

    let movedChild: FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>;
    if (this.children.length > 0) {
      movedChild = this.children.shift()!;
      previousNode.children.push(movedChild);
    }

    await this.storage.persistInternal(this);
    await this.storage.persistInternal(previousNode);
    return firstKey;
  }

  canMergeWithNext(
    _key: Keystype,
    nextNode: FBInternalNode<Keystype, ValuesType> | FBLeafNode<Keystype, ValuesType>,
  ): boolean {
    if (nextNode.isLeaf) return false;
    const totalKeys = this.keys.length + nextNode.keys.length;
    return totalKeys <= this.storage.getMaxKeySize();
  }

  async mergeWithNext(
    _key: Keystype,
    nextNode: FBInternalNode<Keystype, ValuesType> | FBLeafNode<Keystype, ValuesType>,
  ): Promise<void> {
    if (nextNode.isLeaf) {
      throw new Error('Cannot merge with non-internal node');
    }
    const nextInternal = nextNode;
    this.keys.push(_key, ...nextInternal.keys);
    this.childBlockIds.push(...nextInternal.childBlockIds);

    if (nextInternal.children.length > 0) {
      this.children.push(...nextInternal.children);
    }

    await this.getStorage().persistInternal(this);

    if (typeof nextInternal.blockId === 'number' && nextInternal.blockId !== NO_BLOCK) {
      try {
        await this.getStorage().FBfile.freeBlob(nextInternal.blockId);
      } catch (e) {
        console.warn(`failed to free merged next internal block id ${nextInternal.blockId}:`, e);
      } finally {
        this.getStorage().deleteCachedBlock(nextInternal.blockId);
      }
    }
  }
}

export class FBChildCursor<Keystype, ValuesType>
  implements ChildCursor<Keystype, ValuesType, FBLeafNode<Keystype, ValuesType>, FBInternalNode<Keystype, ValuesType>>
{
  private position: number = 0;

  constructor(public readonly parent: FBInternalNode<Keystype, ValuesType>) {}

  setPosition(pos: number): void {
    this.position = pos;
  }

  reset(): void {
    this.position = 0;
  }

  isFirstChild(): boolean {
    return this.position === 0;
  }

  isLastChild(): boolean {
    return this.position === this.parent.childBlockIds.length - 1;
  }

  async getChild(offset: number = 0): Promise<FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>> {
    const targetPosition = this.position + offset;
    const maxChildren = Math.max(this.parent.childBlockIds.length, this.parent.children.length);
    if (targetPosition < 0 || targetPosition >= maxChildren) {
      throw new Error('Child cursor out of bounds');
    }

    const maybeChild = this.parent.children && this.parent.children[targetPosition];
    if (maybeChild) {
      return maybeChild;
    }

    const blockId = this.parent.childBlockIds[targetPosition];
    if (blockId === NO_BLOCK || blockId === undefined || blockId === null) {
      const kidsBlockIds = JSON.stringify(this.parent.childBlockIds);
      const kidsInMemory = JSON.stringify((this.parent.children || []).map((c) => (c ? (c.blockId ?? null) : null)));
      throw new Error(
        `Child absent at position ${targetPosition}: blockId=${String(blockId)}; parent.keys=${JSON.stringify(
          this.parent.keys,
        )}; childBlockIds=${kidsBlockIds}; children.blockIds=${kidsInMemory}`,
      );
    }

    const childNode = await this.parent.getStorage().loadNode(blockId);
    return childNode;
  }

  getKeyAfter(): Keystype {
    if (this.position < 0 || this.position >= this.parent.keys.length) {
      throw new Error('No key after for this child position');
    }
    return this.parent.keys[this.position];
  }

  moveNext(): void {
    this.position++;
  }

  movePrev(): void {
    this.position--;
  }

  async replaceKeysAndChildrenAfterBy(
    count: number,
    replacementKeys: Keystype[],
    replacementChildren: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[],
  ): Promise<{ nodes: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[]; keys: Keystype[] }> {
    const replacementIds: number[] = [];
    for (const child of replacementChildren) {
      if (child.blockId === undefined || child.blockId === NO_BLOCK) {
        if (child.isLeaf) {
          await this.parent.getStorage().persistLeaf(child);
        } else {
          await this.parent.getStorage().persistInternal(child);
        }
      }
      replacementIds.push(child.blockId as number);
    }

    this.parent.keys.splice(this.position, count, ...replacementKeys);
    this.parent.childBlockIds.splice(this.position, count + 1, ...replacementIds);
    this.parent.children.splice(this.position, count + 1, ...replacementChildren);

    await this.parent.getStorage().persistInternal(this.parent);

    const nodes: (FBLeafNode<Keystype, ValuesType> | FBInternalNode<Keystype, ValuesType>)[] = [];
    for (const id of this.parent.childBlockIds) {
      const node = await this.parent.getStorage().loadNode(id);
      nodes.push(node);
    }
    return { nodes, keys: this.parent.keys.slice() };
  }
}
