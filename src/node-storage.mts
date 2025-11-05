// @author Mathias Bouhon Keulen
// @date 2025-11-05

export interface NodeStorage<
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
  readonly compareKeys: (a: KeysType, b: KeysType) => number;
  readonly keySize: (key: KeysType) => number;
  getMaxKeySize(): number;
  createTree(): Promise<LeafNodeStorageType>;
  createLeaf(): LeafNodeStorageType;
  createInternalNode(
    children: (LeafNodeStorageType | InternalNodeStorageType)[],
    keys: KeysType[],
  ): InternalNodeStorageType;

  allocateInternalNodeStorage(
    children: (LeafNodeStorageType | InternalNodeStorageType)[],
    keys: KeysType[],
  ): Promise<InternalNodeStorageType>;
}

export interface NodeBaseStorage<
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
  readonly isLeaf: boolean;
  canMergeWithNext(key: KeysType, nextNode: LeafNodeStorageType | InternalNodeStorageType): boolean;
  mergeWithNext(key: KeysType, nextNode: LeafNodeStorageType | InternalNodeStorageType): void;
}

export interface LeafNodeStorage<
  KeysType,
  ValuesType,
  leafNodeStorageType extends LeafNodeStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>,
  InternalNodeStorageType extends InternalNodeStorage<
    KeysType,
    ValuesType,
    leafNodeStorageType,
    InternalNodeStorageType
  >,
> extends NodeBaseStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType> {
  readonly isLeaf: true;
  keys: KeysType[];
  values: ValuesType[];

  getCursorBeforeFirst(): LeafCursor<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>;
  getCursorBeforeKey(key: KeysType):
    | { cursor: LeafCursor<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>; isAtKey: boolean }
    | PromiseLike<{
        cursor: LeafCursor<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>;
        isAtKey: boolean;
      }>;

  getNextLeaf(): Promise<LeafNodeStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType> | null>;
}

export interface InternalNodeStorage<
  KeysType,
  ValuesType,
  leafNodeStorageType extends LeafNodeStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>,
  InternalNodeStorageType extends InternalNodeStorage<
    KeysType,
    ValuesType,
    leafNodeStorageType,
    InternalNodeStorageType
  >,
> extends NodeBaseStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType> {
  readonly isLeaf: false;
  keys: KeysType[];
  children: (leafNodeStorageType | InternalNodeStorageType)[];
  getChildCursorAtFirstChild(): Promise<
    ChildCursor<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>
  >;
  getChildCursorAtKey(key: KeysType): Promise<{
    cursor: ChildCursor<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>;
    isAtKey: boolean;
  }>;
  isUnderfull(): boolean;
  deallocateUnderfull(): Promise<leafNodeStorageType | InternalNodeStorageType>;
  moveLastChildTo(separatorKey: KeysType, nextNode: InternalNodeStorageType): Promise<KeysType>;
  moveFirstChildTo(previousNode: InternalNodeStorageType, separatorKey: KeysType): Promise<KeysType>;
}

export interface LeafCursor<
  KeysType,
  ValuesType,
  leafNodeStorageType extends LeafNodeStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>,
  InternalNodeStorageType extends InternalNodeStorage<
    KeysType,
    ValuesType,
    leafNodeStorageType,
    InternalNodeStorageType
  >,
> {
  readonly leaf: leafNodeStorageType;
  reset(): void;
  isAfterLast(): boolean;
  getKeyValuePairAfter(): { key: KeysType; value: ValuesType };
  moveNext(): void;
  movePrev(): void;
  insert(key: KeysType, value: ValuesType): Promise<{ nodes: leafNodeStorageType[]; keys: KeysType[] }>;
  removeKeyValuePairAfter(): Promise<void>;
}

export interface ChildCursor<
  KeysType,
  ValuesType,
  leafNodeStorageType extends LeafNodeStorage<KeysType, ValuesType, leafNodeStorageType, InternalNodeStorageType>,
  InternalNodeStorageType extends InternalNodeStorage<
    KeysType,
    ValuesType,
    leafNodeStorageType,
    InternalNodeStorageType
  >,
> {
  reset(): void;
  isFirstChild(): boolean;
  isLastChild(): boolean;
  getChild(offset?: number): Promise<leafNodeStorageType | InternalNodeStorageType>;
  getKeyAfter(): KeysType;
  moveNext(): void;
  movePrev(): void;
  replaceKeysAndChildrenAfterBy(
    count: number,
    replacementKeys: KeysType[],
    replacementChildren: (leafNodeStorageType | InternalNodeStorageType)[],
  ): Promise<{ nodes: (leafNodeStorageType | InternalNodeStorageType)[]; keys: KeysType[] }>;
}
