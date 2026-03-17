// @author MaartenHaine, Jari Daemen, Frederick Hillen
// used Claude for debugging
// @date 2026-02-11

import { BPlusTree } from './b-plus-tree.mjs';
import { FBNodeStorage, FBLeafNode, FBInternalNode } from './node-storage/fb-node-storage.mjs';
import { FreeBlockFile, DEFAULT_BLOCK_SIZE, NO_BLOCK } from './freeblockfile.mjs';
import { AtomicFileImpl } from './atomic-operations/atomic-file.mjs';
import { WALManagerImpl } from './atomic-operations/wal-manager.mjs';
import { type File } from './file/file.mjs';
import { randomUUID } from 'crypto';

// Document interface
// shows all valid data types for the document
export type DocumentValue =
  | string
  | number
  | boolean
  | null
  | bigint
  | DocumentValue[]
  | { [key: string]: DocumentValue };

// Gives a documetn its own id. It is an interface that can be easily given as input
// and as interface to implemented.
export interface Document {
  id: string;
  [key: string]: DocumentValue;
}

// Filter operators interface
export interface FilterOperators {
  [field: string]: {
    $eq?: DocumentValue;
    $ne?: DocumentValue;
    $gt?: DocumentValue;
    $gte?: DocumentValue;
    $lt?: DocumentValue;
    $lte?: DocumentValue;
    $in?: DocumentValue[];
    $nin?: DocumentValue[];
    $includes?: string;
  };
}

// Aggregation query interface
export interface AggregateQuery {
  groupBy: string;
  operations: {
    count?: string;
    sum?: { field: string; as: string }[];
    avg?: { field: string; as: string }[];
    min?: { field: string; as: string }[];
    max?: { field: string; as: string }[];
  };
}

// Join query interface
export interface JoinQuery {
  collection: string;
  on: string;
  rightOn?: string;
}

// Query options interface
export interface Query {
  filter?: (doc: Document) => boolean;
  filterOps?: FilterOperators;
  sort?: { field: string; order: 'asc' | 'desc' };
  skip?: number;
  limit?: number;
  idRange?: { min?: string; max?: string };
  projection?: string[];
  aggregate?: AggregateQuery;
  join?: JoinQuery;
}

/**
 * Serializes a field value for use as a B+ Tree key.
 * Ensures proper ordering: numbers, strings, booleans, null, bigint.
 *
 * @param {unknown} value The value to serialize
 * @returns {string} A string representation that maintains sort order
 */
export function serializeFieldValue(value: unknown): string {
  if (value === null || value === undefined) return 'null';

  if (typeof value === 'boolean') {
    return value ? 'boolT' : 'boolF';
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      if (Number.isNaN(value)) return 'num:NaN';
      return value === Infinity ? 'num:+Inf' : 'num:-Inf';
    }
    const sign = value < 0 ? '-' : '+';
    const abs = Math.abs(value);
    const intPart = Math.floor(abs).toString().padStart(16, '0');
    const fracPart = Math.round((abs - Math.floor(abs)) * 1e8)
      .toString()
      .padStart(8, '0');
    return `num:${sign}${intPart}.${fracPart}`;
  }

  if (typeof value === 'bigint') {
    const sign = value < 0n ? '-' : '+';
    const abs = value < 0n ? -value : value;
    return `bigint:${sign}${abs.toString().padStart(20, '0')}`;
  }

  // Default to string
  if (typeof value === 'object' && value !== null) {
    return `str:${JSON.stringify(value)}`;
  }
  if (value === undefined) {
    return 'str:undefined';
  }
  // For primitive types, convert to string
  return `str:${value as string | number | boolean | bigint}`;
}

/**
 * Deserializes a field value from its B+ Tree key representation.
 *
 * @param {string} serialized The serialized string
 * @returns {DocumentValue} The original value
 */
export function deserializeFieldValue(serialized: string): DocumentValue {
  if (serialized === '' || serialized === 'null') return null;

  if (serialized.startsWith('bool')) {
    return serialized === 'boolT';
  }

  if (serialized.startsWith('num:')) {
    const body = serialized.substring(4);
    if (body === 'NaN') return NaN;
    if (body === '+Inf') return Infinity;
    if (body === '-Inf') return -Infinity;

    const sign = body[0];
    const rest = body.substring(1);
    const [intPart, fracPart = '0'] = rest.split('.');
    const num = Number((sign === '-' ? '-' : '') + Number(intPart) + '.' + (fracPart || '0'));
    return num;
  }

  if (serialized.startsWith('bigint:')) {
    try {
      const body = serialized.substring(7);
      const sign = body[0];
      const rest = body.substring(1);
      const value = BigInt(rest);
      return sign === '-' ? -value : value;
    } catch {
      return null;
    }
  }

  if (serialized.startsWith('str:')) {
    const strValue = serialized.substring(4);
    try {
      const parsed: unknown = JSON.parse(strValue);
      // Check if it was originally an object/array
      if (typeof parsed === 'object' && parsed !== null) {
        return parsed as DocumentValue;
      }
    } catch {
      // Not a JSON string, return as is
    }
    return strValue;
  }

  return serialized;
}

/**
 * Helper: check if field is indexable
 * @param {string} fieldName The field name to check
 * @returns {boolean} True if the field is indexable
 */
export function isIndexableField(fieldName: string): boolean {
  return !fieldName.startsWith('_') && fieldName !== 'id';
}

//TODO: Make it so you can construct a collection without needing to give a BPlusTree as an input.
// Maybe make a seperate function for that.
// Collection class with secondary index support
export class Collection {
  private documentHeap: FreeBlockFile;

  // Indexes: field name -> B+ Tree
  // The 'id' index is guaranteed to exist and serves as the primary index
  private indexes: Map<string, BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>> =
    new Map();

  private onChangeCallback?: () => Promise<void>;
  private createIndexStorage?: () => FBNodeStorage<string, number>;
  private onIndexCreated?: (fieldName: string, rootBlockId: number) => Promise<void>;
  private onDocumentCountChanged?: (documentCount: number) => Promise<void>;
  private cachedDocumentCount: number | null = null;

  constructor(
    documentHeap: FreeBlockFile,
    primaryIndexTree: BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>,
    onChangeCallback?: () => Promise<void>,
    createIndexStorage?: () => FBNodeStorage<string, number>,
    onIndexCreated?: (fieldName: string, rootBlockId: number) => Promise<void>,
    onDocumentCountChanged?: (documentCount: number) => Promise<void>,
    initialDocumentCount?: number,
  ) {
    this.documentHeap = documentHeap;
    this.indexes.set('id', primaryIndexTree);
    this.onChangeCallback = onChangeCallback;
    this.createIndexStorage = createIndexStorage;
    this.onIndexCreated = onIndexCreated;
    this.onDocumentCountChanged = onDocumentCountChanged;
    this.cachedDocumentCount = initialDocumentCount ?? null;
  }

  //TODO: check if nodestorage changes affected this function.
  /**
   * Creates a secondary index on a field.
   * @param {string} fieldName The field to index
   * @param {FBNodeStorage<string, string>} storage The storage to use for the index B+ Tree
   * @throws {Error} If the field is not indexable (starts with _ or is 'id')
   * @returns {Promise<void>} A promise that resolves when the index is created
   */
  async createIndex(fieldName: string, storage: FBNodeStorage<string, number>): Promise<void> {
    if (!isIndexableField(fieldName)) {
      throw new Error(`Field ${fieldName} cannot be indexed (starts with _ or is 'id')`);
    }

    if (this.indexes.has(fieldName)) {
      throw new Error(`Index already exists for field: ${fieldName}`);
    }

    const indexTree = new BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>(
      storage,
      50,
    );
    await indexTree.init();

    const primaryTree = this.indexes.get('id')!;
    for await (const { key: docId, value: startBlockId } of primaryTree.entries()) {
      const docBuffer = await this.documentHeap.readBlob(startBlockId);
      if (docBuffer.length > 0) {
        const doc = JSON.parse(docBuffer.toString()) as Document;
        const fieldValue = doc[fieldName];
        if (fieldValue !== undefined && fieldValue !== null) {
          const indexKey = serializeFieldValue(fieldValue) + ':' + docId;
          await indexTree.insert(indexKey, startBlockId);
        }
      }
    }

    this.indexes.set(fieldName, indexTree);

    // checking if this has the method onIndexCreated
    if (this.onIndexCreated) {
      const root = indexTree.getRoot();
      if (root.blockId === undefined || root.blockId === NO_BLOCK) {
        if (root.isLeaf) {
          await storage.persistLeaf(root);
        } else {
          await storage.persistInternal(root);
        }
      }
      await this.onIndexCreated(fieldName, root.blockId!);
    }

    // checking if this had the method onChangeCallback
    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }
  }

  /**
   * Drops a secondary index.
   * @param {string} fieldName The field to drop the index for
   * @returns {Promise<void>} A promise that resolves when the index is dropped
   */
  async dropIndex(fieldName: string): Promise<void> {
    if (fieldName === 'id') {
      throw new Error('Cannot drop the primary ID index');
    }
    if (!this.indexes.has(fieldName)) {
      throw new Error(`Index does not exist for field: ${fieldName}`);
    }
    this.indexes.delete(fieldName);

    // checking if this had the method onChangeCallback
    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }
  }

  /**
   * Gets the Document Heap
   */
  getDocumentHeap() {
    return this.documentHeap;
  }

  /**
   * Gets the list of indexed fields.
   */
  getIndexedFields(): string[] {
    return Array.from(this.indexes.keys()).filter((field) => field !== 'id');
  }

  /**
   * Gets a secondary index tree for a field.
   */
  getIndex(
    fieldName: string,
  ): BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>> | undefined {
    return this.indexes.get(fieldName);
  }

  /**
   * Sets secondary indexes (used when loading from disk).
   */
  setIndexes(
    indexes: Map<string, BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>>,
  ): void {
    const primaryTree = this.indexes.get('id');
    this.indexes = indexes;
    // ensure 'id' is always preserved or updated
    if (primaryTree !== undefined && !this.indexes.has('id')) {
      this.indexes.set('id', primaryTree);
    }
  }

  /**
   * Inserts a document into the collection.
   * If the document does not have an id, one will be generated.
   * @param {Omit<Document, 'id'> & { id?: string }} doc The document to insert.
   * @returns {Promise<Document>} The inserted document.
   */
  async insert(doc: Omit<Document, 'id'> & { id?: string }): Promise<Document> {
    const id = doc.id || randomUUID();
    const newDoc: Document = JSON.parse(JSON.stringify({ ...doc, id })) as Document;
    const docBuffer = Buffer.from(JSON.stringify(newDoc));

    // Allocate space and write document to the heap
    const startBlockId = await this.documentHeap.allocateAndWrite(docBuffer);

    // Insert into all indexes, including 'id'
    const newlyCreatedIndexes = new Set<string>();

    for (const [key, value] of Object.entries(newDoc)) {
      if (value !== undefined && value !== null) {
        if (isIndexableField(key) && !this.indexes.has(key)) {
          // checks if this has the createIndexStorage method
          if (this.createIndexStorage) {
            await this.createIndex(key, this.createIndexStorage());
            newlyCreatedIndexes.add(key);
          }
        }
      }
    }

    // Insert to all available indexes
    for (const [fieldName, indexTree] of this.indexes.entries()) {
      if (fieldName === 'id') {
        await indexTree.insert(id, startBlockId);
        continue;
      }

      const fieldValue = newDoc[fieldName];
      if (fieldValue !== undefined && fieldValue !== null) {
        const indexKey = serializeFieldValue(fieldValue) + ':' + id;
        await indexTree.insert(indexKey, startBlockId);
      }
    }

    // checks if this has the method onChangeCallback
    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }

    if (this.cachedDocumentCount === null) {
      this.cachedDocumentCount = 1;
    } else {
      this.cachedDocumentCount++;
    }

    // checks if this has the method onDocumentCountChanged
    if (this.onDocumentCountChanged) {
      await this.onDocumentCountChanged(this.cachedDocumentCount);
    }

    return newDoc;
  }

  /**
   * Applies filter operators to get matching document IDs using indexes.
   * @param {FilterOperators} filterOps The filter operators to apply.
   * @returns {Promise<Set<string> | null>} A set of document IDs that match the query, or null if no index is available.
   */
  private async applyFilterOps(filterOps: FilterOperators): Promise<Set<number> | null> {
    const indexedFields: Array<{ field: string; ops: FilterOperators[string]; score: number }> = [];

    for (const [field, ops] of Object.entries(filterOps)) {
      if (!this.indexes.has(field) || field === 'id') continue;

      let score = Infinity;
      if (ops.$eq !== undefined) score = 1;
      else if (ops.$in !== undefined) score = ops.$in.length;
      else if (ops.$gt !== undefined || ops.$gte !== undefined || ops.$lt !== undefined || ops.$lte !== undefined) {
        score = 100;
      } else {
        continue;
      }

      indexedFields.push({ field, ops, score });
    }

    if (indexedFields.length === 0) return null;
    indexedFields.sort((a, b) => a.score - b.score); // ASC

    const matchesOpsOnValue = (value: DocumentValue | undefined, ops: FilterOperators[string]): boolean => {
      if (ops.$eq !== undefined && value !== ops.$eq) return false;
      if (ops.$in !== undefined && !ops.$in.includes(value as DocumentValue)) return false;

      if (ops.$gt !== undefined) {
        if (value === null || value === undefined) return false;
        if (!((value as unknown as number) > (ops.$gt as unknown as number))) return false;
      }
      if (ops.$gte !== undefined) {
        if (value === null || value === undefined) return false;
        if (!((value as unknown as number) >= (ops.$gte as unknown as number))) return false;
      }
      if (ops.$lt !== undefined) {
        if (value === null || value === undefined) return false;
        if (!((value as unknown as number) < (ops.$lt as unknown as number))) return false;
      }
      if (ops.$lte !== undefined) {
        if (value === null || value === undefined) return false;
        if (!((value as unknown as number) <= (ops.$lte as unknown as number))) return false;
      }

      return true;
    };

    const collectInitialPointers = async (
      indexTree: BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>,
      ops: FilterOperators[string],
    ): Promise<Set<number> | null> => {
      const pointers = new Set<number>();

      if (ops.$eq !== undefined) {
        const prefix = serializeFieldValue(ops.$eq) + ':';
        for await (const { value: startBlockId } of indexTree.range(prefix, prefix + '\uffff', {
          inclusiveStart: true,
          inclusiveEnd: true,
        })) {
          pointers.add(startBlockId);
        }
        return pointers;
      }

      if (ops.$gt !== undefined || ops.$gte !== undefined || ops.$lt !== undefined || ops.$lte !== undefined) {
        const minVal = ops.$gt !== undefined ? ops.$gt : ops.$gte;
        const maxVal = ops.$lt !== undefined ? ops.$lt : ops.$lte;
        const minInclusive = ops.$gte !== undefined;
        const maxInclusive = ops.$lte !== undefined;

        const startKey = minVal !== undefined && minVal !== null ? serializeFieldValue(minVal) + ':' : '';
        const endKey = maxVal !== undefined && maxVal !== null ? serializeFieldValue(maxVal) + ':\uffff' : '\uffff';

        for await (const { value: startBlockId } of indexTree.range(startKey, endKey, {
          inclusiveStart: minInclusive,
          inclusiveEnd: maxInclusive,
        })) {
          pointers.add(startBlockId);
        }
        return pointers;
      }

      if (ops.$in !== undefined) {
        const sortedValues = ops.$in
          .map((v) => ({ serialized: serializeFieldValue(v) }))
          .sort((a, b) => (a.serialized < b.serialized ? -1 : a.serialized > b.serialized ? 1 : 0)); // ASC

        if (sortedValues.length === 0) return pointers;

        const firstKey = sortedValues[0].serialized + ':';
        const lastKey = sortedValues[sortedValues.length - 1].serialized + ':\uffff';

        let valueIndex = 0;
        for await (const { key, value: startBlockId } of indexTree.range(firstKey, lastKey, {
          inclusiveStart: true,
          inclusiveEnd: true,
        })) {
          const colonIndex = key.lastIndexOf(':');
          const serializedValue = key.substring(0, colonIndex);

          while (valueIndex < sortedValues.length && sortedValues[valueIndex].serialized < serializedValue) {
            valueIndex++;
          }

          if (valueIndex < sortedValues.length && sortedValues[valueIndex].serialized === serializedValue) {
            pointers.add(startBlockId);
          }

          if (valueIndex >= sortedValues.length) break;
        }
      } else if (ops.$includes !== undefined && Object.keys(ops).length === 1) {
        // If $includes is the only operator, index scan is useless, skip index use for this field
        return null;
      }

      return pointers;
    };

    const [first, ...rest] = indexedFields;
    const firstIndex = this.indexes.get(first.field)!;
    let resultSet = await collectInitialPointers(firstIndex, first.ops);

    if (resultSet === null) return null;
    if (resultSet.size === 0) return resultSet;

    const docCache = new Map<number, Document | null>();

    for (const { field, ops } of rest) {
      const previousSize = resultSet.size;
      const nextSet = new Set<number>();

      for (const startBlockId of resultSet) {
        let doc = docCache.get(startBlockId);
        if (doc === undefined) {
          const docBuffer = await this.documentHeap.readBlob(startBlockId);
          if (docBuffer.length > 0) {
            doc = JSON.parse(docBuffer.toString()) as Document;
          } else {
            doc = null;
          }
          docCache.set(startBlockId, doc);
        }

        if (doc === null) continue;
        if (matchesOpsOnValue(doc[field], ops)) {
          nextSet.add(startBlockId);
        }
      }

      resultSet = nextSet;
      if (resultSet.size === 0) break;

      if (previousSize > 0 && resultSet.size / previousSize < 0.9) break;
    }

    return resultSet;
  }

  private async getCachedDocumentCount(): Promise<number> {
    if (this.cachedDocumentCount !== null) {
      return this.cachedDocumentCount;
    }

    let count = 0;
    for await (const entry of this.indexes.get('id')!.entries()) {
      void entry;
      count++;
    }

    this.cachedDocumentCount = count;
    return count;
  }

  /**
   * Finds documents in the collection.
   * @param {Query} query The query options.
   * @returns {Promise<Document[]>} An array of documents matching the query.
   */
  async find(query: Query = {}): Promise<Document[]> {
    let results: Document[] = [];
    let candidatePointers: Set<number> | null = null;
    const primaryTree = this.indexes.get('id')!;

    // Step 1: Use filter operators with indexes if available
    if (query.filterOps !== undefined) {
      candidatePointers = await this.applyFilterOps(query.filterOps);

      if (candidatePointers !== null) {
        const estimatedTotalDocs = await this.getCachedDocumentCount();
        const totalDocs = Math.max(1, estimatedTotalDocs);
        const candidateRatio = candidatePointers.size / totalDocs;
        const scanRatio = 1 - candidateRatio;

        if (candidateRatio <= scanRatio) {
          for (const startBlockId of candidatePointers) {
            const docBuffer = await this.documentHeap.readBlob(startBlockId);
            if (docBuffer.length > 0) {
              const doc = JSON.parse(docBuffer.toString()) as Document;
              let matches = true;
              for (const [field, ops] of Object.entries(query.filterOps)) {
                const value = doc[field];
                if (ops.$eq !== undefined && value !== ops.$eq) matches = false;
                if (ops.$ne !== undefined && value === ops.$ne) matches = false;
                if (ops.$gt !== undefined && ops.$gt !== null) {
                  if (typeof ops.$gt === 'string' || typeof value === 'string') {
                    throw new Error(
                      `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                    );
                  }
                  if (value !== null && !((value as unknown as number) > (ops.$gt as unknown as number)))
                    matches = false;
                }
                if (ops.$gte !== undefined && ops.$gte !== null) {
                  if (typeof ops.$gte === 'string' || typeof value === 'string') {
                    throw new Error(
                      `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                    );
                  }
                  if (value !== null && !((value as unknown as number) >= (ops.$gte as unknown as number)))
                    matches = false;
                }
                if (ops.$lt !== undefined && ops.$lt !== null) {
                  if (typeof ops.$lt === 'string' || typeof value === 'string') {
                    throw new Error(
                      `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                    );
                  }
                  if (value !== null && !((value as unknown as number) < (ops.$lt as unknown as number)))
                    matches = false;
                }
                if (ops.$lte !== undefined && ops.$lte !== null) {
                  if (typeof ops.$lte === 'string' || typeof value === 'string') {
                    throw new Error(
                      `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                    );
                  }
                  if (value !== null && !((value as unknown as number) <= (ops.$lte as unknown as number)))
                    matches = false;
                }
                if (ops.$in !== undefined && !ops.$in.includes(value)) matches = false;
                if (ops.$nin !== undefined && ops.$nin.includes(value)) matches = false;
                if (ops.$includes !== undefined && (typeof value !== 'string' || !value.includes(ops.$includes)))
                  matches = false;
              }

              if (matches && (!query.filter || query.filter(doc))) {
                results.push(doc);
              }
            }
          }
        } else {
          for await (const { value: startBlockId } of primaryTree.entries()) {
            if (!candidatePointers.has(startBlockId)) continue;

            const docBuffer = await this.documentHeap.readBlob(startBlockId);
            if (docBuffer.length === 0) continue;
            const doc = JSON.parse(docBuffer.toString()) as Document;

            let matches = true;
            for (const [field, ops] of Object.entries(query.filterOps)) {
              const value = doc[field];
              if (ops.$eq !== undefined && value !== ops.$eq) matches = false;
              if (ops.$ne !== undefined && value === ops.$ne) matches = false;
              if (ops.$gt !== undefined && ops.$gt !== null) {
                if (typeof ops.$gt === 'string' || typeof value === 'string') {
                  throw new Error(
                    `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                  );
                }
                if (value !== null && !((value as unknown as number) > (ops.$gt as unknown as number))) matches = false;
              }
              if (ops.$gte !== undefined && ops.$gte !== null) {
                if (typeof ops.$gte === 'string' || typeof value === 'string') {
                  throw new Error(
                    `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                  );
                }
                if (value !== null && !((value as unknown as number) >= (ops.$gte as unknown as number)))
                  matches = false;
              }
              if (ops.$lt !== undefined && ops.$lt !== null) {
                if (typeof ops.$lt === 'string' || typeof value === 'string') {
                  throw new Error(
                    `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                  );
                }
                if (value !== null && !((value as unknown as number) < (ops.$lt as unknown as number))) matches = false;
              }
              if (ops.$lte !== undefined && ops.$lte !== null) {
                if (typeof ops.$lte === 'string' || typeof value === 'string') {
                  throw new Error(
                    `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
                  );
                }
                if (value !== null && !((value as unknown as number) <= (ops.$lte as unknown as number)))
                  matches = false;
              }
              if (ops.$in !== undefined && !ops.$in.includes(value)) matches = false;
              if (ops.$nin !== undefined && ops.$nin.includes(value)) matches = false;
              if (ops.$includes !== undefined && (typeof value !== 'string' || !value.includes(ops.$includes)))
                matches = false;
            }

            if (matches && (!query.filter || query.filter(doc))) {
              results.push(doc);
            }
          }
        }

        results = this.applyProjection(results, query.projection);
        results = this.applySorting(results, query.sort);
        return this.applyPagination(results, query.skip, query.limit);
      }
    }

    // Step 2: if no index was used
    let iterator: AsyncGenerator<{ key: string; value: number }, void, unknown>;

    if (query.idRange !== undefined) {
      const { min, max } = query.idRange;

      if (min !== undefined && max !== undefined) {
        iterator = primaryTree.range(min, max, {
          inclusiveStart: true,
          inclusiveEnd: true,
        });
      } else if (min !== undefined) {
        iterator = primaryTree.entriesFrom(min);
      } else if (max !== undefined) {
        iterator = primaryTree.entries();
        for await (const { key, value: startBlockId } of iterator) {
          if (key > max) break;
          const docBuffer = await this.documentHeap.readBlob(startBlockId);
          if (docBuffer.length > 0) {
            const doc = JSON.parse(docBuffer.toString()) as Document;
            if (!query.filter || query.filter(doc)) {
              results.push(doc);
            }
          }
        }

        results = this.applyProjection(results, query.projection);
        results = this.applySorting(results, query.sort);
        return this.applyPagination(results, query.skip, query.limit);
      } else {
        iterator = primaryTree.entries();
      }

      if (min !== undefined || max !== undefined) {
        for await (const { value: startBlockId } of iterator) {
          const docBuffer = await this.documentHeap.readBlob(startBlockId);
          if (docBuffer.length > 0) {
            const doc = JSON.parse(docBuffer.toString()) as Document;
            if (!query.filter || query.filter(doc)) {
              results.push(doc);
            }
          }
        }

        results = this.applyProjection(results, query.projection);
        results = this.applySorting(results, query.sort);
        return this.applyPagination(results, query.skip, query.limit);
      }
    }

    if (!query.filter && !query.filterOps && query.sort?.field === 'id' && query.sort.order === 'asc') {
      iterator = primaryTree.entries();

      let count = 0;
      const start = query.skip ?? 0;
      const limit = query.limit ?? Infinity;

      for await (const { value: startBlockId } of iterator) {
        if (count >= start && count < start + limit) {
          const docBuffer = await this.documentHeap.readBlob(startBlockId);
          if (docBuffer.length > 0) {
            results.push(JSON.parse(docBuffer.toString()) as Document);
          }
        }
        count++;
        if (count >= start + limit) break;
      }

      return this.applyProjection(results, query.projection);
    }

    // Handle descending ID sort
    if (query.sort?.field === 'id' && query.sort.order === 'desc') {
      const all: Document[] = [];
      for await (const { value: startBlockId } of primaryTree.reverseEntries()) {
        const docBuffer = await this.documentHeap.readBlob(startBlockId);
        if (docBuffer.length > 0) {
          const doc = JSON.parse(docBuffer.toString()) as Document;
          if (query.filter === undefined || query.filter(doc)) {
            all.push(doc);
          }
        }
      }
      // all.reverse();

      results = this.applyProjection(all, query.projection);
      return this.applyPagination(results, query.skip, query.limit);
    }

    // Full scan with filter
    iterator = primaryTree.entries();
    for await (const { value: startBlockId } of iterator) {
      const docBuffer = await this.documentHeap.readBlob(startBlockId);
      if (docBuffer.length === 0) continue;
      const doc = JSON.parse(docBuffer.toString()) as Document;

      if (query.filter !== undefined && !query.filter(doc)) {
        continue;
      }

      if (query.filterOps !== undefined) {
        let matches = true;
        for (const [field, ops] of Object.entries(query.filterOps)) {
          const docValue = doc[field] as number | string | boolean | null;
          if (ops.$eq !== undefined && docValue !== ops.$eq) matches = false;
          if (ops.$ne !== undefined && docValue === ops.$ne) matches = false;
          if (ops.$gt !== undefined && ops.$gt !== null) {
            if (typeof ops.$gt === 'string' || typeof docValue === 'string') {
              throw new Error(
                `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
              );
            }
            if (docValue !== null && !((docValue as number) > (ops.$gt as unknown as number))) matches = false;
          }
          if (ops.$gte !== undefined && ops.$gte !== null) {
            if (typeof ops.$gte === 'string' || typeof docValue === 'string') {
              throw new Error(
                `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
              );
            }
            if (docValue !== null && !((docValue as number) >= (ops.$gte as unknown as number))) matches = false;
          }
          if (ops.$lt !== undefined && ops.$lt !== null) {
            if (typeof ops.$lt === 'string' || typeof docValue === 'string') {
              throw new Error(
                `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
              );
            }
            if (docValue !== null && !((docValue as number) < (ops.$lt as unknown as number))) matches = false;
          }
          if (ops.$lte !== undefined && ops.$lte !== null) {
            if (typeof ops.$lte === 'string' || typeof docValue === 'string') {
              throw new Error(
                `Comparison operators ($gt, $lt, etc.) are only supported for numbers. Attempted to compare string.`,
              );
            }
            if (docValue !== null && !((docValue as number) <= (ops.$lte as unknown as number))) matches = false;
          }
          if (ops.$in !== undefined && !ops.$in.includes(docValue as Exclude<DocumentValue, object>)) matches = false;
          if (ops.$nin !== undefined && ops.$nin.includes(docValue as Exclude<DocumentValue, object>)) matches = false;
          if (ops.$includes !== undefined && (typeof docValue !== 'string' || !docValue.includes(ops.$includes)))
            matches = false;
        }

        if (!matches) {
          continue;
        }
      }

      results.push(doc);
    }

    results = this.applyProjection(results, query.projection);
    results = this.applySorting(results, query.sort);
    return this.applyPagination(results, query.skip, query.limit);
  }

  /**
   * Performs aggregation on the collection.
   * Uses secondary indexes when available for efficient grouping (O(log n + k)).
   * @param {object} options The aggregation options.
   * @param {string} options.groupBy The field to group by.
   * @param {object} options.operations The aggregation operations to perform.
   * @param {string} [options.operations.count] Optional field name to store count.
   * @param {Array<{ field: string; as: string }>} [options.operations.sum] Optional sum operations.
   * @param {Array<{ field: string; as: string }>} [options.operations.avg] Optional average operations.
   * @param {Array<{ field: string; as: string }>} [options.operations.min] Optional min operations.
   * @param {Array<{ field: string; as: string }>} [options.operations.max] Optional max operations.
   * @param {(doc: Document) => boolean} [options.filter] Optional filter function to apply before aggregation.
   * @returns {Promise<Document[]>} The aggregation results.
   */
  async aggregate(options: {
    groupBy?: string | null;
    operations: {
      count?: string;
      sum?: Array<{ field: string; as: string }>;
      avg?: Array<{ field: string; as: string }>;
      min?: Array<{ field: string; as: string }>;
      max?: Array<{ field: string; as: string }>;
    };
    filter?: (doc: Document) => boolean;
  }): Promise<Document[]> {
    const { groupBy, operations, filter } = options;
    const groups = new Map<DocumentValue, Document[]>();
    const primaryTree = this.indexes.get('id')!;

    // Use secondary index if available fr grouping
    let iterator: AsyncIterable<{ key: string; value: number }>;

    if (groupBy && this.indexes.has(groupBy) && groupBy !== 'id') {
      const indexTree = this.indexes.get(groupBy)!;
      iterator = indexTree.entries();

      for await (const { value: startBlockId } of iterator) {
        const docBuffer = await this.documentHeap.readBlob(startBlockId);
        if (docBuffer.length === 0) continue;
        const doc = JSON.parse(docBuffer.toString()) as Document;
        if (filter !== undefined && !filter(doc)) continue;

        const groupValue = doc[groupBy];
        const groupKey = typeof groupValue === 'object' ? JSON.stringify(groupValue) : String(groupValue);

        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(doc);
      }
    } else {
      // Full scan
      for await (const { value: startBlockId } of primaryTree.entries()) {
        const docBuffer = await this.documentHeap.readBlob(startBlockId);
        if (docBuffer.length === 0) continue;
        const doc = JSON.parse(docBuffer.toString()) as Document;
        if (filter !== undefined && !filter(doc)) continue;

        const groupValue = groupBy ? doc[groupBy] : '_all_';
        const groupKey =
          typeof groupValue === 'object' && groupValue !== null ? JSON.stringify(groupValue) : String(groupValue);

        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(doc);
      }
    }

    // Compute aggregations for each group
    const results: Document[] = [];
    for (const [groupKey, docs] of groups.entries()) {
      const result: Document = groupBy
        ? { id: `group_${groupKey as string}`, [groupBy]: docs[0][groupBy] }
        : { id: `group_all` };

      if (operations.count !== undefined) {
        result[operations.count] = docs.length;
      }

      if (operations.sum !== undefined) {
        for (const { field, as } of operations.sum) {
          result[as] = docs.reduce((sum, doc) => {
            const val = doc[field];
            return sum + (typeof val === 'number' ? val : 0);
          }, 0);
        }
      }

      if (operations.avg !== undefined) {
        for (const { field, as } of operations.avg) {
          const sum = docs.reduce((s, doc) => {
            const val = doc[field];
            return s + (typeof val === 'number' ? val : 0);
          }, 0);
          result[as] = docs.length > 0 ? sum / docs.length : 0;
        }
      }

      if (operations.min !== undefined) {
        for (const { field, as } of operations.min) {
          const values = docs.map((d) => d[field]).filter((v) => typeof v === 'number');
          result[as] = values.length > 0 ? Math.min(...values) : null;
        }
      }

      if (operations.max !== undefined) {
        for (const { field, as } of operations.max) {
          const values = docs.map((d) => d[field]).filter((v) => typeof v === 'number');
          result[as] = values.length > 0 ? Math.max(...values) : null;
        }
      }

      results.push(result);
    }

    return results;
  }

  /**
   * Applies projection to results (SELECT specific fields).
   */
  private applyProjection(docs: Document[], fields?: string[]): Document[] {
    if (fields === undefined || fields.length === 0) {
      return docs;
    }

    return docs.map((doc) => {
      const projected: Document = { id: doc.id };
      for (const field of fields) {
        if (field !== 'id' && doc[field] !== undefined) {
          projected[field] = doc[field];
        }
      }
      return projected;
    });
  }

  /**
   * Applies sorting to results.
   */
  private applySorting(docs: Document[], sort?: { field: string; order: 'asc' | 'desc' }): Document[] {
    if (!sort || sort.field === 'id') {
      return docs;
    }

    return docs.sort((a, b) => {
      const valA = a[sort.field];
      const valB = b[sort.field];
      if (valA === null || valA === undefined) return 1;
      if (valB === null || valB === undefined) return -1;
      if ((valA as unknown as number) < (valB as unknown as number)) return sort.order === 'asc' ? -1 : 1;
      if ((valA as unknown as number) > (valB as unknown as number)) return sort.order === 'asc' ? 1 : -1;
      return 0;
    });
  }

  /**
   * Applies pagination to results.
   */
  private applyPagination(docs: Document[], skip?: number, limit?: number): Document[] {
    const start = skip ?? 0;
    const end = limit ? start + limit : undefined;
    return docs.slice(start, end);
  }

  /**
   * Retrieves a document by its ID.
   * @param {string} id The ID of the document.
   * @returns {Promise<Document | null>} The document, or null if not found.
   */
  async findById(id: string): Promise<Document | null> {
    const primaryTree = this.indexes.get('id')!;
    const startBlockId = await primaryTree.search(id);
    if (startBlockId === null) return null;

    const docBuffer = await this.documentHeap.readBlob(startBlockId);
    if (docBuffer.length === 0) return null;
    return JSON.parse(docBuffer.toString()) as Document;
  }

  /**
   * Updates a document in the collection.
   * @param {string} id The ID of the document to update.
   * @param {Partial<Document>} updates Partial document with updates.
   * @returns {Promise<Document | null>} The updated document, or null if not found.
   */
  async update(id: string, updates: Partial<Document>): Promise<Document | null> {
    const primaryTree = this.indexes.get('id')!;
    const startBlockId = await primaryTree.search(id);
    if (startBlockId === null) return null;

    const docBuffer = await this.documentHeap.readBlob(startBlockId);
    if (docBuffer.length === 0) return null;

    const existing = JSON.parse(docBuffer.toString()) as Document;
    const updated: Document = structuredClone({ ...existing, ...updates, id });

    // Remove old index entries and add new ones for changed fields
    for (const [fieldName, indexTree] of this.indexes.entries()) {
      if (fieldName === 'id') continue; // id can't change via update

      const oldValue = existing[fieldName];
      const newValue = updated[fieldName];

      // If value changed, remove old entry from index
      if (oldValue !== newValue && oldValue !== undefined && oldValue !== null) {
        const oldIndexKey = serializeFieldValue(oldValue) + ':' + id;
        await indexTree.delete(oldIndexKey);
      }

      // Add new entry to index if value exists
      if (newValue !== undefined && newValue !== null && oldValue !== newValue) {
        const newIndexKey = serializeFieldValue(newValue) + ':' + id;
        await indexTree.insert(newIndexKey, startBlockId);
      }
    }

    // Overwrite the document in the heap
    await this.documentHeap.overwriteBlock(startBlockId, Buffer.from(JSON.stringify(updated)));

    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }

    return JSON.parse(JSON.stringify(updated)) as Document;
  }

  /**
   * Deletes a document from the collection.
   * @param {string} id The ID of the document to delete.
   * @returns {Promise<boolean>} True if the document was deleted, false if not found.
   */
  async delete(id: string): Promise<boolean> {
    const primaryTree = this.indexes.get('id')!;
    const startBlockId = await primaryTree.search(id);
    if (startBlockId === null) return false;

    const docBuffer = await this.documentHeap.readBlob(startBlockId);
    if (docBuffer.length === 0) return false;
    const existing = JSON.parse(docBuffer.toString()) as Document;

    // Remove from all secondary indexes
    for (const [fieldName, indexTree] of this.indexes.entries()) {
      if (fieldName === 'id') continue;
      const fieldValue = existing[fieldName];
      if (fieldValue !== undefined && fieldValue !== null) {
        const indexKey = serializeFieldValue(fieldValue) + ':' + id;
        await indexTree.delete(indexKey);
      }
    }

    // Remove from primary index and free heap memory
    await primaryTree.delete(id);
    await this.documentHeap.freeBlob(startBlockId);

    if (this.cachedDocumentCount !== null && this.cachedDocumentCount > 0) {
      this.cachedDocumentCount--;
    }

    // checks if this has the onDocumentCountChanged method
    if (this.onDocumentCountChanged && this.cachedDocumentCount !== null) {
      await this.onDocumentCountChanged(this.cachedDocumentCount);
    }

    // checks if this has the onChangeCallback method
    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }

    return true;
  }
}

/**
 * The SimpleDBMS database manager.
 */
export class SimpleDBMS {
  private fbFile: FreeBlockFile;
  private documentHeap: FreeBlockFile;
  private catalogTree!: BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>; //TODO: waht does this do again?
  private catalogStorage!: FBNodeStorage<string, number>;
  private collections: Map<string, Collection> = new Map();

  private constructor(fbFile: FreeBlockFile, documentHeap: FreeBlockFile) {
    this.fbFile = fbFile;
    this.documentHeap = documentHeap;
  }

  /**
   * Gets the FreeBlockFile instance.
   * @returns {FreeBlockFile} The FreeBlockFile instance.
   */
  public getFreeBlockFile(): FreeBlockFile {
    return this.fbFile;
  }

  /**
   * Creates a new database.
   * @param {File} file The file to use for the database index.
   * @param {File} walFile The file to use for the index write-ahead log.
   * @param {File} heapFile The file to use for the document heap storage.
   * @param {File} heapWalFile The file to use for the document heap write-ahead log.
   * @returns {Promise<SimpleDBMS>} A new SimpleDBMS instance.
   */
  static async create(file: File, walFile: File, heapFile: File, heapWalFile: File): Promise<SimpleDBMS> {
    const walManager = new WALManagerImpl(walFile, file);
    const atomicFile = new AtomicFileImpl(file, walManager);
    const fbFile = new FreeBlockFile(file, atomicFile, DEFAULT_BLOCK_SIZE);
    await fbFile.open();

    const heapWalManager = new WALManagerImpl(heapWalFile, heapFile);
    const heapAtomicFile = new AtomicFileImpl(heapFile, heapWalManager);
    const documentHeap = new FreeBlockFile(heapFile, heapAtomicFile, DEFAULT_BLOCK_SIZE);
    await documentHeap.open();

    const db = new SimpleDBMS(fbFile, documentHeap);
    await db.initCatalog(true);
    return db;
  }

  /**
   * Opens an existing database.
   * @param {File} file The file to use for the database index.
   * @param {File} walFile The file to use for the index write-ahead log.
   * @param {File} heapFile The file to use for the document heap storage.
   * @param {File} heapWalFile The file to use for the document heap write-ahead log.
   * @returns {Promise<SimpleDBMS>} A SimpleDBMS instance.
   */
  static async open(file: File, walFile: File, heapFile: File, heapWalFile: File): Promise<SimpleDBMS> {
    const walManager = new WALManagerImpl(walFile, file);
    const atomicFile = new AtomicFileImpl(file, walManager);
    await atomicFile.recover();
    const fbFile = new FreeBlockFile(file, atomicFile, DEFAULT_BLOCK_SIZE);
    await fbFile.open();

    const heapWalManager = new WALManagerImpl(heapWalFile, heapFile);
    const heapAtomicFile = new AtomicFileImpl(heapFile, heapWalManager);
    await heapAtomicFile.recover();
    const documentHeap = new FreeBlockFile(heapFile, heapAtomicFile, DEFAULT_BLOCK_SIZE);
    await documentHeap.open();

    const db = new SimpleDBMS(fbFile, documentHeap);
    await db.initCatalog(false);
    return db;
  }

  /**
   * Database header format for storing metadata.
   */
  private dbHeader: {
    catalogRootBlockId: number;
    collections: {
      [name: string]: {
        rootBlockId: number;
        indexes: { [field: string]: number };
        documentCount: number;
      };
    };
  } = { catalogRootBlockId: 0, collections: {} };

  private async initCatalog(isNew: boolean) {
    this.catalogStorage = new FBNodeStorage<string, number>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      (key) => key.length,
      this.fbFile,
      4096,
    );

    this.catalogTree = new BPlusTree(this.catalogStorage, 100);

    if (isNew) {
      await this.catalogTree.init();
      await this.saveCatalogRoot();
    } else {
      const headerBuf = await this.fbFile.readHeader();
      if (headerBuf.length === 0) {
        await this.catalogTree.init();
        await this.saveCatalogRoot();
      } else {
        try {
          // Remove null bytes before parsing
          const jsonStr = headerBuf.toString().replace(/\0/g, '');
          this.dbHeader = JSON.parse(jsonStr) as typeof this.dbHeader;
          const rootNode = await this.catalogStorage.loadNode(this.dbHeader.catalogRootBlockId);
          this.catalogTree.load(rootNode);
        } catch {
          const rootBlockId = headerBuf.readUInt32LE(0);
          this.dbHeader.catalogRootBlockId = rootBlockId;
          const rootNode = await this.catalogStorage.loadNode(rootBlockId);
          this.catalogTree.load(rootNode);
        }
      }
    }
  }

  private async saveCatalogRoot() {
    const root = this.catalogTree.getRoot();
    let rootId: number;

    if (root.isLeaf) {
      await this.catalogStorage.persistLeaf(root);
      rootId = root.blockId!;
    } else {
      await this.catalogStorage.persistInternal(root);
      rootId = root.blockId!;
    }

    this.dbHeader.catalogRootBlockId = rootId;

    const headerBuf = Buffer.from(JSON.stringify(this.dbHeader));
    await this.fbFile.writeHeader(headerBuf);
    await this.fbFile.commit();
  }

  /**
   * Gets a list of all existing collection names.
   * @returns {string[]} An array of collection names.
   */
  getCollectionNames(): string[] {
    return Object.keys(this.dbHeader.collections);
  }

  /**
   * Creates a new collection.
   * @param {string} name The name of the collection to create.
   * @returns {Promise<Collection>} The newly created collection.
   */
  async createCollection(name: string): Promise<Collection> {
    if (this.collections.has(name) || (await this.catalogTree.search(name)) !== null) {
      throw new Error(`Collection '${name}' already exists`);
    }

    const storage = new FBNodeStorage<string, number>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      (key) => key.length,
      this.fbFile,
      4096,
    );
    const tree = new BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>(storage, 50);

    this.dbHeader.collections[name] = {
      rootBlockId: 0,
      indexes: {},
      documentCount: 0,
    };

    await tree.init();
    await this.saveCollectionRoot(name, tree, storage);

    const collection = new Collection(
      this.documentHeap,
      tree,
      async () => {
        await this.saveCollectionRoot(name, tree, storage);
      },
      () =>
        new FBNodeStorage<string, number>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024, // Estimated index key size (required by API but unused in capacity calculations)
          this.fbFile,
          4096,
        ),
      async (fieldName: string, rootBlockId: number) => {
        await this.saveIndexMetadata(name, fieldName, rootBlockId);
      },
      async (documentCount: number) => {
        await this.saveDocumentCountMetadata(name, documentCount);
      },
      0,
    );

    this.collections.set(name, collection);
    return collection;
  }

  /**
   * Gets a collection.
   * @param {string} name The name of the collection.
   * @returns {Promise<Collection>} The collection.
   */
  async getCollection(name: string): Promise<Collection> {
    if (this.collections.has(name)) {
      return this.collections.get(name)!;
    }

    const rootBlockId = await this.catalogTree.search(name);

    if (rootBlockId === null) {
      throw new Error(`Collection '${name}' not found`);
    }

    const storage = new FBNodeStorage<string, number>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      (key) => key.length,
      this.fbFile,
      4096,
    );
    const tree = new BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>(storage, 50);

    const rootNode = await storage.loadNode(rootBlockId);
    tree.load(rootNode);

    const collectionMeta = this.dbHeader.collections[name];

    const collection = new Collection(
      this.documentHeap,
      tree,
      async () => {
        await this.saveCollectionRoot(name, tree, storage);
      },
      () =>
        new FBNodeStorage<string, number>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024,
          this.fbFile,
          4096,
        ),
      async (fieldName: string, rootBlockId: number) => {
        await this.saveIndexMetadata(name, fieldName, rootBlockId);
      },
      async (documentCount: number) => {
        await this.saveDocumentCountMetadata(name, documentCount);
      },
      collectionMeta.documentCount,
    );

    if (collectionMeta?.indexes !== undefined) {
      const indexMap = new Map<
        string,
        BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>
      >();

      for (const [field, indexRootId] of Object.entries(collectionMeta.indexes)) {
        const indexStorage = new FBNodeStorage<string, number>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024, // Estimated index key size (required by API but unused in capacity calculations)
          this.fbFile,
          4096,
        );
        const indexTree = new BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>(
          indexStorage,
          50,
        );

        const indexRootNode = await indexStorage.loadNode(indexRootId);
        indexTree.load(indexRootNode);

        indexMap.set(field, indexTree);
      }

      collection.setIndexes(indexMap);
    }

    this.collections.set(name, collection);
    return collection;
  }

  /**
   * Performs a natural join between two collections on a common field.
   * Uses hash join algorithm for O(n + m) performance.
   * @param {object} options The join options.
   * @param {string} options.leftCollection The left collection name.
   * @param {string} options.rightCollection The right collection name.
   * @param {string} options.on The field to join on for the left collection.
   * @param {string} [options.rightOn] The field to join on for the right collection (defaults to 'on').
   * @param {'inner' | 'left' | 'right'} [options.type='inner'] The type of join.
   * @returns {Promise<Document[]>} The joined documents.
   */
  async join(options: {
    leftCollection: string;
    rightCollection: string;
    on: string;
    rightOn?: string;
    type?: 'inner' | 'left' | 'right';
  }): Promise<Document[]> {
    const { leftCollection, rightCollection, on, rightOn = on, type = 'inner' } = options;

    const left = await this.getCollection(leftCollection);
    const right = await this.getCollection(rightCollection);

    const rightMap = new Map<DocumentValue, Document[]>();

    if (right.getIndexedFields().includes(rightOn)) {
      const indexTree = right.getIndex(rightOn);
      if (indexTree !== undefined) {
        for await (const { value: startBlockId } of indexTree.entries()) {
          const docBuffer = await right.getDocumentHeap().readBlob(startBlockId);
          if (docBuffer.length > 0) {
            const doc = JSON.parse(docBuffer.toString()) as Document;
            const key = doc[rightOn];
            if (!rightMap.has(key)) {
              rightMap.set(key, []);
            }
            rightMap.get(key)!.push(doc);
          }
        }
      }
    } else {
      const rightDocs = await right.find({});
      for (const doc of rightDocs) {
        const key = doc[rightOn];
        if (!rightMap.has(key)) {
          rightMap.set(key, []);
        }
        rightMap.get(key)!.push(doc);
      }
    }

    const results: Document[] = [];
    const leftDocs = await left.find({});
    for (const leftDoc of leftDocs) {
      const key = leftDoc[on];
      const rightDocs = rightMap.get(key);

      if (rightDocs !== undefined && rightDocs.length > 0) {
        for (const rightDoc of rightDocs) {
          const merged: Document = { ...leftDoc };
          for (const [field, value] of Object.entries(rightDoc)) {
            if (field === 'id' || field === rightOn) {
              continue;
            }

            if (field in leftDoc) {
              merged[`${rightCollection}_${field}`] = value;
            } else {
              merged[field] = value;
            }
          }
          results.push(merged);
        }
      } else if (type === 'left') {
        results.push({ ...leftDoc });
      }
    }

    return results;
  }

  private async saveCollectionRoot(
    name: string,
    tree: BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>,
    storage: FBNodeStorage<string, number>,
  ) {
    const root = tree.getRoot();
    let rootId: number;
    if (root.isLeaf) {
      await storage.persistLeaf(root);
      rootId = root.blockId!;
    } else {
      await storage.persistInternal(root);
      rootId = root.blockId!;
    }

    if (this.dbHeader.collections[name] === undefined) {
      this.dbHeader.collections[name] = { rootBlockId: rootId, indexes: {}, documentCount: 0 };
    } else {
      this.dbHeader.collections[name].rootBlockId = rootId;
    }

    if ((await this.catalogTree.search(name)) === null) {
      await this.catalogTree.insert(name, rootId);
    }
    await this.saveCatalogRoot();
  }

  /**
   * Saves index metadata for a collection.
   *
   * @param {string} collectionName The name of the collection.
   * @param {string} field The indexed field.
   * @param {number} rootBlockId The root block ID of the index B+ Tree.
   * @returns {Promise<void>} A promise that resolves when the metadata is saved.
   */
  async saveIndexMetadata(collectionName: string, field: string, rootBlockId: number): Promise<void> {
    if (this.dbHeader.collections[collectionName] === undefined) {
      this.dbHeader.collections[collectionName] = { rootBlockId: 0, indexes: {}, documentCount: 0 };
    }
    this.dbHeader.collections[collectionName].indexes[field] = rootBlockId;
    await this.saveCatalogRoot();
  }

  async saveDocumentCountMetadata(collectionName: string, documentCount: number): Promise<void> {
    if (this.dbHeader.collections[collectionName] === undefined) {
      this.dbHeader.collections[collectionName] = { rootBlockId: 0, indexes: {}, documentCount: 0 };
    }
    this.dbHeader.collections[collectionName].documentCount = documentCount;
    await this.saveCatalogRoot();
  }

  /**
   * Removes index metadata for a collection.
   * @param {string} collectionName The name of the collection.
   * @param {string} field The indexed field.
   * @returns {Promise<void>} A promise that resolves when the metadata is removed.
   */
  async removeIndexMetadata(collectionName: string, field: string): Promise<void> {
    if (this.dbHeader.collections[collectionName]?.indexes !== undefined) {
      delete this.dbHeader.collections[collectionName].indexes[field];
      await this.saveCatalogRoot();
    }
  }

  /**
   * Closes the database.
   */
  async close() {
    await this.fbFile.commit();
    await this.documentHeap.commit();
    await this.fbFile.close();
    await this.documentHeap.close();
  }
}
