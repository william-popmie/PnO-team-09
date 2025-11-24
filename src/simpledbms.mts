// @author MaartenHaine, Jari Daemen
// used Claude for debugging
// @date 2025-11-22

import { BPlusTree } from './b-plus-tree.mjs';
import { FBNodeStorage, FBLeafNode, FBInternalNode } from './node-storage/fb-node-storage.mjs';
import { FreeBlockFile, DEFAULT_BLOCK_SIZE } from './freeblockfile.mjs';
import { AtomicFileImpl } from './atomic-operations/atomic-file.mjs';
import { WALManagerImpl } from './atomic-operations/wal-manager.mjs';
import { type File } from './file/file.mjs';
import { randomUUID } from 'crypto';

// Document interface
export type DocumentValue =
  | string
  | number
  | boolean
  | null
  | bigint
  | DocumentValue[]
  | { [key: string]: DocumentValue };

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

// Collection class with secondary index support
export class Collection {
  private primaryTree: BPlusTree<string, Document, FBLeafNode<string, Document>, FBInternalNode<string, Document>>;

  // Secondary indexes: field name -> B+ Tree
  private secondaryIndexes: Map<
    string,
    BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>>
  > = new Map();

  private onChangeCallback?: () => Promise<void>;

  constructor(
    primaryTree: BPlusTree<string, Document, FBLeafNode<string, Document>, FBInternalNode<string, Document>>,
    onChangeCallback?: () => Promise<void>,
  ) {
    this.primaryTree = primaryTree;
    this.onChangeCallback = onChangeCallback;
  }

  /**
   * Creates a secondary index on a field.
   * @param {string} fieldName The field to index
   * @param {FBNodeStorage<string, string>} storage The storage to use for the index B+ Tree
   * @returns {Promise<void>} A promise that resolves when the index is created
   */
  async createIndex(fieldName: string, storage: FBNodeStorage<string, string>): Promise<void> {
    if (!isIndexableField(fieldName)) {
      throw new Error(`Field ${fieldName} cannot be indexed (starts with _ or is 'id')`);
    }

    if (this.secondaryIndexes.has(fieldName)) {
      return;
    }

    const indexTree = new BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>>(
      storage,
      50,
    );
    await indexTree.init();

    for await (const { key: docId, value: doc } of this.primaryTree.entries()) {
      const fieldValue = doc[fieldName];
      if (fieldValue !== undefined && fieldValue !== null) {
        const indexKey = serializeFieldValue(fieldValue) + ':' + docId;
        await indexTree.insert(indexKey, docId);
      }
    }

    this.secondaryIndexes.set(fieldName, indexTree);

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
    this.secondaryIndexes.delete(fieldName);

    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }
  }

  /**
   * Gets the list of indexed fields.
   */
  getIndexedFields(): string[] {
    return Array.from(this.secondaryIndexes.keys());
  }

  /**
   * Gets a secondary index tree for a field.
   */
  getIndex(
    fieldName: string,
  ): BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>> | undefined {
    return this.secondaryIndexes.get(fieldName);
  }

  /**
   * Sets secondary indexes (used when loading from disk).
   */
  setIndexes(
    indexes: Map<string, BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>>>,
  ): void {
    this.secondaryIndexes = indexes;
  }

  /**
   * Inserts a document into the collection.
   * If the document does not have an id, one will be generated.
   * @param {Omit<Document, 'id'> & { id?: string }} doc The document to insert.
   * @returns {Promise<Document>} The inserted document.
   */
  async insert(doc: Omit<Document, 'id'> & { id?: string }): Promise<Document> {
    const id = doc.id ?? randomUUID();
    const newDoc: Document = { ...doc, id };

    // Insert into primary index
    await this.primaryTree.insert(id, newDoc);

    // Update all secondary indexes
    for (const [fieldName, indexTree] of this.secondaryIndexes.entries()) {
      const fieldValue = newDoc[fieldName];
      if (fieldValue !== undefined && fieldValue !== null) {
        const indexKey = serializeFieldValue(fieldValue) + ':' + id;
        await indexTree.insert(indexKey, id);
      }
    }

    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }

    return newDoc;
  }

  /**
   * Applies filter operators to get matching document IDs using indexes.
   * @param {FilterOperators} filterOps The filter operators to apply.
   * @returns {Promise<Set<string> | null>} A set of document IDs that match the query, or null if no index is available.
   */
  private async applyFilterOps(filterOps: FilterOperators): Promise<Set<string> | null> {
    let bestField: string | null = null;
    let bestOps: FilterOperators[string] | null = null;

    for (const [field, ops] of Object.entries(filterOps)) {
      if (this.secondaryIndexes.has(field)) {
        bestField = field;
        bestOps = ops;
        break;
      }
    }

    if (!bestField || !bestOps) {
      return null;
    }

    const indexTree = this.secondaryIndexes.get(bestField)!;
    const matchingIds = new Set<string>();

    if (bestOps.$eq !== undefined) {
      const value = bestOps.$eq;
      const prefix = serializeFieldValue(value) + ':';

      for await (const { key, value: docId } of indexTree.entries()) {
        if (key.startsWith(prefix)) {
          matchingIds.add(docId);
        } else if (key > prefix + '\uffff') {
          break;
        }
      }
    } else if (
      bestOps.$gt !== undefined ||
      bestOps.$gte !== undefined ||
      bestOps.$lt !== undefined ||
      bestOps.$lte !== undefined
    ) {
      const minVal = bestOps.$gt !== undefined ? bestOps.$gt : bestOps.$gte;
      const maxVal = bestOps.$lt !== undefined ? bestOps.$lt : bestOps.$lte;
      const minInclusive = bestOps.$gte !== undefined;
      const maxInclusive = bestOps.$lte !== undefined;

      for await (const { key, value: docId } of indexTree.entries()) {
        const colonIndex = key.lastIndexOf(':');
        const serializedValue = key.substring(0, colonIndex);
        const actualValue = deserializeFieldValue(serializedValue);

        let matches = true;
        if (minVal !== undefined && minVal !== null) {
          matches =
            matches &&
            (minInclusive
              ? (actualValue as unknown as number) >= (minVal as unknown as number)
              : (actualValue as unknown as number) > (minVal as unknown as number));
        }
        if (maxVal !== undefined && maxVal !== null) {
          matches =
            matches &&
            (maxInclusive
              ? (actualValue as unknown as number) <= (maxVal as unknown as number)
              : (actualValue as unknown as number) < (maxVal as unknown as number));
        }
        if (matches) {
          matchingIds.add(docId);
        }
      }
    } else if (bestOps.$in !== undefined) {
      for (const value of bestOps.$in) {
        const prefix = serializeFieldValue(value) + ':';
        for await (const { key, value: docId } of indexTree.entries()) {
          if (key.startsWith(prefix)) {
            matchingIds.add(docId);
          } else if (key > prefix + '\uffff') {
            break;
          }
        }
      }
    }

    return matchingIds;
  }

  /**
   * Finds documents in the collection.
   * @param {Query} query The query options.
   * @returns {Promise<Document[]>} An array of documents matching the query.
   */
  async find(query: Query = {}): Promise<Document[]> {
    let results: Document[] = [];
    let candidateIds: Set<string> | null = null;

    // Step 1: Use filter operators with indexes if available
    if (query.filterOps) {
      candidateIds = await this.applyFilterOps(query.filterOps);

      if (candidateIds) {
        for (const id of candidateIds) {
          const doc = await this.primaryTree.search(id);
          if (doc) {
            let matches = true;
            for (const [field, ops] of Object.entries(query.filterOps)) {
              const value = doc[field];
              if (ops.$eq !== undefined && value !== ops.$eq) matches = false;
              if (ops.$ne !== undefined && value === ops.$ne) matches = false;
              if (
                ops.$gt !== undefined &&
                ops.$gt !== null &&
                value !== null &&
                !((value as unknown as number) > (ops.$gt as unknown as number))
              )
                matches = false;
              if (
                ops.$gte !== undefined &&
                ops.$gte !== null &&
                value !== null &&
                !((value as unknown as number) >= (ops.$gte as unknown as number))
              )
                matches = false;
              if (
                ops.$lt !== undefined &&
                ops.$lt !== null &&
                value !== null &&
                !((value as unknown as number) < (ops.$lt as unknown as number))
              )
                matches = false;
              if (
                ops.$lte !== undefined &&
                ops.$lte !== null &&
                value !== null &&
                !((value as unknown as number) <= (ops.$lte as unknown as number))
              )
                matches = false;
              if (ops.$in !== undefined && !ops.$in.includes(value)) matches = false;
              if (ops.$nin !== undefined && ops.$nin.includes(value)) matches = false;
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

    // Step 2:if no index was used
    let iterator: AsyncGenerator<{ key: string; value: Document }, void, unknown>;

    if (query.idRange) {
      const { min, max } = query.idRange;

      if (min && max) {
        iterator = this.primaryTree.range(min, max, {
          inclusiveStart: true,
          inclusiveEnd: true,
        });
      } else if (min) {
        iterator = this.primaryTree.entriesFrom(min);
      } else if (max) {
        iterator = this.primaryTree.entries();
        for await (const { key, value } of iterator) {
          if (key > max) break;
          if (!query.filter || query.filter(value)) {
            results.push(value);
          }
        }

        results = this.applyProjection(results, query.projection);
        results = this.applySorting(results, query.sort);
        return this.applyPagination(results, query.skip, query.limit);
      } else {
        iterator = this.primaryTree.entries();
      }

      if (min || max) {
        for await (const { value } of iterator) {
          if (!query.filter || query.filter(value)) {
            results.push(value);
          }
        }

        results = this.applyProjection(results, query.projection);
        results = this.applySorting(results, query.sort);
        return this.applyPagination(results, query.skip, query.limit);
      }
    }

    if (!query.filter && !query.filterOps && query.sort?.field === 'id' && query.sort.order === 'asc') {
      iterator = this.primaryTree.entries();

      let count = 0;
      const start = query.skip ?? 0;
      const limit = query.limit ?? Infinity;

      for await (const { value } of iterator) {
        if (count >= start && count < start + limit) {
          results.push(value);
        }
        count++;
        if (count >= start + limit) break;
      }

      return this.applyProjection(results, query.projection);
    }

    // Handle descending ID sort
    if (query.sort?.field === 'id' && query.sort.order === 'desc') {
      //TODO: Implement reverse iteration in BPlusTree
      const all: Document[] = [];
      for await (const { value } of this.primaryTree.reverseEntries()) {
        if (!query.filter || query.filter(value)) {
          all.push(value);
        }
      }
      // all.reverse();

      results = this.applyProjection(all, query.projection);
      return this.applyPagination(results, query.skip, query.limit);
    }

    // Full scan with filter
    iterator = this.primaryTree.entries();
    for await (const { value } of iterator) {
      if (query.filter && !query.filter(value)) {
        continue;
      }
      results.push(value);
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
    groupBy: string;
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

    // Use secondary index if available fr grouping
    let iterator: AsyncIterable<{ key: string; value: Document | string }>;

    if (this.secondaryIndexes.has(groupBy)) {
      const indexTree = this.secondaryIndexes.get(groupBy)!;
      iterator = indexTree.entries();

      for await (const { value: docId } of iterator) {
        const doc = await this.primaryTree.search(docId as string);
        if (!doc || (filter && !filter(doc))) continue;

        const groupValue = doc[groupBy];
        if (!groups.has(groupValue)) {
          groups.set(groupValue, []);
        }
        groups.get(groupValue)!.push(doc);
      }
    } else {
      // Full scan
      for await (const { value: doc } of this.primaryTree.entries()) {
        if (filter && !filter(doc)) continue;

        const groupValue = doc[groupBy];
        if (!groups.has(groupValue)) {
          groups.set(groupValue, []);
        }
        groups.get(groupValue)!.push(doc);
      }
    }

    // Compute aggregations for each group
    const results: Document[] = [];
    for (const [groupValue, docs] of groups.entries()) {
      const groupValueStr =
        typeof groupValue === 'object' && groupValue !== null ? JSON.stringify(groupValue) : String(groupValue);
      const result: Document = { id: `group_${groupValueStr}`, [groupBy]: groupValue };

      if (operations.count) {
        result[operations.count] = docs.length;
      }

      if (operations.sum) {
        for (const { field, as } of operations.sum) {
          result[as] = docs.reduce((sum, doc) => {
            const val = doc[field];
            return sum + (typeof val === 'number' ? val : 0);
          }, 0);
        }
      }

      if (operations.avg) {
        for (const { field, as } of operations.avg) {
          const sum = docs.reduce((s, doc) => {
            const val = doc[field];
            return s + (typeof val === 'number' ? val : 0);
          }, 0);
          result[as] = docs.length > 0 ? sum / docs.length : 0;
        }
      }

      if (operations.min) {
        for (const { field, as } of operations.min) {
          const values = docs.map((d) => d[field]).filter((v) => typeof v === 'number');
          result[as] = values.length > 0 ? Math.min(...values) : null;
        }
      }

      if (operations.max) {
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
    if (!fields || fields.length === 0) {
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
    return await this.primaryTree.search(id);
  }

  /**
   * Updates a document in the collection.
   * @param {string} id The ID of the document to update.
   * @param {Partial<Document>} updates Partial document with updates.
   * @returns {Promise<Document | null>} The updated document, or null if not found.
   */
  async update(id: string, updates: Partial<Document>): Promise<Document | null> {
    const existing = await this.primaryTree.search(id);
    if (!existing) return null;

    const updated: Document = { ...existing, ...updates, id };

    // Remove old index entries for changed fields
    for (const [fieldName, indexTree] of this.secondaryIndexes.entries()) {
      const oldValue = existing[fieldName];
      const newValue = updated[fieldName];

      // If value changed, remove old entry
      if (oldValue !== newValue && oldValue !== undefined && oldValue !== null) {
        const oldIndexKey = serializeFieldValue(oldValue) + ':' + id;
        await indexTree.delete(oldIndexKey);
      }

      // Add new entry if value exists
      if (newValue !== undefined && newValue !== null) {
        const newIndexKey = serializeFieldValue(newValue) + ':' + id;
        await indexTree.insert(newIndexKey, id);
      }
    }

    await this.primaryTree.insert(id, updated);

    if (this.onChangeCallback) {
      await this.onChangeCallback();
    }

    return updated;
  }

  /**
   * Deletes a document from the collection.
   * @param {string} id The ID of the document to delete.
   * @returns {Promise<boolean>} True if the document was deleted, false if not found.
   */
  async delete(id: string): Promise<boolean> {
    const existing = await this.primaryTree.search(id);
    if (!existing) return false;

    // Remove from all secondary indexes
    for (const [fieldName, indexTree] of this.secondaryIndexes.entries()) {
      const fieldValue = existing[fieldName];
      if (fieldValue !== undefined && fieldValue !== null) {
        const indexKey = serializeFieldValue(fieldValue) + ':' + id;
        await indexTree.delete(indexKey);
      }
    }

    await this.primaryTree.delete(id);

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
  private catalogTree!: BPlusTree<string, number, FBLeafNode<string, number>, FBInternalNode<string, number>>;
  private catalogStorage!: FBNodeStorage<string, number>;
  private collections: Map<string, Collection> = new Map();

  private constructor(fbFile: FreeBlockFile) {
    this.fbFile = fbFile;
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
   * @param {File} file The file to use for the database.
   * @param {File} walFile The file to use for the write-ahead log.
   * @returns {Promise<SimpleDBMS>} A new SimpleDBMS instance.
   */
  static async create(file: File, walFile: File): Promise<SimpleDBMS> {
    const walManager = new WALManagerImpl(walFile, file);
    const atomicFile = new AtomicFileImpl(file, walManager);
    const fbFile = new FreeBlockFile(file, atomicFile, DEFAULT_BLOCK_SIZE);

    await fbFile.open();

    const db = new SimpleDBMS(fbFile);
    await db.initCatalog(true);
    return db;
  }

  /**
   * Opens an existing database.
   * @param {File} file The file to use for the database.
   * @param {File} walFile The file to use for the write-ahead log.
   * @returns {Promise<SimpleDBMS>} A SimpleDBMS instance.
   */
  static async open(file: File, walFile: File): Promise<SimpleDBMS> {
    const walManager = new WALManagerImpl(walFile, file);
    const atomicFile = new AtomicFileImpl(file, walManager);
    await atomicFile.recover();

    const fbFile = new FreeBlockFile(file, atomicFile, DEFAULT_BLOCK_SIZE);
    await fbFile.open();

    const db = new SimpleDBMS(fbFile);
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
          this.dbHeader = JSON.parse(headerBuf.toString()) as typeof this.dbHeader;
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
   * Gets a collection.
   * @param {string} name The name of the collection.
   * @returns {Promise<Collection>} The collection.
   */
  async getCollection(name: string): Promise<Collection> {
    if (this.collections.has(name)) {
      return this.collections.get(name)!;
    }

    const rootBlockId = await this.catalogTree.search(name);

    const storage = new FBNodeStorage<string, Document>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      (key) => key.length,
      this.fbFile,
      4096,
    );
    const tree = new BPlusTree<string, Document, FBLeafNode<string, Document>, FBInternalNode<string, Document>>(
      storage,
      50,
    );

    if (rootBlockId !== null) {
      const rootNode = await storage.loadNode(rootBlockId);
      tree.load(rootNode);
    } else {
      await tree.init();
      await this.saveCollectionRoot(name, tree, storage);
    }

    const collection = new Collection(tree, async () => {
      await this.saveCollectionRoot(name, tree, storage);
    });

    const collectionMeta = this.dbHeader.collections[name];
    if (collectionMeta?.indexes) {
      const indexMap = new Map<
        string,
        BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>>
      >();

      for (const [field, indexRootId] of Object.entries(collectionMeta.indexes)) {
        const indexStorage = new FBNodeStorage<string, string>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024,
          this.fbFile,
          4096,
        );
        const indexTree = new BPlusTree<string, string, FBLeafNode<string, string>, FBInternalNode<string, string>>(
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
   * @param {string} options.on The field to join on.
   * @param {'inner' | 'left' | 'right'} [options.type='inner'] The type of join.
   * @returns {Promise<Document[]>} The joined documents.
   */
  async join(options: {
    leftCollection: string;
    rightCollection: string;
    on: string;
    type?: 'inner' | 'left' | 'right';
  }): Promise<Document[]> {
    const { leftCollection, rightCollection, on, type = 'inner' } = options;

    const left = await this.getCollection(leftCollection);
    const right = await this.getCollection(rightCollection);

    const rightMap = new Map<DocumentValue, Document[]>();

    if (right.getIndexedFields().includes(on)) {
      const indexTree = right.getIndex(on);
      if (indexTree) {
        for await (const { value: docId } of indexTree.entries()) {
          const doc = await right.findById(docId);
          if (doc) {
            const key = doc[on];
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
        const key = doc[on];
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

      if (rightDocs && rightDocs.length > 0) {
        for (const rightDoc of rightDocs) {
          const merged: Document = { ...leftDoc };
          for (const [field, value] of Object.entries(rightDoc)) {
            if (field !== 'id' && field !== on) {
              merged[`${rightCollection}_${field}`] = value;
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
    tree: BPlusTree<string, Document, FBLeafNode<string, Document>, FBInternalNode<string, Document>>,
    storage: FBNodeStorage<string, Document>,
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

    await this.catalogTree.insert(name, rootId);
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
    if (!this.dbHeader.collections[collectionName]) {
      this.dbHeader.collections[collectionName] = { rootBlockId: 0, indexes: {} };
    }
    this.dbHeader.collections[collectionName].indexes[field] = rootBlockId;
    await this.saveCatalogRoot();
  }

  /**
   * Removes index metadata for a collection.
   * @param {string} collectionName The name of the collection.
   * @param {string} field The indexed field.
   * @returns {Promise<void>} A promise that resolves when the metadata is removed.
   */
  async removeIndexMetadata(collectionName: string, field: string): Promise<void> {
    if (this.dbHeader.collections[collectionName]?.indexes) {
      delete this.dbHeader.collections[collectionName].indexes[field];
      await this.saveCatalogRoot();
    }
  }

  /**
   * Closes the database.
   */
  async close() {
    await this.fbFile.close();
  }
}
