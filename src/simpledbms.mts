import type { BPlusTree } from './b-plus-tree.mjs';
import { FreeBlockFile } from './freeblockfile.mjs';
import type { InternalNodeStorage, LeafNodeStorage } from './node-storage.mjs';

// unique numeric identifier for a document (mapped to BlobId)
export type DocumentId = number;
// object representing user's datarecord
export type Document = Record<string, any>;
// value type for indexing key
export type FieldValue = string | number | bigint | boolean;

// Metadata for a single collection
interface CollectionConfig {
  nextDocId: DocumentId;
  primaryIndexBlockId: number;
  secondaryIndexes: Map<string, number>;
}

interface DbHeaderData {
  collections: Map<string, CollectionConfig>;
}

// These following functions help with convert javascript object to json string and then to
// raw sequence of bytes so that it can be easily used for FreeBlockFile write
const serializeDocument = (document: Document): Buffer => {
  return Buffer.from(JSON.stringify(document), 'utf-8');
};

const serializeDocument = (data: Buffer): Document => {
  return JSON.parse(data.toString('utf-8'));
};

/**
 * This function splits the combined secondary key of the FieldValue:docId
 */
const splitSecondaryKey = (key: String): { fieldValue: FieldValue; docId: DocumentId } => {
  const parts = key.split(':');
  const docId = parseInt(parts.pop() || '0', 10);
  const fieldValue = parts.join(':'); //this is just to make sure that if there are colons in the fieldvalue it doesn't cause problems
  return { fieldValue, docId };
};

// basic helper to easily filter if a field can be indexed
// we use underscore to filter ones that we cannot index: _noindex
const isTraversableField = (fieldName: string): boolean => {
  return !fieldName.startsWith('_');
};

/**
 * this gives an async iterator interface to easily traverse documents in order for B plus tree
 */
export class Cursor implements AsyncIterable<{ key: FieldValue; document: Document }> {
  private readonly collection: Collection;
  private readonly index: BPlusTree<string, null, LeafNodeStorageType, InternalNodeStorageType>;
  private readonly fieldName: string;
  private readonly iterator: AsyncGenerator<any, void, unknown>;

  constructor(
    collection: Collection,
    fieldName: string,
    index: BPlusTree<string, null, LeafNodeStorageType, InternalNodeStorageType>,
  ) {
    this.collection = collection;
    this.fieldName = fieldName;
    this.index = index;
    this.iterator = index.entries();
  }

  /**
   * this retrieves the next document in the indexed order
   */
  public async next(): Promise<{ key: FieldValue; document: Document } | null> {
    const result = await this.iterator.next();

    if (result.done) {
      return null;
    }

    const keyString = result.value;
    const { fieldValue, docId } = splitSecondaryKey(keyString);
    const document = await this.collection.get(docId);
    return { key: fieldValue, document };
  }

  public [Symbol.asyncIterator](): AsyncGenerator<{ key: FieldValue; document: Document }, void, unknown> {
    return this;
  }
}

// We are making a seperation of different indexes:
// primary index: document id --> block ID
// secondary index: this is for the fieldvalues,
// find all document where age = 17 for example this will result in multiple results instead of unique
export class Collection {
  public readonly name: string;
  private readonly dbms: SimpleDBMS;
  private config: CollectionConfig;
  private primaryIndex: BPlusTree<DocumentId, number, LeafNodeStorageType, InternalNodeStorageType>;
  private secondaryIndexes: Map<string, BPlusTree<string, null, LeafNodeStorageType, InternalNodeStorageType>>;

  constructor(
    dbms: SimpleDBMS,
    name: string,
    config: CollectionConfig,
    primaryIndex: BPlusTree<DocumentId, number, LeafNodeStorageType, InternalNodeStorageType>,
    secondaryIndexes: Map<string, BPlusTree<string, null, LeafNodeStorageType, InternalNodeStorageType>>,
  ) {
    this.dbms = dbms;
    this.name = name;
    this.config = config;
    this.primaryIndex = primaryIndex;
    this.secondaryIndexes = secondaryIndexes;
  }

  private createSecondaryKey(fieldValue: FieldValue, docId: DocumentId): string {
    return `${fieldValue}:${docId}`;
  }

  public async insert(document: Document): Promise<DocumentId> {
    return;
  }

  public async get(id: DocumentId): Promise<Document> {
    return;
  }
}

export class SimpleDBMS {
  private filePath: string;
  private freeBlockFile: FreeBlockFile;
  private bTreeOrder: number;
  private collections: Map<string, Collection>;
  private dbHeader: DbHeaderData;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  async init(): Promise<void> {
    this.freeBlockFile = new FreeBlockFile(this.filePath);
    await this.freeBlockFile.open();
    const headerData = await this.freeBlockFile.readHeader();
    // add a deserialize DbHeader for collection
  }
}
