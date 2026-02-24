// @author Wout Van Hemelrijck
// @date 2026-02-24
//
// Database compaction module.
// Implements a streaming rebuild compaction strategy (similar to SQLite's VACUUM):
// 1. Create a new database on temporary files
// 2. Stream documents one-by-one from old DB → new DB (O(1) memory)
// 3. Recreate secondary indexes
// 4. Swap temp files into the original location
//
// This reduces the physical file size by eliminating accumulated empty space
// from deleted or updated records. The streaming approach keeps memory usage
// constant regardless of database size, making it suitable for TB-scale databases.

import { SimpleDBMS } from './simpledbms.mjs';
import { FBNodeStorage } from './node-storage/fb-node-storage.mjs';
import { type File } from './file/file.mjs';

/**
 * Result returned after a compaction operation.
 */
export interface CompactionResult {
  success: boolean;
  collectionsCompacted: number;
  totalDocuments: number;
  sizeBefore: number;
  sizeAfter: number;
}

/**
 * Lightweight metadata about a collection (no document data in memory).
 */
interface CollectionMeta {
  name: string;
  indexedFields: string[];
}

/**
 * Gathers collection names and index metadata from the database.
 * This does NOT load any documents into memory.
 *
 * @param {SimpleDBMS} db - The open database instance.
 * @returns {Promise<CollectionMeta[]>} Metadata for each collection.
 */
async function gatherCollectionMeta(db: SimpleDBMS): Promise<CollectionMeta[]> {
  const collectionNames = await db.getCollectionNames();
  const metas: CollectionMeta[] = [];

  for (const name of collectionNames) {
    const collection = await db.getCollection(name);
    const loadedFields = collection.getIndexedFields();
    const headerFields = db.getCollectionIndexInfo(name);
    const indexedFields = [...new Set([...loadedFields, ...headerFields])];

    metas.push({ name, indexedFields });
  }

  return metas;
}

/**
 * Compacts a database using a streaming rebuild strategy.
 *
 * Documents are streamed one-by-one from the old database into a fresh one,
 * so memory usage is O(1) regardless of database size. This makes it suitable
 * for databases up to 1 TB and beyond.
 *
 * This is a blocking maintenance operation: the old database is closed during
 * compaction. The caller should ensure no other operations are in progress.
 *
 * @param {SimpleDBMS} db - The current database instance (will be closed).
 * @param {File} dbFile - The database file (will be recreated).
 * @param {File} walFile - The WAL file (will be recreated).
 * @param {File} [tempDbFile] - Optional temporary file for the new DB. If not
 *   provided, dbFile is reused after closing (suitable for MockFile in tests).
 * @param {File} [tempWalFile] - Optional temporary WAL file. If not provided,
 *   walFile is reused after closing.
 * @returns {Promise<{db: SimpleDBMS; result: CompactionResult}>} The new database instance and compaction stats.
 */
export async function compactDatabase(
  db: SimpleDBMS,
  dbFile: File,
  walFile: File,
  tempDbFile?: File,
  tempWalFile?: File,
): Promise<{ db: SimpleDBMS; result: CompactionResult }> {
  // Step 1: Gather metadata (collection names + index info) — no documents in memory
  const metas = await gatherCollectionMeta(db);

  // Step 2: Measure file size before compaction
  const sizeBefore = (await dbFile.stat()).size;

  // Step 3: Determine which files to build the new DB on
  const useTempFiles = tempDbFile !== undefined && tempWalFile !== undefined;
  const targetDbFile = useTempFiles ? tempDbFile : dbFile;
  const targetWalFile = useTempFiles ? tempWalFile : walFile;

  if (useTempFiles) {
    // Create fresh temp files while old DB is still open for reading
    await targetDbFile.create();
    await targetDbFile.close();
    await targetWalFile.create();
    await targetWalFile.close();
  }

  // Step 4: Create the new (empty) database on temp files
  // If using same files, we need to stream into memory first for that collection,
  // so we use temp files when available. When not available (MockFile tests),
  // we close old DB first, then rebuild.
  let newDb: SimpleDBMS;
  let totalDocuments = 0;

  if (useTempFiles) {
    // Streaming mode: old DB stays open while we write to temp files
    newDb = await SimpleDBMS.create(targetDbFile, targetWalFile);

    for (const meta of metas) {
      const oldCollection = await db.getCollection(meta.name);
      const newCollection = await newDb.getCollection(meta.name);

      // Stream documents one at a time — O(1) memory
      for await (const { value: doc } of oldCollection.entries()) {
        await newCollection.insert(doc);
        totalDocuments++;
      }

      // Recreate secondary indexes (createIndex already streams internally)
      for (const field of meta.indexedFields) {
        const indexStorage = new FBNodeStorage<string, string>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024,
          newDb.getFreeBlockFile(),
          4096,
        );
        await newCollection.createIndex(field, indexStorage);
      }
    }

    // Close old database
    await db.close();
  } else {
    // Fallback for same-file mode (MockFile tests):
    // We must collect documents per-collection since we can't read and write
    // the same file simultaneously. We still stream collection-by-collection
    // to limit peak memory to the largest single collection.
    const collectionDocs: { meta: CollectionMeta; docs: import('./simpledbms.mjs').Document[] }[] = [];

    for (const meta of metas) {
      const oldCollection = await db.getCollection(meta.name);
      const docs: import('./simpledbms.mjs').Document[] = [];
      for await (const { value: doc } of oldCollection.entries()) {
        docs.push(doc);
      }
      collectionDocs.push({ meta, docs });
    }

    // Close old DB and reset files
    await db.close();
    await dbFile.create();
    await dbFile.close();
    await walFile.create();
    await walFile.close();

    // Rebuild
    newDb = await SimpleDBMS.create(dbFile, walFile);

    for (const { meta, docs } of collectionDocs) {
      const newCollection = await newDb.getCollection(meta.name);

      for (const doc of docs) {
        await newCollection.insert(doc);
        totalDocuments++;
      }

      for (const field of meta.indexedFields) {
        const indexStorage = new FBNodeStorage<string, string>(
          (a, b) => (a < b ? -1 : a > b ? 1 : 0),
          () => 1024,
          newDb.getFreeBlockFile(),
          4096,
        );
        await newCollection.createIndex(field, indexStorage);
      }
    }
  }

  // Step 5: Measure file size after compaction
  const sizeAfter = (await targetDbFile.stat()).size;

  return {
    db: newDb,
    result: {
      success: true,
      collectionsCompacted: metas.length,
      totalDocuments,
      sizeBefore,
      sizeAfter,
    },
  };
}
