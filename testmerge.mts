// @author Maarten Haine, Jari Daemen, William Ragnarsson, Wout Van Hemelrijck
// @date 2025-11-22

import 'dotenv/config';
import express from 'express';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';
<<<<<<< src/simpledbmsd.ours.mts
import { SimpleDBMS, type Document, type AggregateQuery, type FilterOperators } from './simpledbms.mjs';
=======
import cors from 'cors';
import { EncryptionService } from './encryption-service.mjs';
import { CompressionService, resolveCompressionAlgorithmFromEnvironment } from './compression/compression.mjs';
import { deserializeCompressionEnvelope, serializeCompressionEnvelope } from './compression/envelope.mjs';
import { SimpleDBMS, type DocumentValue } from './simpledbms.mjs';
>>>>>>> src/simpledbmsd.theirs.mts
import { RealFile } from './file/file.mjs';
import { FBNodeStorage } from './node-storage/fb-node-storage.mjs';

const app = express();
const port = 3000;
<<<<<<< src/simpledbmsd.ours.mts
=======
const passwordHasher = new PasswordHasher();
const contentCompressionService = new CompressionService({
  algorithm: resolveCompressionAlgorithmFromEnvironment(),
});
const DOCUMENT_COMPRESSED_PAYLOAD_MAGIC = Buffer.from('DOC1', 'ascii');

function encodeContentForStorage(content: Record<string, unknown>): Buffer {
  const jsonBuffer = Buffer.from(JSON.stringify(content));
  const compressed = contentCompressionService.compress(jsonBuffer);

  if (compressed.compressedSize >= compressed.originalSize) {
    return jsonBuffer;
  }

  return serializeCompressionEnvelope(DOCUMENT_COMPRESSED_PAYLOAD_MAGIC, compressed);
}

function decodeContentFromStorage(payload: Buffer): Record<string, unknown> {
  const compressed = deserializeCompressionEnvelope(payload, DOCUMENT_COMPRESSED_PAYLOAD_MAGIC);
  if (compressed === null) {
    return JSON.parse(payload.toString()) as Record<string, unknown>;
  }

  const decoded = contentCompressionService.decompress(compressed);
  return JSON.parse(decoded.toString()) as Record<string, unknown>;
}

// Initialize encryption service
const masterKey = process.env['ENCRYPTION_KEY'] || EncryptionService.generateMasterKey();
let encryptionService: EncryptionService;
>>>>>>> src/simpledbmsd.theirs.mts

app.use(express.json());

let db!: SimpleDBMS;

async function initDB(
  customDbPath?: string,
  customWalPath?: string,
  customHeapPath?: string,
  customHeapWalPath?: string,
) {
  try {
    const dbPath = customDbPath || process.argv[2] || 'mydb.db';
    const walPath = customWalPath || process.argv[3] || 'mydb.wal';
    const heapPath = customHeapPath || process.argv[4] || 'mydb-heap.db';
    const heapWalPath = customHeapWalPath || process.argv[5] || 'mydb-heap.wal';

    const dbFile = new RealFile(dbPath);
    const walFile = new RealFile(walPath);
    const heapFile = new RealFile(heapPath);
    const heapWalFile = new RealFile(heapWalPath);

    try {
      db = await SimpleDBMS.open(dbFile, walFile, heapFile, heapWalFile);
      console.log('Database opened successfully.');
    } catch {
      console.log('Could not open existing database, creating new one...');
      await dbFile.create();
      await dbFile.close();
      await walFile.create();
      await walFile.close();
      await heapFile.create();
      await heapFile.close();
      await heapWalFile.create();
      await heapWalFile.close();
      db = await SimpleDBMS.create(dbFile, walFile, heapFile, heapWalFile);
      console.log('Database created successfully.');
    }
  } catch (error) {
    console.error('Failed to initialize database:', error);
    throw error;
  }
}

const swaggerOptions = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'SimpleDBMS API',
      version: '1.0.0',
      description: 'A simple database management system API',
    },
    servers: [
      {
        url: `http://localhost:${port}`,
      },
    ],
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          bearerFormat: 'JWT',
        },
      },
    },
  },
  apis: ['./src/simpledbmsd.mts'],
};

const swaggerSpec = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

<<<<<<< src/simpledbmsd.ours.mts
=======
let db: SimpleDBMS;

/**
 * Load dummy demo account data from JSON file
 * This creates a demo user with pre-populated collections and documents
 */
async function loadDummyAccount() {
  try {
    const dummyDataPath = path.join(__dirname, '../data/dummy-account.json');

    if (!existsSync(dummyDataPath)) {
      console.log('No dummy-account.json found, skipping demo data initialization.');
      return;
    }

    const dummyDataContent = await readFile(dummyDataPath, 'utf-8');
    const dummyData = JSON.parse(dummyDataContent) as {
      username: string;
      password: string;
      collections: Array<{
        name: string;
        documents: Array<{
          name: string;
          content: Record<string, unknown>;
        }>;
      }>;
    };

    // Check if demo user already exists
    const usersCollection = await db.getCollection('users');
    const existingUsers = await usersCollection.find();
    const demoUserExists = existingUsers.some((user) => {
      const userData = user as unknown as { username: string };
      return userData.username && userData.username.toLowerCase() === dummyData.username.toLowerCase();
    });

    if (demoUserExists) {
      console.log('Demo user already exists, skipping initialization.');
      return;
    }

    console.log('Creating demo account...');

    // Hash the password
    const hashedPassword = await passwordHasher.hashPassword(dummyData.password);

    // Create collections list
    const collectionNames = dummyData.collections.map((col) => col.name);

    // Create the demo user
    const demoUser = await usersCollection.insert({
      username: dummyData.username,
      password: hashedPassword,
      collections: collectionNames,
      createdAt: new Date().toISOString(),
    });

    console.log(`Demo user '${dummyData.username}' created with ID: ${demoUser.id}`);

    // Create collections and insert documents
    for (const collectionData of dummyData.collections) {
      const collection = await db.getCollection(collectionData.name);
      console.log(`  Creating collection: ${collectionData.name}`);

      for (const docData of collectionData.documents) {
        // Compress then encrypt the document content
        const encodedContent = encodeContentForStorage(docData.content);
        const encryptedBuffer = encryptionService.encrypt(encodedContent);

        await collection.insert({
          name: docData.name,
          userId: demoUser.id,
          createdAt: new Date().toISOString(),
          content: encryptedBuffer.toString('base64') as unknown as Record<string, DocumentValue>,
        });
      }

      console.log(`    Added ${collectionData.documents.length} documents to ${collectionData.name}`);
    }

    console.log(
      `Demo account setup complete! Login with username: '${dummyData.username}' password: '${dummyData.password}'`,
    );
  } catch (error) {
    console.error('Failed to load dummy account:', error);
    // Don't throw - this is optional initialization
  }
}

async function initDB(customDbPath?: string, customWalPath?: string) {
  try {
    const dbPath = customDbPath || process.argv[2] || 'mydb.db';
    const walPath = customWalPath || process.argv[3] || 'mydb.wal';
    const dbFile = new RealFile(dbPath);
    const walFile = new RealFile(walPath);
    let isNewDatabase = false;

    try {
      db = await SimpleDBMS.open(dbFile, walFile);
      console.log('Database opened successfully.');
    } catch {
      console.log('Could not open existing database, creating new one...');
      await dbFile.create();
      await dbFile.close();
      await walFile.create();
      await walFile.close();
      db = await SimpleDBMS.create(dbFile, walFile);
      console.log('Database created successfully.');
      isNewDatabase = true;
    }

    encryptionService = EncryptionService.fromHexKey(masterKey);

    // Load dummy account data if this is a new database
    if (isNewDatabase) {
      await loadDummyAccount();
    }
  } catch (error) {
    console.error('Failed to initialize database:', error);
    throw error;
  }
}

>>>>>>> src/simpledbmsd.theirs.mts
/**
 * @swagger
 * /db:
 *   get:
 *     summary: List all collections in the database
 *     responses:
 *       200:
 *         description: A list of collection names
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 collections:
 *                   type: array
 *                   items:
 *                     type: string
 *               example:
 *                 collections: ["users", "products", "orders"]
 */
app.get('/db', (_req, res) => {
  try {
    const collections = db.getCollectionNames();
    res.json({ collections });
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}:
 *   get:
 *     summary: Find documents in a collection
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
<<<<<<< src/simpledbmsd.ours.mts
 *       - in: query
 *         name: filter
 *         schema:
 *           type: string
 *         description: 'JSON string for filter (e.g., {"age": {"$gt": 20}})'
=======
>>>>>>> src/simpledbmsd.theirs.mts
 *     responses:
 *       200:
 *         description: List of documents
 */
app.get('/db/:collection', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const filterQuery = req.query['filter'];
    const limitQuery = req.query['limit'];
    const skipQuery = req.query['skip'];
    const sortFieldQuery = req.query['sortField'];
    const sortOrderQuery = req.query['sortOrder'];

    let filterOps: FilterOperators | undefined = undefined;
    if (typeof filterQuery === 'string') {
      try {
        filterOps = JSON.parse(filterQuery) as FilterOperators;
      } catch {
        res.status(400).json({ error: 'Invalid JSON in filter query parameter' });
        return;
      }
    }

    let limit: number | undefined = undefined;
    let skip: number | undefined = undefined;
    if (typeof limitQuery === 'string') limit = parseInt(limitQuery, 10);
    if (typeof skipQuery === 'string') skip = parseInt(skipQuery, 10);

    let sort: { field: string; order: 'asc' | 'desc' } | undefined = undefined;
    if (typeof sortFieldQuery === 'string') {
      sort = { field: sortFieldQuery, order: sortOrderQuery === 'desc' ? 'desc' : 'asc' };
    }

    const collection = await db.getCollection(collectionName);
    const docs = await collection.find({ filterOps, limit, skip, sort });
    res.json(docs);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Comparison operators')) {
      res.status(400).json({ error: error.message });
      return;
    }
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
<<<<<<< src/simpledbmsd.ours.mts
 * /db/{collection}/indexes:
=======
 * /db/{collection}/paged:
 *   get:
 *     summary: Find documents in a collection with keyset pagination
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           minimum: 1
 *         description: Maximum number of documents to return
 *       - in: query
 *         name: after
 *         schema:
 *           type: string
 *         description: Cursor id; returns documents strictly after this id (keyset pagination)
 *       - in: query
 *         name: filterField
 *         schema:
 *           type: string
 *         description: Field name for equality filter
 *       - in: query
 *         name: filterValue
 *         schema:
 *           type: string
 *         description: Field value for equality filter
 *       - in: query
 *         name: sortField
 *         schema:
 *           type: string
 *         description: Field name to sort by
 *       - in: query
 *         name: sortOrder
 *         schema:
 *           type: string
 *           enum: [asc, desc]
 *         description: Sort direction (defaults to asc)
 *       - in: query
 *         name: projection
 *         schema:
 *           type: string
 *         description: Comma-separated list of fields to return
 *     responses:
 *       200:
 *         description: Paged documents with metadata
 */
app.get('/db/:collection/paged', async (req, res) => {
  try {
    const parseQueryValue = (raw: string): DocumentValue => {
      if (raw === 'null') return null;
      if (raw === 'true') return true;
      if (raw === 'false') return false;
      const numeric = Number(raw);
      if (!Number.isNaN(numeric) && raw.trim() !== '') return numeric;
      try {
        return JSON.parse(raw) as DocumentValue;
      } catch {
        return raw;
      }
    };

    const collectionName = req.params.collection;
    const collection = await db.getCollection(collectionName);
    const rawLimit = req.query['limit'];
    const rawAfter = req.query['after'];
    const rawFilterField = req.query['filterField'];
    const rawFilterValue = req.query['filterValue'];
    const rawSortField = req.query['sortField'];
    const rawSortOrder = req.query['sortOrder'];
    const rawProjection = req.query['projection'];

    const limit = typeof rawLimit === 'string' && rawLimit.length > 0 ? Number.parseInt(rawLimit, 10) : null;
    const after = typeof rawAfter === 'string' && rawAfter.length > 0 ? rawAfter : null;
    const filterField =
      typeof rawFilterField === 'string' && rawFilterField.trim().length > 0 ? rawFilterField.trim() : null;
    const filterValue = typeof rawFilterValue === 'string' ? parseQueryValue(rawFilterValue) : null;
    const sortField = typeof rawSortField === 'string' && rawSortField.trim().length > 0 ? rawSortField.trim() : null;
    const sortOrder: 'asc' | 'desc' = rawSortOrder === 'desc' ? 'desc' : 'asc';
    const projection =
      typeof rawProjection === 'string' && rawProjection.trim().length > 0
        ? rawProjection
            .split(',')
            .map((field) => field.trim())
            .filter((field) => field.length > 0)
        : null;

    if (limit !== null && (!Number.isInteger(limit) || limit < 1)) {
      res.status(400).json({ error: 'Invalid limit. Expected integer >= 1.' });
      return;
    }

    if (rawSortOrder !== undefined && rawSortOrder !== 'asc' && rawSortOrder !== 'desc') {
      res.status(400).json({ error: "Invalid sortOrder. Expected 'asc' or 'desc'." });
      return;
    }

    if (filterField !== null && rawFilterValue === undefined) {
      res.status(400).json({ error: 'filterValue is required when filterField is provided.' });
      return;
    }

    if (filterField === null && rawFilterValue !== undefined) {
      res.status(400).json({ error: 'filterField is required when filterValue is provided.' });
      return;
    }

    const resolvedLimit = limit ?? 25;
    const hasQueryShaping = Boolean(filterField || sortField || projection);

    let pagePlusOne: import('./simpledbms.mjs').Document[];
    if (!hasQueryShaping) {
      pagePlusOne = await collection.findPagedAfter(resolvedLimit + 1, after);
    } else {
      const query: import('./simpledbms.mjs').Query = {};

      if (filterField !== null && filterValue !== null) {
        query.filter = (doc) => doc[filterField] === filterValue;
      }

      if (sortField !== null) {
        query.sort = { field: sortField, order: sortOrder };
      }

      if (projection !== null && projection.length > 0) {
        query.projection = projection;
      }

      const shapedDocs = await collection.find(query);
      const startIndex = after !== null ? Math.max(0, shapedDocs.findIndex((doc) => doc.id === after) + 1) : 0;
      pagePlusOne = shapedDocs.slice(startIndex, startIndex + resolvedLimit + 1);
    }

    const hasNextPage = pagePlusOne.length > resolvedLimit;
    const docs = hasNextPage ? pagePlusOne.slice(0, resolvedLimit) : pagePlusOne;
    const nextCursor = hasNextPage ? (docs[docs.length - 1]?.id ?? null) : null;

    res.json({
      items: docs,
      limit: resolvedLimit,
      after: after ?? null,
      mode: 'keyset',
      query: {
        filterField: filterField ?? null,
        filterValue: rawFilterValue ?? null,
        sortField: sortField ?? null,
        sortOrder: sortField ? sortOrder : null,
        projection: projection ?? null,
      },
      hasNextPage,
      nextCursor,
    });
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/{id}:
>>>>>>> src/simpledbmsd.theirs.mts
 *   get:
 *     summary: List all indexes for a collection
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of indexed fields
 */
app.get('/db/:collection/indexes', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const collection = await db.getCollection(collectionName);
    const indexes = collection.getIndexedFields();
    res.json({ indexes });
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/{id}:
 *   get:
 *     summary: Get a document by ID
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: The document
 *       404:
 *         description: Not found
 */
app.get('/db/:collection/:id', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const id = req.params.id;
    const collection = await db.getCollection(collectionName);
    const doc = await collection.findById(id);
    if (doc) {
      res.json(doc);
    } else {
      res.status(404).json({ error: 'Document not found' });
    }
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db:
 *   post:
 *     summary: Create a new collection
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - name
 *             properties:
 *               name:
 *                 type: string
 *                 description: The name of the new collection
 *             example:
 *               name: "new_collection"
 *     responses:
 *       201:
 *         description: Collection created successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                 collection:
 *                   type: string
 *               example:
 *                 message: "Collection 'new_collection' created"
 *                 collection: "new_collection"
 *       400:
 *         description: Bad request (missing name)
 */
app.post('/db', async (req, res) => {
  try {
    const { name } = req.body as { name?: string };
    if (!name || typeof name !== 'string') {
      res.status(400).json({ error: 'Collection name is required and must be a string' });
      return;
    }

    // Checking if it already exists to avoid silently overwriting (though getCollection should be able to handle this, just tob e sure)
    const existingCollections = db.getCollectionNames();
    if (existingCollections.includes(name)) {
      res.status(400).json({ error: `Collection '${name}' already exists` });
      return;
    }
    await db.createCollection(name);

    res.status(201).json({ message: `Collection '${name}' created`, collection: name });
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}:
 *   post:
 *     summary: Insert a document into a collection
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             example:
 *               name: "John Doe"
 *               age: 25
 *               isActive: true
 *     responses:
 *       201:
 *         description: Created
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 */
app.post('/db/:collection', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const doc = req.body as Omit<Document, 'id'> & { id?: string };
    const collection = await db.getCollection(collectionName);
    const newDoc = await collection.insert(doc);
    res.status(201).json(newDoc);
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/indexes/{field}:
 *   post:
 *     summary: Create an index on a field
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: field
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       201:
 *         description: Index created
 */
app.post('/db/:collection/indexes/:field', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const field = req.params.field;
    const collection = await db.getCollection(collectionName);

    // Create storage for the index
    const indexStorage = new FBNodeStorage<string, number>(
      (a, b) => (a < b ? -1 : a > b ? 1 : 0),
      () => 1024,
      db.getFreeBlockFile(),
      4096,
    );

    await collection.createIndex(field, indexStorage);
    res.status(201).json({ success: true, field, message: `Index created on field '${field}'` });
  } catch (error) {
    if (
      error instanceof Error &&
      (error.message.startsWith('Index already exists') || error.message.startsWith('Field'))
    ) {
      res.status(400).json({ error: error.message });
      return;
    }
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/aggregate:
 *   post:
 *     summary: Perform aggregation on a collection
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             example:
 *               groupBy: "category"
 *               operations:
 *                 count: "totalProducts"
 *                 avg:
 *                   - field: "price"
 *                     as: "averagePrice"
 *                 max:
 *                   - field: "price"
 *                     as: "highestPrice"
 *                 sum:
 *                   - field: "stockQuantity"
 *                     as: "totalStock"
 *     responses:
 *       200:
 *         description: Aggregation results
 */
app.post('/db/:collection/aggregate', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const body = req.body as { groupBy?: string | null; operations: AggregateQuery['operations'] };
    const { groupBy, operations } = body;

    if (!operations) {
      res.status(400).json({ error: 'operations are required' });
      return;
    }

    const collection = await db.getCollection(collectionName);
    const results = await collection.aggregate({ groupBy, operations });
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/bulk:
 *   post:
 *     summary: Perform bulk operations
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             example:
 *               operations:
 *                 - type: "insert"
 *                   document:
 *                     name: "Alice"
 *                     age: 30
 *                 - type: "update"
 *                   id: "123e4567-e89b-12d3-a456-426614174000"
 *                   updates:
 *                     age: 31
 *                 - type: "delete"
 *                   id: "987fcdeb-51a2-43d7-9012-3456789abcde"
 *     responses:
 *       200:
 *         description: Bulk operation results
 */
app.post('/db/:collection/bulk', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const body = req.body as { operations?: unknown[] };
    const { operations } = body;

    if (!operations || !Array.isArray(operations)) {
      res.status(400).json({ error: 'operations array is required' });
      return;
    }

    const collection = await db.getCollection(collectionName);
    const results: Array<{
      success: boolean;
      type?: string;
      id?: string;
      found?: boolean;
      deleted?: boolean;
      error?: string;
    }> = [];

    for (const op of operations) {
      try {
        const operation = op as { type: string; document?: unknown; id?: string; updates?: unknown };
        if (operation.type === 'insert') {
          const doc = await collection.insert(operation.document as Omit<Document, 'id'> & { id?: string });
          results.push({ success: true, type: 'insert', id: doc.id });
        } else if (operation.type === 'update') {
          const doc = await collection.update(operation.id as string, operation.updates as Partial<Document>);
          results.push({ success: true, type: 'update', id: operation.id, found: !!doc });
        } else if (operation.type === 'delete') {
          const deleted = await collection.delete(operation.id as string);
          results.push({ success: true, type: 'delete', id: operation.id, deleted });
        } else {
          results.push({ success: false, error: `Unknown operation type: ${operation.type}` });
        }
      } catch (error) {
        results.push({ success: false, error: (error as Error).message });
      }
    }

    res.json({ results });
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/join:
 *   post:
 *     summary: Join two collections on a common field
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             example:
 *               collection: "departments"
 *               on: "departmentId"
 *               rightOn: "id"
 *     responses:
 *       200:
 *         description: Join results
 */
app.post('/db/:collection/join', async (req, res) => {
  try {
    const leftCollection = req.params.collection;
    const body = req.body as { collection?: string; on?: string; rightOn?: string; type?: 'inner' | 'left' | 'right' };
    const { collection: rightCollection, on, rightOn, type } = body;

    if (!rightCollection || !on) {
      res.status(400).json({ error: 'collection and on fields are required' });
      return;
    }

    const results = await db.join({
      leftCollection,
      rightCollection,
      on,
      rightOn,
      type: type || 'inner',
    });

    res.json(results);
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/{id}:
 *   put:
 *     summary: Update a document
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             example:
 *               age: 26
 *               isActive: false
 *     responses:
 *       200:
 *         description: Updated document
 *       404:
 *         description: Not found
 */
app.put('/db/:collection/:id', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const id = req.params.id;
    const updates = req.body as Partial<Document>;
    const collection = await db.getCollection(collectionName);
    const updated = await collection.update(id, updates);
    if (updated) {
      res.json(updated);
    } else {
      res.status(404).json({ error: 'Document not found' });
    }
<<<<<<< src/simpledbmsd.ours.mts
=======

    // Compress then encrypt the document content before storing
    const encodedContent = encodeContentForStorage(documentContent || {});
    const encryptedBuffer = encryptionService.encrypt(encodedContent);

    // Create the document in the collection with encrypted content
    await collection.insert({
      name: documentName,
      userId: req.user!.userId,
      createdAt: new Date().toISOString(),
      content: encryptedBuffer.toString('base64') as unknown as Record<string, DocumentValue>,
    });

    const response = addTokenToResponse(req, {
      success: true,
      message: `Document '${documentName}' created successfully in collection '${collectionName}'`,
    });

    res.status(201).json(response);
>>>>>>> src/simpledbmsd.theirs.mts
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/{id}:
 *   delete:
 *     summary: Delete a document
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Deleted
 *       404:
 *         description: Not found
 */
app.delete('/db/:collection/:id', async (req, res) => {
  try {
    const collectionName = req.params.collection;
    const id = req.params.id;
    const collection = await db.getCollection(collectionName);
    const deleted = await collection.delete(id);
    if (deleted) {
      res.json({ success: true });
    } else {
      res.status(404).json({ error: 'Document not found' });
    }
  } catch (error) {
    res.status(500).json({ error: (error as Error).message });
  }
});

/**
 * @swagger
 * /db/{collection}/indexes/{field}:
 *   delete:
 *     summary: Drop an index from a field
 *     parameters:
 *       - in: path
 *         name: collection
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: field
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Index dropped
 */
app.delete('/db/:collection/indexes/:field', async (req, res) => {
  try {
    const { collection: collectionName, field } = req.params;
    const collection = await db.getCollection(collectionName);
<<<<<<< src/simpledbmsd.ours.mts
    await collection.dropIndex(field);
    res.json({ success: true, field, message: `Index dropped for field '${field}'` });
=======
    const allDocuments = await collection.find();

    // Find the document by name and userId
    const document = allDocuments.find((doc) => {
      const docData = doc as unknown as { name?: string; userId?: string };
      return docData.name === documentName && docData.userId === req.user!.userId;
    });

    if (!document) {
      res.status(404).json({ success: false, message: 'Document not found' });
      return;
    }

    // Extract and decrypt the content
    const docData = document as unknown as { content?: string };
    const encryptedContent = docData.content || '';

    // Decrypt then decode the content
    const decryptedBuffer = encryptionService.decrypt(Buffer.from(encryptedContent, 'base64'));
    const documentContent = decodeContentFromStorage(decryptedBuffer);

    const response = addTokenToResponse(req, {
      success: true,
      message: 'Document content fetched successfully',
      documentContent,
    });

    res.json(response);
>>>>>>> src/simpledbmsd.theirs.mts
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('Index does not exist')) {
      res.status(400).json({ error: error.message });
      return;
    }
<<<<<<< src/simpledbmsd.ours.mts
    res.status(500).json({ error: (error as Error).message });
=======

    // Compress then encrypt the new content before storing
    const encodedContent = encodeContentForStorage(newDocumentContent);
    const encryptedBuffer = encryptionService.encrypt(encodedContent);

    // Update the document content while preserving all system fields
    const docData = document as unknown as { createdAt?: string };
    await collection.update(document.id, {
      name: documentName,
      userId: req.user!.userId,
      createdAt: docData.createdAt || new Date().toISOString(),
      content: encryptedBuffer.toString('base64') as unknown as Record<string, DocumentValue>,
    });

    const response = addTokenToResponse(req, {
      success: true,
      message: `Document '${documentName}' updated successfully in collection '${collectionName}'`,
    });

    res.json(response);
  } catch (error) {
    console.error('Update document error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
>>>>>>> src/simpledbmsd.theirs.mts
  }
});

// Start server
if (process.env['NODE_ENV'] !== 'test') {
  initDB()
    .then(() => {
      app.listen(port, () => {
        console.log(`SimpleDBMS Daemon listening at http://localhost:${port}`);
        console.log(`Swagger UI available at http://localhost:${port}/api-docs`);
      });
    })
    .catch((err) => {
      console.error('Failed to start daemon:', err);
      process.exit(1);
    });
}

export { app, initDB };
