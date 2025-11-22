// @author Maarten Haine
// also used Claude to help with debugging
// @date 2025-11-22

import express from 'express';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';
import cors from 'cors';
import { SimpleDBMS } from './simpledbms.mjs';
import { RealFile } from './file/file.mjs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

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
    },
    apis: ['./src/simpledbmsd.mts'],
};

const swaggerSpec = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

let db: SimpleDBMS;

async function initDB(customDbPath?: string, customWalPath?: string) {
    try {
        const dbPath = customDbPath || process.argv[2] || 'mydb.db';
        const walPath = customWalPath || process.argv[3] || 'mydb.wal';
        const dbFile = new RealFile(dbPath);
        const walFile = new RealFile(walPath);
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
        }
    } catch (error) {
        console.error('Failed to initialize database:', error);
        throw error;
    }
}

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
        const doc = req.body as Omit<import('./simpledbms.mjs').Document, 'id'> & { id?: string };
        const collection = await db.getCollection(collectionName);
        const newDoc = await collection.insert(doc);
        res.status(201).json(newDoc);
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
 *       - in: query
 *         name: filter
 *         schema:
 *           type: string
 *         description: JSON string for filter (not fully implemented in query parser yet)
 *     responses:
 *       200:
 *         description: List of documents
 */
app.get('/db/:collection', async (req, res) => {
    try {
        const collectionName = req.params.collection;
        const collection = await db.getCollection(collectionName);
        const docs = await collection.find();
        res.json(docs);
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
        const updates = req.body as Partial<import('./simpledbms.mjs').Document>;
        const collection = await db.getCollection(collectionName);
        const updated = await collection.update(id, updates);
        if (updated) {
            res.json(updated);
        } else {
            res.status(404).json({ error: 'Document not found' });
        }
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
 * /db/{collection}/indexes:
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
        const { FBNodeStorage } = await import('./node-storage/fb-node-storage.mjs');
        const indexStorage = new FBNodeStorage<string, string>(
            (a, b) => (a < b ? -1 : a > b ? 1 : 0),
            () => 1024,
            db.getFreeBlockFile(),
            4096,
        );

        await collection.createIndex(field, indexStorage);
        res.status(201).json({ success: true, field, message: `Index created on field '${field}'` });
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
        const collectionName = req.params.collection;
        const field = req.params.field;
        const collection = await db.getCollection(collectionName);
        await collection.dropIndex(field);
        res.json({ success: true, field, message: `Index dropped from field '${field}'` });
    } catch (error) {
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
 *     responses:
 *       200:
 *         description: Aggregation results
 */
app.post('/db/:collection/aggregate', async (req, res) => {
    try {
        const collectionName = req.params.collection;
        const body = req.body as { groupBy?: string; operations?: import('./simpledbms.mjs').AggregateQuery['operations'] };
        const { groupBy, operations } = body;

        if (!groupBy || !operations) {
            res.status(400).json({ error: 'groupBy and operations are required' });
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
        const results: Array<{ success: boolean; type?: string; id?: string; found?: boolean; deleted?: boolean; error?: string }> = [];

        for (const op of operations) {
            try {
                const operation = op as { type: string; document?: unknown; id?: string; updates?: unknown };
                if (operation.type === 'insert') {
                    const doc = await collection.insert(operation.document as Omit<import('./simpledbms.mjs').Document, 'id'> & { id?: string });
                    results.push({ success: true, type: 'insert', id: doc.id });
                } else if (operation.type === 'update') {
                    const doc = await collection.update(operation.id as string, operation.updates as Partial<import('./simpledbms.mjs').Document>);
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
 *     responses:
 *       200:
 *         description: Join results
 */
app.post('/db/:collection/join', async (req, res) => {
    try {
        const leftCollection = req.params.collection;
        const body = req.body as { collection?: string; on?: string; type?: 'inner' | 'left' | 'right' };
        const { collection: rightCollection, on, type } = body;

        if (!rightCollection || !on) {
            res.status(400).json({ error: 'collection and on fields are required' });
            return;
        }

        const results = await db.join({
            leftCollection,
            rightCollection,
            on,
            type: type || 'inner',
        });

        res.json(results);
    } catch (error) {
        res.status(500).json({ error: (error as Error).message });
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
