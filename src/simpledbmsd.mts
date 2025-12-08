// @author Maarten Haine, Jari Daemen, William Ragnarsson, Wout Van Hemelrijck
// also used Claude to help with debugging
// @date 2025-11-22

import express from 'express';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';
import cors from 'cors';
import { EncryptionService } from './encryption-service.mjs';
import { SimpleDBMS, type DocumentValue } from './simpledbms.mjs';
import { RealFile } from './file/file.mjs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  authenticateToken,
  addTokenToResponse,
  generateToken,
  validateAndRefreshToken,
  type AuthenticatedRequest,
} from './authentication.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = 3000;

// Initialize encryption service
const masterKey = process.env['ENCRYPTION_KEY'] || EncryptionService.generateMasterKey();
let encryptionService: EncryptionService;

app.use(cors());
app.use(express.json());
// Serve static frontend assets (HTML, CSS) from src, scripts from build
app.use('/components', express.static(path.join(__dirname, '../src/frontend/components')));
app.use('/styles', express.static(path.join(__dirname, '../src/frontend/styles')));
app.use('/scripts', express.static(path.join(__dirname, 'frontend/scripts')));

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

    encryptionService = EncryptionService.fromHexKey(masterKey);
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
          const doc = await collection.insert(
            operation.document as Omit<import('./simpledbms.mjs').Document, 'id'> & { id?: string },
          );
          results.push({ success: true, type: 'insert', id: doc.id });
        } else if (operation.type === 'update') {
          const doc = await collection.update(
            operation.id as string,
            operation.updates as Partial<import('./simpledbms.mjs').Document>,
          );
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

/**
 * William Ragnarsson
 * Frontend webapp routing endpoints
 * Note: These endpoints are not included in public API documentation for security reasons
 */

/**
 * POST /api/signup
 * Register a new user account
 * @param {string} username - The desired username
 * @param {string} password - The user's password (TODO: should be hashed)
 * @returns {object} { success: boolean, message: string, token: string }
 */
app.post('/api/signup', async (req, res) => {
  try {
    const { username, password } = req.body as { username?: string; password?: string };

    // Validate input
    if (!username || !password) {
      res.status(400).json({ success: false, message: 'Username and password are required' });
      return;
    }

    // Get users collection (this internally uses db.getCollection())
    const usersCollection = await db.getCollection('users');

    // Check if user already exists
    const existingUsers = await usersCollection.find();
    const userExists = existingUsers.some((user) => {
      const userData = user as unknown as { username: string };
      return userData.username && userData.username.toLowerCase() === username.toLowerCase();
    });

    if (userExists) {
      res.status(400).json({ success: false, message: 'Username already exists' });
      return;
    }

    // Create new user (this internally calls collection.insert())
    const newUser = await usersCollection.insert({
      username,
      password, // TODO: Hash this in production!
      collections: [],
      createdAt: new Date().toISOString(),
    });

    // Create JWT token (expires in 30 minutes)
    const token = generateToken(newUser.id, username);

    res.status(201).json({
      success: true,
      message: 'User created successfully',
      token,
    });
  } catch (error) {
    console.error('Signup error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * POST /api/login
 * Authenticate a user with credentials or validate an existing token
 * @param {string} [username] - Username for credential-based login
 * @param {string} [password] - Password for credential-based login
 * @param {string} [token] - Existing JWT token for validation
 * @returns {object} { success: boolean, message: string, token: string }
 */
app.post('/api/login', async (req, res) => {
  try {
    const {
      username,
      password,
      token: existingToken,
    } = req.body as {
      username?: string;
      password?: string;
      token?: string;
    };

    // If token is provided, validate it for auto-login
    if (existingToken) {
      const validation = validateAndRefreshToken(existingToken);

      if (validation.valid) {
        res.json({
          success: true,
          message: 'Already authenticated',
          token: validation.newToken || existingToken,
        });
        return;
      }
      // Token invalid or expired, fall through to username/password login
    }

    // Validate input
    if (!username || !password) {
      res.status(400).json({ success: false, message: 'Username and password are required' });
      return;
    }

    // Get users collection
    const usersCollection = await db.getCollection('users');
    const users = await usersCollection.find();

    // Find user
    const user = users.find((u) => {
      const userData = u as unknown as { username?: string };
      return userData.username && userData.username.toLowerCase() === username.toLowerCase();
    }) as { id: string; username: string; password: string } | undefined;

    if (!user) {
      res.status(401).json({ success: false, message: 'Invalid username or password' });
      return;
    }

    // Check password (plain text for now - TODO: use bcrypt.compare() in production)
    if (user.password !== password) {
      res.status(401).json({ success: false, message: 'Invalid username or password' });
      return;
    }

    // Create JWT token
    const token = generateToken(user.id, user.username);

    res.json({
      success: true,
      message: 'Login successful',
      token,
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * POST /api/createCollection
 * Create a new collection for the authenticated user
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection to create
 * @returns {object} { success: boolean, message: string, token?: string }
 */
app.post('/api/createCollection', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const { collectionName } = req.body as { collectionName?: string };

    if (!collectionName) {
      res.status(400).json({ success: false, message: 'collectionName is required' });
      return;
    }

    // Check if user already has this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (user) {
      const userData = user as unknown as { collections?: string[] };

      // Initialize collections array if it doesn't exist
      if (!userData.collections) {
        userData.collections = [];
      }

      // Check if collection already exists for this user
      if (userData.collections.includes(collectionName)) {
        res.status(400).json({ success: false, message: 'Collection already exists' });
        return;
      }

      // Create the collection and add to user's list
      await db.getCollection(collectionName);
      userData.collections.push(collectionName);
      await usersCollection.update(req.user!.userId, { collections: userData.collections });
    }

    const response = addTokenToResponse(req, {
      success: true,
      message: `Collection created succesfully and assigned to: ${req.user!.username}`,
    });

    res.status(201).json(response);
  } catch (error) {
    console.error('Create collection error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * GET /api/fetchCollections
 * Retrieve all collections owned by the authenticated user
 * @requires Authentication - Bearer token in Authorization header
 * @returns {object} { success: boolean, message: string, collections: string[], token?: string }
 */
app.get('/api/fetchCollections', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    // Get user's collections list
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    let collections: string[] = [];
    if (user) {
      const userData = user as unknown as { collections: string[] };
      collections = userData.collections || null;
    }

    if (!collections) {
      res.status(400).json({ success: false, message: 'Server error when fetching usercollections' });
      return;
    }

    const response = addTokenToResponse(req, {
      success: true,
      message: 'collections fetched succesfully',
      collections,
    });

    res.json(response);
  } catch (error) {
    console.error('Get collections error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * DELETE /api/deleteCollection
 * Delete a collection from the authenticated user's account
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection to delete
 * @returns {object} { success: boolean, message: string, token?: string }
 */
app.delete('/api/deleteCollection', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const { collectionName } = req.body as { collectionName?: string };

    if (!collectionName) {
      res.status(400).json({ success: false, message: 'collectionName is required' });
      return;
    }

    // Get user document
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections: string[] };

    // Check if collection exists in user's list
    if (!userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Delete all documents in the collection that belong to this user
    const collection = await db.getCollection(collectionName);
    const allDocuments = await collection.find();

    const userDocuments = allDocuments.filter((doc) => {
      const docData = doc as unknown as { userId?: string };
      return docData.userId === req.user!.userId;
    });

    // Delete each document
    for (const doc of userDocuments) {
      await collection.delete(doc.id);
    }

    console.log(
      `Deleted ${userDocuments.length} documents from collection '${collectionName}' for user ${req.user!.userId}`,
    );

    // Remove collection from user's list
    userData.collections = userData.collections.filter((name) => name !== collectionName);
    await usersCollection.update(req.user!.userId, { collections: userData.collections });

    const response = addTokenToResponse(req, {
      success: true,
      message: `Collection '${collectionName}' and ${userDocuments.length} associated document(s) deleted successfully`,
    });

    res.json(response);
  } catch (error) {
    console.error('Delete collection error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * POST /api/createDocument
 * Create a new document in a collection for the authenticated user
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection
 * @param {string} documentName - Name of the document (must be unique per user per collection)
 * @param {object} documentContent - JSON object containing the document data
 * @returns {object} { success: boolean, message: string, token?: string }
 */
app.post('/api/createDocument', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const { collectionName, documentName, documentContent } = req.body as {
      collectionName?: string;
      documentName?: string;
      documentContent?: Record<string, unknown>;
    };

    if (!collectionName || !documentName) {
      res.status(400).json({ success: false, message: 'collectionName and documentName are required' });
      return;
    }

    // Verify user has access to this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections?: string[] };

    // Check if collection exists in user's list
    if (!userData.collections || !userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Get the collection and check for duplicate document names
    const collection = await db.getCollection(collectionName);
    const existingDocuments = await collection.find();

    // Check if a document with this name already exists for this user in this collection
    const documentExists = existingDocuments.some((doc) => {
      const docData = doc as unknown as { name?: string; userId?: string };
      return docData.name === documentName && docData.userId === req.user!.userId;
    });

    if (documentExists) {
      res.status(400).json({ success: false, message: 'A document with this name already exists in the collection' });
      return;
    }

    // Encrypt the document content before storing
    const encryptedBuffer = encryptionService.encrypt(Buffer.from(JSON.stringify(documentContent || {})));

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
  } catch (error) {
    console.error('Create document error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * DELETE /api/deleteDocument
 * Delete a document from a collection
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection
 * @param {string} documentName - Name of the document to delete
 * @returns {object} { success: boolean, message: string, token?: string }
 */
app.delete('/api/deleteDocument', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const { collectionName, documentName } = req.body as {
      collectionName?: string;
      documentName?: string;
    };

    if (!collectionName || !documentName) {
      res.status(400).json({ success: false, message: 'collectionName and documentName are required' });
      return;
    }

    // Verify user has access to this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections?: string[] };

    // Check if collection exists in user's list
    if (!userData.collections || !userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Find and delete the document
    const collection = await db.getCollection(collectionName);
    const documents = await collection.find();

    // Find the document by name and userId
    const document = documents.find((doc) => {
      const docData = doc as unknown as { name?: string; userId?: string };
      return docData.name === documentName && docData.userId === req.user!.userId;
    });

    if (!document) {
      res.status(404).json({ success: false, message: 'Document not found' });
      return;
    }

    // Delete the document
    await collection.delete(document.id);

    const response = addTokenToResponse(req, {
      success: true,
      message: `Document '${documentName}' deleted successfully from collection '${collectionName}'`,
    });

    res.json(response);
  } catch (error) {
    console.error('Delete document error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * GET /api/fetchDocuments
 * Retrieve all document names from a collection for the authenticated user
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection (query parameter)
 * @returns {object} { success: boolean, message: string, documentNames: string[], token?: string }
 */
app.get('/api/fetchDocuments', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const collectionName = req.query['collectionName'] as string | undefined;

    if (!collectionName) {
      res.status(400).json({ success: false, message: 'collectionName is required' });
      return;
    }

    // Verify user has access to this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections?: string[] };

    // Check if collection exists in user's list
    if (!userData.collections || !userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Get all documents from the collection that belong to this user
    const collection = await db.getCollection(collectionName);
    const allDocuments = await collection.find();

    // Filter documents by userId and extract names
    const documentNames = allDocuments
      .filter((doc) => {
        const docData = doc as unknown as { userId?: string };
        return docData.userId === req.user!.userId;
      })
      .map((doc) => {
        const docData = doc as unknown as { name?: string };
        return docData.name || '';
      })
      .filter((name) => name !== ''); // Remove empty names

    const response = addTokenToResponse(req, {
      success: true,
      message: 'Documents fetched successfully',
      documentNames,
    });

    res.json(response);
  } catch (error) {
    console.error('Fetch documents error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * GET /api/fetchDocumentContent
 * Retrieve the full content of a specific document
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection (query parameter)
 * @param {string} documentName - Name of the document (query parameter)
 * @returns {object} { success: boolean, message: string, documentContent: object, token?: string }
 */
app.get('/api/fetchDocumentContent', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const collectionName = req.query['collectionName'] as string | undefined;
    const documentName = req.query['documentName'] as string | undefined;

    if (!collectionName || !documentName) {
      res.status(400).json({ success: false, message: 'collectionName and documentName are required' });
      return;
    }

    // Verify user has access to this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections?: string[] };

    // Check if collection exists in user's list
    if (!userData.collections || !userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Find the document in the collection
    const collection = await db.getCollection(collectionName);
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

    // Decrypt the content
    const decryptedBuffer = encryptionService.decrypt(Buffer.from(encryptedContent, 'base64'));
    const documentContent = JSON.parse(decryptedBuffer.toString()) as Record<string, unknown>;

    const response = addTokenToResponse(req, {
      success: true,
      message: 'Document content fetched successfully',
      documentContent,
    });

    res.json(response);
  } catch (error) {
    console.error('Fetch document content error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * PUT /api/updateDocument
 * Update the content of an existing document
 * @requires Authentication - Bearer token in Authorization header
 * @param {string} collectionName - Name of the collection
 * @param {string} documentName - Name of the document to update
 * @param {object} newDocumentContent - New JSON object to replace the document content
 * @returns {object} { success: boolean, message: string, token?: string }
 * @note The name and userId fields are preserved during update
 */
app.put('/api/updateDocument', authenticateToken, async (req: AuthenticatedRequest, res) => {
  try {
    const { collectionName, documentName, newDocumentContent } = req.body as {
      collectionName?: string;
      documentName?: string;
      newDocumentContent?: Record<string, unknown>;
    };

    if (!collectionName || !documentName || !newDocumentContent) {
      res
        .status(400)
        .json({ success: false, message: 'collectionName, documentName, and newDocumentContent are required' });
      return;
    }

    // Verify user has access to this collection
    const usersCollection = await db.getCollection('users');
    const user = await usersCollection.findById(req.user!.userId);

    if (!user) {
      res.status(404).json({ success: false, message: 'User not found' });
      return;
    }

    const userData = user as unknown as { collections?: string[] };

    // Check if collection exists in user's list
    if (!userData.collections || !userData.collections.includes(collectionName)) {
      res.status(400).json({ success: false, message: 'Collection not found in user collections' });
      return;
    }

    // Find the document in the collection
    const collection = await db.getCollection(collectionName);
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

    // Encrypt the new content before storing
    const encryptedBuffer = encryptionService.encrypt(Buffer.from(JSON.stringify(newDocumentContent)));

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
