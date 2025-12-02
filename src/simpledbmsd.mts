// @author Maarten Haine, Jari Daemen, William Ragnarsson
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
import {
  authenticateToken,
  addTokenToResponse,
  generateToken,
  verifyToken,
  type AuthenticatedRequest,
} from './authentication.mjs';

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
 */

/**
 * @swagger
 * /api/signup:
 *   post:
 *     summary: Register a new user
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               username:
 *                 type: string
 *               password:
 *                 type: string
 *     responses:
 *       201:
 *         description: User created, returns JWT token
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 token:
 *                   type: string
 *                 user:
 *                   type: object
 *       400:
 *         description: Invalid input or user already exists
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
 * @swagger
 * /api/login:
 *   post:
 *     summary: Login a user or validate existing token
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               username:
 *                 type: string
 *               password:
 *                 type: string
 *               token:
 *                 type: string
 *                 description: Existing JWT token to validate (optional)
 *     responses:
 *       200:
 *         description: Login successful or token validated
 *       401:
 *         description: Invalid credentials or token
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
      const decoded = verifyToken(existingToken);

      if (decoded) {
        // Token is valid - check if it needs refresh
        const response: { success: boolean; message: string; token?: string } = {
          success: true,
          message: 'Already authenticated',
        };

        // Check if token is about to expire (5 minutes or less)
        if (decoded.exp) {
          const currentTime = Math.floor(Date.now() / 1000);
          const timeUntilExpiry = decoded.exp - currentTime;

          // If less than 5 minutes (300 seconds) remaining, issue new token
          if (timeUntilExpiry <= 300) {
            response.token = generateToken(decoded.userId, decoded.username);
          }
        }

        res.json(response);
        return;
      }
      // Token invalid or expired, fall through to username/password login
    }

    // Validate input for credential-based login
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

    // Create JWT token (expires in 30 minutes)
    const newToken = generateToken(user.id, user.username);

    res.json({
      success: true,
      message: 'Login successful',
      newToken,
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * @swagger
 * /api/createCollection:
 *   post:
 *     summary: Create a new collection linked to the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - collectionName
 *             properties:
 *               collectionName:
 *                 type: string
 *                 description: Name of the collection to create
 *                 example: myTasks
 *     responses:
 *       201:
 *         description: Collection created successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 collectionName:
 *                   type: string
 *                   example: myTasks
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *                   example: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *       400:
 *         description: Bad request - missing collectionName
 *       401:
 *         description: Unauthorized - invalid or missing token
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
 * @swagger
 * /api/fetchCollections:
 *   get:
 *     summary: Get all collections for the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *     responses:
 *       200:
 *         description: List of user's collection names
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: collections fetched successfully
 *                 collections:
 *                   type: array
 *                   items:
 *                     type: string
 *                   example: ["myTasks", "myNotes", "todos"]
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       400:
 *         description: Server error when fetching user collections
 *       401:
 *         description: Unauthorized - invalid or missing token
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
 * @swagger
 * /api/deleteCollection:
 *   delete:
 *     summary: Delete a collection from the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - collectionName
 *             properties:
 *               collectionName:
 *                 type: string
 *                 description: Name of the collection to delete
 *                 example: myTasks
 *     responses:
 *       200:
 *         description: Collection deleted successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: Collection deleted successfully
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       400:
 *         description: Bad request - missing collectionName or collection not found
 *       401:
 *         description: Unauthorized - invalid or missing token
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

    // Remove collection from user's list
    userData.collections = userData.collections.filter((name) => name !== collectionName);
    await usersCollection.update(req.user!.userId, { collections: userData.collections });

    const response = addTokenToResponse(req, {
      success: true,
      message: `Collection '${collectionName}' deleted successfully`,
    });

    res.json(response);
  } catch (error) {
    console.error('Delete collection error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * @swagger
 * /api/createDocument:
 *   post:
 *     summary: Create a new document in a collection for the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - collectionName
 *               - documentName
 *             properties:
 *               collectionName:
 *                 type: string
 *                 description: Name of the collection to add the document to
 *                 example: myTasks
 *               documentName:
 *                 type: string
 *                 description: Name of the document
 *                 example: Buy groceries
 *               documentContent:
 *                 type: object
 *                 description: Content of the document (JSON object)
 *                 example: { "description": "Buy milk and eggs", "priority": "high", "completed": false }
 *     responses:
 *       201:
 *         description: Document created successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: Document created successfully
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       400:
 *         description: Bad request - missing required fields or collection not found
 *       401:
 *         description: Unauthorized - invalid or missing token
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

    // Create the document in the collection
    const collection = await db.getCollection(collectionName);
    await collection.insert({
      name: documentName,
      userId: req.user!.userId,
      createdAt: new Date().toISOString(),
      ...documentContent, // Spread the document content into the document
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
 * @swagger
 * /api/deleteDocument:
 *   delete:
 *     summary: Delete a document from a collection for the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - collectionName
 *               - documentName
 *             properties:
 *               collectionName:
 *                 type: string
 *                 description: Name of the collection containing the document
 *                 example: myTasks
 *               documentName:
 *                 type: string
 *                 description: Name of the document to delete
 *                 example: Buy groceries
 *     responses:
 *       200:
 *         description: Document deleted successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: Document deleted successfully
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       400:
 *         description: Bad request - missing required fields or document not found
 *       401:
 *         description: Unauthorized - invalid or missing token
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
 * @swagger
 * /api/fetchDocuments:
 *   get:
 *     summary: Get all documents from a collection for the authenticated user
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: header
 *         name: Authorization
 *         required: true
 *         schema:
 *           type: string
 *         description: Bearer token (format - "Bearer YOUR_JWT_TOKEN")
 *         example: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 *       - in: query
 *         name: collectionName
 *         required: true
 *         schema:
 *           type: string
 *         description: Name of the collection to fetch documents from
 *         example: myTasks
 *     responses:
 *       200:
 *         description: List of document names from the collection
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 message:
 *                   type: string
 *                   example: Documents fetched successfully
 *                 documentNames:
 *                   type: array
 *                   items:
 *                     type: string
 *                   example: ["Buy groceries", "Clean house", "Call mom"]
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       400:
 *         description: Bad request - missing collectionName or collection not found
 *       401:
 *         description: Unauthorized - invalid or missing token
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
