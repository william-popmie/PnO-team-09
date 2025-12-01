// @author Maarten Haine, Jari Daemen, William Ragnarsson
// also used Claude to help with debugging
// @date 2025-11-22

import express from 'express';
import swaggerUi from 'swagger-ui-express';
import swaggerJsdoc from 'swagger-jsdoc';
import cors from 'cors';
import jwt from 'jsonwebtoken';
import { SimpleDBMS } from './simpledbms.mjs';
import { RealFile } from './file/file.mjs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = 3000;
const JWT_SECRET = process.env['JWT_SECRET'] || 'your-secret-key-change-in-production';

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
      createdAt: new Date().toISOString(),
    });

    // Create JWT token (expires in 30 minutes)
    const token = jwt.sign({ userId: newUser.id, username }, JWT_SECRET, { expiresIn: '30m' });

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
 *     summary: Login a user
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
 *         description: Login successful
 *       401:
 *         description: Invalid credentials
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

    // If token is provided, validate it
    if (existingToken) {
      try {
        const decoded = jwt.verify(existingToken, JWT_SECRET) as {
          userId: string;
          username: string;
          iat?: number;
          exp?: number;
        };

        // Check if token is about to expire (5 minutes or less)
        let newToken: string | undefined;
        if (decoded.exp) {
          const currentTime = Math.floor(Date.now() / 1000);
          const timeUntilExpiry = decoded.exp - currentTime;

          // If less than 5 minutes (300 seconds) remaining, issue new token
          if (timeUntilExpiry <= 300) {
            newToken = jwt.sign({ userId: decoded.userId, username: decoded.username }, JWT_SECRET, {
              expiresIn: '30m',
            });
          }
        }

        // Token is valid
        const response: { success: boolean; message: string; token?: string } = {
          success: true,
          message: 'Already authenticated',
        };

        if (newToken) {
          response.token = newToken;
        }

        res.json(response);
        return;
      } catch (error) {
        // Token invalid or expired, continue with username/password login
        console.log('Token validation failed, proceeding with credentials');
      }
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

    // Create JWT token (expires in 30 minutes)
    const newToken = jwt.sign({ userId: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30m' });

    res.json({
      success: true,
      message: 'Login successful',
      token: newToken,
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
app.post('/api/createCollection', async (req, res) => {
  try {
    const authHeader = req.headers['authorization'] || req.headers['Authorization'];
    const authValue = typeof authHeader === 'string' ? authHeader : authHeader?.[0];
    const token = authValue?.startsWith('Bearer ') ? authValue.substring(7) : null;

    if (!token) {
      res.status(401).json({ success: false, message: 'No token provided' });
      return;
    }

    // Verify token and extract user info
    let decoded: { userId: string; username: string; iat?: number; exp?: number };
    try {
      decoded = jwt.verify(token, JWT_SECRET) as { userId: string; username: string; iat?: number; exp?: number };
    } catch (error) {
      res.status(401).json({ success: false, message: 'Invalid or expired token' });
      return;
    }

    const { collectionName } = req.body as { collectionName?: string };

    if (!collectionName) {
      res.status(400).json({ success: false, message: 'collectionName is required' });
      return;
    }

    // Get or create the collection
    await db.getCollection(collectionName);

    // Check if token is about to expire (5 minutes or less)
    let newToken: string | undefined;
    if (decoded.exp) {
      const currentTime = Math.floor(Date.now() / 1000);
      const timeUntilExpiry = decoded.exp - currentTime;

      // If less than 5 minutes (300 seconds) remaining, issue new token
      if (timeUntilExpiry <= 300) {
        newToken = jwt.sign({ userId: decoded.userId, username: decoded.username }, JWT_SECRET, { expiresIn: '30m' });
      }
    }

    const response: {
      success: boolean;
      collectionName: string;
      token?: string;
    } = {
      success: true,
      collectionName,
    };

    if (newToken) {
      response.token = newToken;
    }

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
 *         description: List of user's collections with document counts
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 collections:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       name:
 *                         type: string
 *                         example: myTasks
 *                       documentCount:
 *                         type: number
 *                         example: 5
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       401:
 *         description: Unauthorized - invalid or missing token
 */
app.get('/api/fetchCollections', async (req, res) => {
  try {
    const authHeader = req.headers['authorization'] || req.headers['Authorization'];
    const authValue = typeof authHeader === 'string' ? authHeader : authHeader?.[0];
    const token = authValue?.startsWith('Bearer ') ? authValue.substring(7) : null;

    if (!token) {
      res.status(401).json({ success: false, message: 'No token provided' });
      return;
    }

    // Verify token and extract user info
    let decoded: { userId: string; username: string; iat?: number; exp?: number };
    try {
      decoded = jwt.verify(token, JWT_SECRET) as { userId: string; username: string; iat?: number; exp?: number };
    } catch (error) {
      res.status(401).json({ success: false, message: 'Invalid or expired token' });
      return;
    }

    // Get all collection names from the catalog
    const collectionNames = new Set<string>();

    // Add collections that are already loaded in memory
    for (const name of db['collections'].keys()) {
      collectionNames.add(name);
    }

    // Get list of collections (just names)
    const collections = Array.from(collectionNames);

    // Check if token is about to expire (5 minutes or less)
    let newToken: string | undefined;
    if (decoded.exp) {
      const currentTime = Math.floor(Date.now() / 1000);
      const timeUntilExpiry = decoded.exp - currentTime;

      if (timeUntilExpiry <= 300) {
        newToken = jwt.sign({ userId: decoded.userId, username: decoded.username }, JWT_SECRET, { expiresIn: '30m' });
      }
    }

    const response: {
      success: boolean;
      collections: string[];
      token?: string;
    } = {
      success: true,
      collections,
    };

    if (newToken) {
      response.token = newToken;
    }

    res.json(response);
  } catch (error) {
    console.error('Get collections error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

/**
 * @swagger
 * /api/collections/all:
 *   get:
 *     summary: Get all collections with all documents for the authenticated user
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
 *         description: All user's collections with all their documents
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                   example: true
 *                 collections:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       name:
 *                         type: string
 *                         example: myTasks
 *                       documentCount:
 *                         type: number
 *                         example: 5
 *                       documents:
 *                         type: array
 *                         items:
 *                           type: object
 *                         example: [{ "id": "abc123", "title": "Buy groceries", "completed": false, "userId": "user123" }]
 *                 token:
 *                   type: string
 *                   description: New token if the old one was about to expire (within 5 minutes)
 *       401:
 *         description: Unauthorized - invalid or missing token
 */
app.get('/api/collections/all', async (req, res) => {
  try {
    const authHeader = req.headers['authorization'] || req.headers['Authorization'];
    const authValue = typeof authHeader === 'string' ? authHeader : authHeader?.[0];
    const token = authValue?.startsWith('Bearer ') ? authValue.substring(7) : null;

    if (!token) {
      res.status(401).json({ success: false, message: 'No token provided' });
      return;
    }

    // Verify token and extract user info
    let decoded: { userId: string; username: string; iat?: number; exp?: number };
    try {
      decoded = jwt.verify(token, JWT_SECRET) as { userId: string; username: string; iat?: number; exp?: number };
    } catch (error) {
      res.status(401).json({ success: false, message: 'Invalid or expired token' });
      return;
    }

    // Get all collection names from the catalog
    const collectionNames = new Set<string>();

    // Add collections that are already loaded in memory
    for (const name of db['collections'].keys()) {
      collectionNames.add(name);
    }

    // For each collection, get all documents belonging to this user
    const userCollections: Array<{ name: string; documentCount: number; documents: unknown[] }> = [];

    for (const collectionName of collectionNames) {
      const collection = await db.getCollection(collectionName);
      const allDocs = await collection.find();

      // Filter documents that belong to this user
      const userDocs = allDocs.filter((doc) => {
        const docData = doc as unknown as { userId?: string };
        return docData.userId === decoded.userId;
      });

      // Only include collections where user has documents
      if (userDocs.length > 0) {
        userCollections.push({
          name: collectionName,
          documentCount: userDocs.length,
          documents: userDocs,
        });
      }
    }

    // Check if token is about to expire (5 minutes or less)
    let newToken: string | undefined;
    if (decoded.exp) {
      const currentTime = Math.floor(Date.now() / 1000);
      const timeUntilExpiry = decoded.exp - currentTime;

      if (timeUntilExpiry <= 300) {
        newToken = jwt.sign({ userId: decoded.userId, username: decoded.username }, JWT_SECRET, { expiresIn: '30m' });
      }
    }

    const response: {
      success: boolean;
      collections: Array<{ name: string; documentCount: number; documents: unknown[] }>;
      token?: string;
    } = {
      success: true,
      collections: userCollections,
    };

    if (newToken) {
      response.token = newToken;
    }

    res.json(response);
  } catch (error) {
    console.error('Get all collections error:', error);
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
