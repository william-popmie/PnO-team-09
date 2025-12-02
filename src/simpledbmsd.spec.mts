// @author MaartenHaine, Jari Daemen
// @date 2025-11-22

import request from 'supertest';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { app, initDB } from './simpledbmsd.mjs';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';

describe('SimpleDBMS Daemon API', () => {
  let tempDir: string;
  let dbPath: string;
  let walPath: string;

  beforeAll(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'simpledbms-test-'));
    dbPath = path.join(tempDir, 'test.db');
    walPath = path.join(tempDir, 'test.wal');
    await initDB(dbPath, walPath);
  });

  afterAll(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  describe('Core REST API', () => {
    it('create a document', async () => {
      const res = await request(app).post('/db/users').send({ name: 'maarten', age: 22 });

      expect(res.status).toBe(201);
      expect((res.body as { id?: string }).id).toBeDefined();
      expect((res.body as { name?: string }).name).toBe('maarten');
    });

    it('find documents', async () => {
      await request(app).post('/db/users').send({ name: 'bob', age: 25 });

      const res = await request(app).get('/db/users');
      expect(res.status).toBe(200);
      expect(Array.isArray(res.body)).toBe(true);
      expect((res.body as unknown[]).length).toBeGreaterThanOrEqual(2); // Alice + Bob
    });

    it('get a document by ID', async () => {
      const createRes = await request(app).post('/db/users').send({ name: 'bob' });
      const id = (createRes.body as { id: string }).id;

      const res = await request(app).get(`/db/users/${id}`);
      expect(res.status).toBe(200);
      expect((res.body as { name?: string }).name).toBe('bob');
    });

    it('update a document', async () => {
      const createRes = await request(app).post('/db/users').send({ name: 'bob', age: 25 });
      const id = (createRes.body as { id: string }).id;

      const res = await request(app).put(`/db/users/${id}`).send({ age: 26 });

      expect(res.status).toBe(200);
      expect((res.body as { age?: number }).age).toBe(26);

      const checkRes = await request(app).get(`/db/users/${id}`);
      expect((checkRes.body as { age?: number }).age).toBe(26);
    });

    it('delete a document', async () => {
      const createRes = await request(app).post('/db/users').send({ name: 'bob' });
      const id = (createRes.body as { id: string }).id;

      const res = await request(app).delete(`/db/users/${id}`);
      expect(res.status).toBe(200);

      const checkRes = await request(app).get(`/db/users/${id}`);
      expect(checkRes.status).toBe(404);
    });
  });

  describe('Authentication API', () => {
    it('should sign up a new user', async () => {
      const res = await request(app).post('/api/signup').send({ username: 'testuser', password: 'testpass' });

      expect(res.status).toBe(201);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { token?: string }).token).toBeDefined();
    });

    it('should not allow duplicate usernames', async () => {
      await request(app).post('/api/signup').send({ username: 'duplicate', password: 'pass' });

      const res = await request(app).post('/api/signup').send({ username: 'duplicate', password: 'pass' });

      expect(res.status).toBe(400);
      expect((res.body as { message?: string }).message).toContain('already exists');
    });

    it('should login with valid credentials', async () => {
      await request(app).post('/api/signup').send({ username: 'loginuser', password: 'loginpass' });

      const res = await request(app).post('/api/login').send({ username: 'loginuser', password: 'loginpass' });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { newToken?: string }).newToken).toBeDefined();
    });

    it('should reject invalid credentials', async () => {
      const res = await request(app).post('/api/login').send({ username: 'nonexistent', password: 'wrongpass' });

      expect(res.status).toBe(401);
    });

    it('should validate existing token', async () => {
      const signupRes = await request(app).post('/api/signup').send({ username: 'tokenuser', password: 'tokenpass' });

      const token = (signupRes.body as { token: string }).token;

      const res = await request(app).post('/api/login').send({ token });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
    });
  });

  describe('Collections API - POST /api/collections', () => {
    let authToken: string;
    let userId: string;

    beforeAll(async () => {
      // Create a user and get auth token for collection tests
      const signupRes = await request(app).post('/api/signup').send({
        username: 'collectionuser',
        password: 'collectionpass',
      });

      authToken = (signupRes.body as { token: string }).token;
      userId = (signupRes.body as { user: { id: string } }).user.id;
    });

    it('create collection with document', async () => {
      const res = await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'tasks',
          document: {
            title: 'Buy groceries',
            completed: false,
          },
        });

      expect(res.status).toBe(201);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { collectionName?: string }).collectionName).toBe('tasks');
      expect((res.body as { document?: { id?: string; title?: string; userId?: string } }).document?.id).toBeDefined();
      expect((res.body as { document?: { title?: string } }).document?.title).toBe('Buy groceries');
      expect((res.body as { document?: { userId?: string } }).document?.userId).toBe(userId);
    });

    it('reject missing token', async () => {
      const res = await request(app)
        .post('/api/collections')
        .send({
          collectionName: 'tasks',
          document: { title: 'Test' },
        });

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('No token provided');
    });

    it('reject invalid token', async () => {
      const res = await request(app)
        .post('/api/collections')
        .set('Authorization', 'Bearer invalid.token.here')
        .send({
          collectionName: 'tasks',
          document: { title: 'Test' },
        });

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('Invalid or expired token');
    });

    it('reject missing collectionName', async () => {
      const res = await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          document: { title: 'Test' },
        });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });

    it('reject missing document', async () => {
      const res = await request(app).post('/api/collections').set('Authorization', `Bearer ${authToken}`).send({
        collectionName: 'tasks',
      });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });
  });

  describe('Collections API - GET /api/collections', () => {
    let authToken: string;
    let userId: string;

    beforeAll(async () => {
      // Create a user and get auth token
      const signupRes = await request(app).post('/api/signup').send({
        username: 'getcollectionuser',
        password: 'getcollectionpass',
      });

      authToken = (signupRes.body as { token: string }).token;
      userId = (signupRes.body as { user: { id: string } }).user.id;

      // Create multiple documents in different collections
      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'notes',
          document: { content: 'Note 1' },
        });

      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'notes',
          document: { content: 'Note 2' },
        });

      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'todos',
          document: { task: 'Todo 1' },
        });
    });

    it('get all user collections with document counts', async () => {
      const res = await request(app).get('/api/collections').set('Authorization', `Bearer ${authToken}`);

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { collections?: unknown[] }).collections).toBeDefined();
      expect(Array.isArray((res.body as { collections?: unknown[] }).collections)).toBe(true);

      const collections = (res.body as { collections: Array<{ name: string; documentCount: number }> }).collections;

      // Find the notes collection
      const notesCollection = collections.find((c) => c.name === 'notes');
      expect(notesCollection).toBeDefined();
      expect(notesCollection?.documentCount).toBeGreaterThanOrEqual(2);

      // Find the todos collection
      const todosCollection = collections.find((c) => c.name === 'todos');
      expect(todosCollection).toBeDefined();
      expect(todosCollection?.documentCount).toBeGreaterThanOrEqual(1);
    });

    it('reject missing token', async () => {
      const res = await request(app).get('/api/collections');

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('No token provided');
    });

    it('reject invalid token', async () => {
      const res = await request(app).get('/api/collections').set('Authorization', 'Bearer invalid.token.here');

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('Invalid or expired token');
    });

    it('only return collections for authenticated user', async () => {
      // Create another user
      const otherUserRes = await request(app).post('/api/signup').send({
        username: 'otheruser',
        password: 'otherpass',
      });

      const otherToken = (otherUserRes.body as { token: string }).token;

      // Create a collection for the other user
      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${otherToken}`)
        .send({
          collectionName: 'privateCollection',
          document: { secret: 'data' },
        });

      // Get collections for the original user
      const res = await request(app).get('/api/collections').set('Authorization', `Bearer ${authToken}`);

      expect(res.status).toBe(200);
      const collections = (res.body as { collections: Array<{ name: string }> }).collections;

      // The original user should not see the other user's private collection
      const hasPrivateCollection = collections.some((c) => c.name === 'privateCollection');
      expect(hasPrivateCollection).toBe(false);
    });
  });

  describe('Collections API - GET /api/collections/all', () => {
    let authToken: string;
    let userId: string;

    beforeAll(async () => {
      // Create a user and get auth token
      const signupRes = await request(app).post('/api/signup').send({
        username: 'getallcollectionuser',
        password: 'getallcollectionpass',
      });

      authToken = (signupRes.body as { token: string }).token;
      userId = (signupRes.body as { user: { id: string } }).user.id;

      // Create some test data
      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'projects',
          document: { name: 'Project A', status: 'active' },
        });

      await request(app)
        .post('/api/collections')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName: 'projects',
          document: { name: 'Project B', status: 'completed' },
        });
    });

    it('get all collections with full documents', async () => {
      const res = await request(app).get('/api/collections/all').set('Authorization', `Bearer ${authToken}`);

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { collections?: unknown[] }).collections).toBeDefined();
      expect(Array.isArray((res.body as { collections?: unknown[] }).collections)).toBe(true);

      const collections = (
        res.body as {
          collections: Array<{ name: string; documentCount: number; documents: unknown[] }>;
        }
      ).collections;

      // Each collection should have documents array
      for (const collection of collections) {
        expect(collection.name).toBeDefined();
        expect(collection.documentCount).toBeGreaterThan(0);
        expect(Array.isArray(collection.documents)).toBe(true);
        expect(collection.documents.length).toBe(collection.documentCount);

        // Each document should have userId matching our user
        for (const doc of collection.documents) {
          expect((doc as { userId?: string }).userId).toBe(userId);
        }
      }
    });

    it('reject missing token', async () => {
      const res = await request(app).get('/api/collections/all');

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('No token provided');
    });

    it('reject invalid token', async () => {
      const res = await request(app).get('/api/collections/all').set('Authorization', 'Bearer invalid.token.here');

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('Invalid or expired token');
    });
  });
});
