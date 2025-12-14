// @author MaartenHaine, Jari Daemen
// @date 2025-11-22

import request from 'supertest';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { app, initDB, loadDummyAccount } from './simpledbmsd.mjs';
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
    // Load dummy account data for testing
    await loadDummyAccount();
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
      expect((res.body as { token?: string }).token).toBeDefined();
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

  describe('Collection Management API', () => {
    let authToken: string;

    beforeAll(async () => {
      const signupRes = await request(app).post('/api/signup').send({ username: 'collectionuser', password: 'pass' });
      authToken = (signupRes.body as { token: string }).token;
    });

    it('should create a collection', async () => {
      const res = await request(app)
        .post('/api/createCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName: 'myNotes' });

      expect(res.status).toBe(201);
      expect((res.body as { success?: boolean }).success).toBe(true);
    });

    it('should not allow duplicate collections', async () => {
      await request(app)
        .post('/api/createCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName: 'duplicateCollection' });

      const res = await request(app)
        .post('/api/createCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName: 'duplicateCollection' });

      expect(res.status).toBe(400);
      expect((res.body as { message?: string }).message).toContain('already exists');
    });

    it('should fetch all collections', async () => {
      const res = await request(app).get('/api/fetchCollections').set('Authorization', `Bearer ${authToken}`);

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect(Array.isArray((res.body as { collections?: string[] }).collections)).toBe(true);
    });

    it('should delete a collection', async () => {
      await request(app)
        .post('/api/createCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName: 'toDelete' });

      const res = await request(app)
        .delete('/api/deleteCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName: 'toDelete' });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
    });

    it('should reject unauthorized collection creation', async () => {
      const res = await request(app).post('/api/createCollection').send({ collectionName: 'unauthorized' });

      expect(res.status).toBe(401);
    });
  });

  describe('Document Management API', () => {
    let authToken: string;
    const collectionName = 'testDocs';

    beforeAll(async () => {
      const signupRes = await request(app).post('/api/signup').send({ username: 'docuser', password: 'pass' });
      authToken = (signupRes.body as { token: string }).token;

      await request(app)
        .post('/api/createCollection')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName });
    });

    it('should create a document', async () => {
      const res = await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'MyFirstDoc',
          documentContent: { text: 'Hello World', priority: 'high' },
        });

      expect(res.status).toBe(201);
      expect((res.body as { success?: boolean }).success).toBe(true);
    });

    it('should not allow duplicate document names', async () => {
      await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'DuplicateDoc',
          documentContent: { text: 'First' },
        });

      const res = await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'DuplicateDoc',
          documentContent: { text: 'Second' },
        });

      expect(res.status).toBe(400);
      expect((res.body as { message?: string }).message).toContain('already exists');
    });

    it('should fetch all document names', async () => {
      const res = await request(app)
        .get('/api/fetchDocuments')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ collectionName });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect(Array.isArray((res.body as { documentNames?: string[] }).documentNames)).toBe(true);
    });

    it('should fetch document content', async () => {
      await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'ContentDoc',
          documentContent: { description: 'Test content', value: 42 },
        });

      const res = await request(app)
        .get('/api/fetchDocumentContent')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ collectionName, documentName: 'ContentDoc' });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { documentContent?: { description?: string } }).documentContent?.description).toBe(
        'Test content',
      );
    });

    it('should update document content', async () => {
      await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'UpdateDoc',
          documentContent: { status: 'draft' },
        });

      const res = await request(app)
        .put('/api/updateDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'UpdateDoc',
          newDocumentContent: { status: 'published', views: 100 },
        });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);

      const checkRes = await request(app)
        .get('/api/fetchDocumentContent')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ collectionName, documentName: 'UpdateDoc' });

      expect((checkRes.body as { documentContent?: { status?: string } }).documentContent?.status).toBe('published');
    });

    it('should delete a document', async () => {
      await request(app)
        .post('/api/createDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          collectionName,
          documentName: 'DeleteDoc',
          documentContent: { temp: true },
        });

      const res = await request(app)
        .delete('/api/deleteDocument')
        .set('Authorization', `Bearer ${authToken}`)
        .send({ collectionName, documentName: 'DeleteDoc' });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);

      const checkRes = await request(app)
        .get('/api/fetchDocumentContent')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ collectionName, documentName: 'DeleteDoc' });

      expect(checkRes.status).toBe(404);
    });

    it('should reject unauthorized document operations', async () => {
      const res = await request(app)
        .post('/api/createDocument')
        .send({ collectionName, documentName: 'Unauthorized', documentContent: {} });

      expect(res.status).toBe(401);
    });
  });

  describe('Dummy Account Data', () => {
    let demoToken: string;

    beforeAll(async () => {
      // Login with demo account
      const loginRes = await request(app).post('/api/login').send({ username: 'demo', password: 'demo12345' });
      demoToken = (loginRes.body as { token: string }).token;
    });

    it('should login with demo account', async () => {
      const res = await request(app).post('/api/login').send({ username: 'demo', password: 'demo12345' });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { token?: string }).token).toBeDefined();
    });
  });
});
