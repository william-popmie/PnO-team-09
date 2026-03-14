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
  let heapPath: string;
  let heapWalPath: string;

  beforeAll(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'simpledbms-test-'));
    dbPath = path.join(tempDir, 'test.db');
    walPath = path.join(tempDir, 'test.wal');
    heapPath = path.join(tempDir, 'test-heap.db');
    heapWalPath = path.join(tempDir, 'test-heap.wal');
    await initDB(dbPath, walPath, heapPath, heapWalPath);
  });

  afterAll(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  describe('Core REST API', () => {
    beforeAll(async () => {
      await request(app).post('/db').send({ name: 'users' });
    });

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
      expect((res.body as unknown[]).length).toBeGreaterThanOrEqual(2);
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

    it('should create and search with index', async () => {
      await request(app).post('/db').send({ name: 'indexedCol' });
      await request(app).post('/db/indexedCol').send({ name: 'indexerBase' });

      // Create index
      const idxRes = await request(app).post('/db/indexedCol/indexes/uniqueField123');
      if (idxRes.status !== 201) throw new Error(JSON.stringify(idxRes.body));
      expect(idxRes.status).toBe(201);

      // Now insert document having the field so it gets indexed
      await request(app).post('/db/indexedCol').send({ name: 'indexedUser', uniqueField123: 200 });

      // Verify index appears in the index list
      const listIdxRes = await request(app).get('/db/indexedCol/indexes');
      expect(listIdxRes.status).toBe(200);
      expect((listIdxRes.body as { indexes: string[] }).indexes).toContain('uniqueField123');

      // Search using index
      const filter = encodeURIComponent(JSON.stringify({ uniqueField123: { $eq: 200 } }));
      const findRes = await request(app).get(`/db/indexedCol?filter=${filter}`);
      expect(findRes.status).toBe(200);
      expect((findRes.body as unknown[]).length).toBeGreaterThanOrEqual(1);
    });

    it('should drop index', async () => {
      const dropRes = await request(app).delete('/db/indexedCol/indexes/uniqueField123');
      if (dropRes.status !== 200) console.error(dropRes.body);
      expect(dropRes.status).toBe(200);

      const listIdxRes = await request(app).get('/db/indexedCol/indexes');
      expect((listIdxRes.body as { indexes: string[] }).indexes).not.toContain('uniqueField123');
    });

    it('should paginate and sort results', async () => {
      // Create some mock data
      await request(app).post('/db/users').send({ name: 'A', score: 10 });
      await request(app).post('/db/users').send({ name: 'B', score: 20 });
      await request(app).post('/db/users').send({ name: 'C', score: 30 });
      await request(app).post('/db/users').send({ name: 'D', score: 40 });

      // Fetch with pagination and sort descending
      const query = '?sortField=score&sortOrder=desc&limit=2&skip=1';
      const res = await request(app).get(`/db/users${query}`);

      expect(res.status).toBe(200);
      const docs = res.body as { name: string, score: number }[];
      expect(docs).toHaveLength(2);
      // Descending total: D(40), C(30), B(20), A(10). Skip 1 -> C, B.
      expect(docs[0].name).toBe('C');
      expect(docs[1].name).toBe('B');
    });
  });
});
