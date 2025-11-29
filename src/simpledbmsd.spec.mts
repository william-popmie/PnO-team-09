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

  describe('Authentication', () => {
    it('signup - create a new user', async () => {
      const res = await request(app).post('/api/signup').send({
        username: 'testuser',
        password: 'testpass123',
      });

      expect(res.status).toBe(201);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { token?: string }).token).toBeDefined();
      expect((res.body as { user?: { id?: string; username?: string } }).user?.username).toBe('testuser');
      expect((res.body as { user?: { id?: string } }).user?.id).toBeDefined();
    });

    it('signup - reject duplicate username', async () => {
      // First signup
      await request(app).post('/api/signup').send({
        username: 'duplicate',
        password: 'password1',
      });

      // Try to signup again with same username
      const res = await request(app).post('/api/signup').send({
        username: 'duplicate',
        password: 'password2',
      });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('already exists');
    });

    it('signup - reject missing username', async () => {
      const res = await request(app).post('/api/signup').send({
        password: 'testpass123',
      });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });

    it('signup - reject missing password', async () => {
      const res = await request(app).post('/api/signup').send({
        username: 'testuser2',
      });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });

    it('login - successful login with valid credentials', async () => {
      // First create a user
      await request(app).post('/api/signup').send({
        username: 'loginuser',
        password: 'loginpass',
      });

      // Then login
      const res = await request(app).post('/api/login').send({
        username: 'loginuser',
        password: 'loginpass',
      });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { token?: string }).token).toBeDefined();
      expect((res.body as { user?: { username?: string } }).user?.username).toBe('loginuser');
    });

    it('login - reject invalid username', async () => {
      const res = await request(app).post('/api/login').send({
        username: 'nonexistent',
        password: 'somepass',
      });

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('Invalid');
    });

    it('login - reject invalid password', async () => {
      // Create a user first
      await request(app).post('/api/signup').send({
        username: 'passtest',
        password: 'correctpass',
      });

      // Try to login with wrong password
      const res = await request(app).post('/api/login').send({
        username: 'passtest',
        password: 'wrongpass',
      });

      expect(res.status).toBe(401);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('Invalid');
    });

    it('login - validate existing token', async () => {
      // Create a user and get token
      const signupRes = await request(app).post('/api/signup').send({
        username: 'tokenuser',
        password: 'tokenpass',
      });

      const token = (signupRes.body as { token: string }).token;

      // Login with token
      const res = await request(app).post('/api/login').send({
        token,
      });

      expect(res.status).toBe(200);
      expect((res.body as { success?: boolean }).success).toBe(true);
      expect((res.body as { message?: string }).message).toContain('Already authenticated');
      expect((res.body as { user?: { username?: string } }).user?.username).toBe('tokenuser');
    });

    it('login - reject invalid token', async () => {
      const res = await request(app).post('/api/login').send({
        token: 'invalid.token.here',
      });

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });

    it('login - reject missing credentials', async () => {
      const res = await request(app).post('/api/login').send({});

      expect(res.status).toBe(400);
      expect((res.body as { success?: boolean }).success).toBe(false);
      expect((res.body as { message?: string }).message).toContain('required');
    });
  });
});
