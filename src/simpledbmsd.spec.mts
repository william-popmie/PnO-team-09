// @author MaartenHaine
// @date 2025-11-22

import request from 'supertest';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { app, initDB } from './simpledbmsd.mjs';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';

describe("SimpleDBMS Daemon API", () => {
    let tempDir: string;
    let dbPath: string;
    let walPath: string;

    beforeAll(async () => {
        tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "simpledbms-test-"));
        dbPath = path.join(tempDir, "test.db");
        walPath = path.join(tempDir, "test.wal");
        await initDB(dbPath, walPath);
    });

    afterAll(async () => {
        await fs.rm(tempDir, { recursive: true, force: true });
    });

    it("create a document", async () => {
        const res = await request(app).post("/db/users").send({ name: "maarten", age: 22 });

        expect(res.status).toBe(201);
        expect((res.body as { id?: string }).id).toBeDefined();
        expect((res.body as { name?: string }).name).toBe("maarten");
    });

    it("find documents", async () => {
        await request(app).post("/db/users").send({ name: "bob", age: 25 });

        const res = await request(app).get("/db/users");
        expect(res.status).toBe(200);
        expect(Array.isArray(res.body)).toBe(true);
        expect((res.body as unknown[]).length).toBeGreaterThanOrEqual(2); // Alice + Bob
    });

    it("get a document by ID", async () => {
        const createRes = await request(app).post("/db/users").send({ name: "bob" });
        const id = (createRes.body as { id: string }).id;

        const res = await request(app).get(`/db/users/${id}`);
        expect(res.status).toBe(200);
        expect((res.body as { name?: string }).name).toBe("bob");
    });

    it("update a document", async () => {
        const createRes = await request(app).post("/db/users").send({ name: "bob", age: 25 });
        const id = (createRes.body as { id: string }).id;

        const res = await request(app).put(`/db/users/${id}`).send({ age: 26 });

        expect(res.status).toBe(200);
        expect((res.body as { age?: number }).age).toBe(26);

        const checkRes = await request(app).get(`/db/users/${id}`);
        expect((checkRes.body as { age?: number }).age).toBe(26);
    });

    it("delete a document", async () => {
        const createRes = await request(app).post("/db/users").send({ name: "bob" });
        const id = (createRes.body as { id: string }).id;

        const res = await request(app).delete(`/db/users/${id}`);
        expect(res.status).toBe(200);

        const checkRes = await request(app).get(`/db/users/${id}`);
        expect(checkRes.status).toBe(404);
    });
});
