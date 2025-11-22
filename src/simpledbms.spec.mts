// @author MaartenHaine
// @date 2025-11-22

import { describe, it, expect, beforeEach } from 'vitest';
import { SimpleDBMS } from './simpledbms.mjs';
import { MockFile } from './file/mockfile.mjs';

describe('SimpleDBMS', () => {
    let dbFile: MockFile;
    let walFile: MockFile;

    beforeEach(() => {
        dbFile = new MockFile(512);
        walFile = new MockFile(512);
    });

    it('create a new database and collection', async () => {
        const db = await SimpleDBMS.create(dbFile, walFile);
        const collection = await db.getCollection('users');
        expect(collection).toBeDefined();
        await db.close();
    });

    it('insert and find documents', async () => {
        const db = await SimpleDBMS.create(dbFile, walFile);
        const collection = await db.getCollection('users');

        const doc = await collection.insert({ name: 'maarten', age: 25 });
        expect(doc.id).toBeDefined();
        expect(doc['name']).toBe('maarten');

        const found = await collection.findById(doc.id);
        expect(found).toEqual(doc);

        const results = await collection.find({ filter: (d) => d['name'] === 'maarten' });
        expect(results).toHaveLength(1);
        expect(results[0]).toEqual(doc);

        await db.close();
    });

    it('update documents', async () => {
        const db = await SimpleDBMS.create(dbFile, walFile);
        const collection = await db.getCollection('users');

        const doc = await collection.insert({ name: 'random', age: 25 });
        const updated = await collection.update(doc.id, { age: 26 });

        expect(updated).toBeDefined();
        expect(updated!['age']).toBe(26);

        const found = await collection.findById(doc.id);
        expect(found!['age']).toBe(26);

        await db.close();
    });

    it('ddelete documents', async () => {
        const db = await SimpleDBMS.create(dbFile, walFile);
        const collection = await db.getCollection('users');

        const doc = await collection.insert({ name: 'random', age: 25 });
        const deleted = await collection.delete(doc.id);
        expect(deleted).toBe(true);

        const found = await collection.findById(doc.id);
        expect(found).toBeNull();

        await db.close();
    });

    it('should persist data across close/open', async () => {
        // Create and populate
        let db = await SimpleDBMS.create(dbFile, walFile);
        let collection = await db.getCollection('users');
        await collection.insert({ id: 'user1', name: 'random' });
        await db.close();

        // Reopen
        db = await SimpleDBMS.open(dbFile, walFile);
        collection = await db.getCollection('users');
        const found = await collection.findById('user1');
        expect(found).toBeDefined();
        expect(found!['name']).toBe('random');
        await db.close();
    });

    it('should handle multiple collections', async () => {
        let db = await SimpleDBMS.create(dbFile, walFile);
        const users = await db.getCollection('users');
        const posts = await db.getCollection('posts');

        await users.insert({ id: 'u1', name: 'random' });
        await posts.insert({ id: 'p1', title: 'randomtitle' });
        await db.close();

        db = await SimpleDBMS.open(dbFile, walFile);
        const users2 = await db.getCollection('users');
        const posts2 = await db.getCollection('posts');

        expect(await users2.findById('u1')).toBeDefined();
        expect(await posts2.findById('p1')).toBeDefined();
        await db.close();
    });
});
