import { describe, it, expect } from 'vitest';
import { MockFileSystem } from './file.mjs';
import { MockFile } from './mockfile.mjs';

describe('MockFileSystem', () => {
    // Helper om een MockFile te maken met bepaalde content
    async function createMockFile(content: string): Promise<MockFile> {
        const file = new MockFile(512); // 512 bytes sector size
        await file.create();
        if (content) {
            await file.writev([Buffer.from(content)], 0);
            await file.sync();
        }
        return file;
    }

    // Helper om content van een File te lezen
    async function readContent(file: MockFile): Promise<string> {
        const stats = await file.stat();
        const buffer = Buffer.alloc(stats.size);
        await file.read(buffer, { position: 0 });
        return buffer.toString();
    }

    describe('basic file operations', () => {
        it('should write and read a file in root', async () => {
            const fs = new MockFileSystem();
            const content = 'Hello, World!';
            const file = await createMockFile(content);
            
            await fs.writeFile('test.txt', file);
            const readBack = await fs.readFile('test.txt');
            
            expect(await readContent(readBack as MockFile)).toBe(content);
        });

        it('should write and read a file in nested directory', async () => {
            const fs = new MockFileSystem();
            const content = 'Nested content';
            const file = await createMockFile(content);
            
            await fs.writeFile('dir1/dir2/test.txt', file);
            const readBack = await fs.readFile('dir1/dir2/test.txt');
            
            expect(await readContent(readBack as MockFile)).toBe(content);
        });

        it('should overwrite existing file', async () => {
            const fs = new MockFileSystem();
            const file1 = await createMockFile('Original');
            const file2 = await createMockFile('Updated');
            
            await fs.writeFile('overwrite.txt', file1);
            await fs.writeFile('overwrite.txt', file2);
            
            const readBack = await fs.readFile('overwrite.txt');
            expect(await readContent(readBack as MockFile)).toBe('Updated');
        });

        it('should fail reading non-existent file', async () => {
            const fs = new MockFileSystem();
            await expect(fs.readFile('not-exists.txt'))
                .rejects.toThrow('File not found');
        });
    });

    describe('directory operations', () => {
        it('should create and list directory contents', async () => {
            const fs = new MockFileSystem();
            await fs.mkdir('testdir');
            const file = await createMockFile('content');
            await fs.writeFile('testdir/file.txt', file);

            const contents = await fs.readdir('testdir');
            expect(contents.has('file.txt')).toBe(true);
        });

        it('should create nested directories automatically via writeFile', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('nested');
            await fs.writeFile('auto/created/dirs/file.txt', file);
            
            const readBack = await fs.readFile('auto/created/dirs/file.txt');
            expect(await readContent(readBack as MockFile)).toBe('nested');
        });

        it('should fail when part of path is a file', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('blocking');
            await fs.writeFile('block.txt', file);
            
            const newFile = await createMockFile('fail');
            await expect(fs.writeFile('block.txt/fail.txt', newFile))
                .rejects.toThrow('is a file, cannot traverse');
        });
    });

    describe('file operations', () => {
        it('should delete file with unlink', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('delete me');
            await fs.writeFile('todelete.txt', file);
            
            await fs.unlink('todelete.txt');
            await expect(fs.readFile('todelete.txt'))
                .rejects.toThrow('File not found');
        });

        it('should rename files', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('rename test');
            await fs.writeFile('dir/old.txt', file);
            
            await fs.rename('dir/old.txt', 'new.txt');
            const readBack = await fs.readFile('dir/new.txt');
            
            expect(await readContent(readBack as MockFile)).toBe('rename test');
            await expect(fs.readFile('dir/old.txt')).rejects.toThrow('File not found');
        });

        it('should remove empty directory', async () => {
            const fs = new MockFileSystem();
            await fs.mkdir('empty-dir');
            await fs.rmdir('empty-dir');
            
            const contents = await fs.readdir('/');
            expect(contents.has('empty-dir')).toBe(false);
        });
    });

    describe('error cases', () => {
        it('should fail when removing non-empty directory', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('preventing delete');
            await fs.writeFile('nonempty/file.txt', file);
            
            await expect(fs.rmdir('nonempty'))
                .rejects.toThrow('is not empty');
        });

        it('should fail when creating directory that exists', async () => {
            const fs = new MockFileSystem();
            await fs.mkdir('existing');
            await expect(fs.mkdir('existing'))
                .rejects.toThrow('already exists');
        });

        it('should fail renaming to existing target', async () => {
            const fs = new MockFileSystem();
            const file1 = await createMockFile('file1');
            const file2 = await createMockFile('file2');
            
            await fs.writeFile('source.txt', file1);
            await fs.writeFile('target.txt', file2);
            
            await expect(fs.rename('source.txt', 'target.txt'))
                .rejects.toThrow('already exists');
        });

        it('should handle empty paths correctly', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('test');
            await expect(fs.writeFile('', file))
                .rejects.toThrow('Path cannot be empty');
        });
    });

    describe('complex scenarios', () => {
        it('should handle deep directory structures', async () => {
            const fs = new MockFileSystem();
            const file = await createMockFile('deep');
            const path = 'very/deep/directory/structure/file.txt';
            
            await fs.writeFile(path, file);
            const readBack = await fs.readFile(path);
            
            expect(await readContent(readBack as MockFile)).toBe('deep');
        });

        it('should maintain file system isolation between instances', async () => {
            const fs1 = new MockFileSystem();
            const fs2 = new MockFileSystem();
            
            const file1 = await createMockFile('one');
            const file2 = await createMockFile('two');
            
            await fs1.writeFile('test.txt', file1);
            await fs2.writeFile('test.txt', file2);
            
            const content1 = await readContent(await fs1.readFile('test.txt') as MockFile);
            const content2 = await readContent(await fs2.readFile('test.txt') as MockFile);
            
            expect(content1).toBe('one');
            expect(content2).toBe('two');
        });
    });
});
