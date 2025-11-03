import type { File } from './mockfile.mjs';
import { MockFile } from './mockfile.mjs';

/**
 * @param path The path to the file
 * @param file The MockFile object representing the file
 * Writes a MockFile (in-memory only, no real disk operations)
 */
export async function writeFile(path: string, file: File): Promise<void> {
  const stats = await file.stat();
  const size = stats.size;
  const buffer = Buffer.alloc(size);
  await file.read(buffer, { position: 0 });
  console.log(`[TEST] Writing ${size} bytes to "${path}" (in-memory, no disk)`);
}

/**
 * @param path The path to the file
 * Reads a "file" (in tests, it is populated with test data)
 */
export async function readFile(path: string): Promise<File> {
  const testData = Buffer.from(`Test content for ${path}`);
  const file = new MockFile(512);
  await file.create();
  await file.writev([testData], 0);
  await file.sync();
  console.log(`[TEST] Read "${path}" into MockFile (in-memory)`);
  return file;
}

/**
 * @param path The path to the directory
 * Dummy implementation for reading directory contents
 */
export function readdir(path: string): Promise<string[]> {
  console.log(`[TEST] readdir called on "${path}"`);
  return Promise.resolve(['file1.txt', 'file2.txt']);
}

/**
 * @param path The path to the file
 * Dummy implementation for deleting a file
 */
export function unlink(path: string): Promise<void> {
  console.log(`[TEST] unlink called on "${path}"`);
  return Promise.resolve();
}

/**
 * @param path The path to the directory
 * Dummy implementation for creating a directory
 */
export function mkdir(path: string): Promise<void> {
  console.log(`[TEST] mkdir called on "${path}"`);
  return Promise.resolve();
}

/**
 * @param path The path to the directory
 * Dummy implementation for removing a directory
 */
export function rmdir(path: string): Promise<void> {
  console.log(`[TEST] rmdir called on "${path}"`);
  return Promise.resolve();
}

/**
 * @param oldPath The current path of the file/directory
 * @param newPath The new path to rename/move to
 * Dummy implementation for renaming a file/directory
 */
export function rename(oldPath: string, newPath: string): Promise<void> {
  console.log(`[TEST] rename "${oldPath}" -> "${newPath}"`);
  return Promise.resolve();
}

/**
 * @param path The path to the file
 * @param data Data to append
 * Dummy implementation for appending data to a file
 */
export function appendFile(path: string, data: Buffer | string): Promise<void> {
  console.log(`[TEST] append ${Buffer.isBuffer(data) ? data.length : data.length} bytes to "${path}"`);
  return Promise.resolve();
}
