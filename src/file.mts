// @author Tijn Gommers
// @date 2025-15-11

import type { File } from './mockfile.mjs';
import { MockFile } from './mockfile.mjs';

/**
 * Represents an in-memory file system structure where both files and directories can be stored.
 * Files are stored as File instances, directories as nested Maps.
 */
type FileSystemNode = File | MockFileSystem;

// Create the MockFileSystem class first before using it

/**
 * MockFileSystem provides an in-memory implementation of a hierarchical file system.
 * It supports operations like creating/reading files and directories, similar to Node's fs module.
 *
 * @example
 * ```typescript
 * const fs = new MockFileSystem();
 *
 * // Create and write a file
 * const file = new MockFile(512);
 * await file.create();
 * await file.writev([Buffer.from('hello')], 0);
 * await fs.writeFile('dir/test.txt', file);
 *
 * // Read it back
 * const readBack = await fs.readFile('dir/test.txt');
 * ```
 */
export class MockFileSystem {
  private store: Map<string, FileSystemNode>;

  constructor() {
    this.store = new Map();
  }

  /**
   * Internal helper to split and validate a path
   * @param {string} path The file or directory path
   * @returns {string[]} Array of path components
   * @throws {Error} If path is empty or contains invalid characters
   */
  private validateAndSplitPath(path: string): string[] {
    if (!path) throw new Error('Path cannot be empty');
    return path.split('/').filter((p) => p.length > 0);
  }

  /**
   * Internal helper to traverse the file system to a specific path
   * @param {string} path The file or directory path
   * @returns {[Map<string,FileSystemNode>,string]} Tuple of [parent directory Map, final path component]
   * @throws {Error} If any parent component is a file or doesn't exist
   */
  private traverseToParent(path: string): [Map<string, FileSystemNode>, string] {
    const parts = this.validateAndSplitPath(path);
    if (parts.length === 0) throw new Error('Invalid path');

    const filename = parts[parts.length - 1];
    let current: Map<string, FileSystemNode> = this.store;

    // Traverse all but the last component
    for (const part of parts.slice(0, -1)) {
      const node = current.get(part);
      if (!node) {
        // Create missing parent directories
        const newDir = new MockFileSystem();
        current.set(part, newDir);
        current = newDir.store;
      } else if (node instanceof MockFileSystem) {
        current = node.store;
      } else {
        throw new Error(`Path component "${part}" is a file, cannot traverse`);
      }
    }

    return [current, filename];
  }

  /**
   * Writes a file to the specified path, creating parent directories as needed
   * @param {string} path The file path where to write the file
   * @param {File} file The File instance to write
   * @returns {Promise<void>}
   */
  async writeFile(path: string, file: File): Promise<void> {
    const [parent, filename] = this.traverseToParent(path);

    // Create a new MockFile to store (copying contents if needed)
    const stats = await file.stat();
    const stored = new MockFile(file.sectorSize);
    await stored.create();

    if (stats.size > 0) {
      const buffer = Buffer.alloc(stats.size);
      await file.read(buffer, { position: 0 });
      await stored.writev([buffer], 0);
      await stored.sync();
    }

    parent.set(filename, stored);
  }

  /**
   * Reads a file from the specified path
   * @param {string} path The file path to read
   * @returns {Promise<File>} file instance at the given path
   * @throws {Error} If file doesn't exist or path points to a directory
   */
  async readFile(path: string): Promise<File> {
    const [parent, filename] = this.traverseToParent(path);
    const node = parent.get(filename);

    if (!node) {
      throw new Error(`File not found: "${path}"`);
    }
    if (node instanceof MockFileSystem) {
      throw new Error(`Path "${path}" points to a directory, not a file`);
    }

    return Promise.resolve(node);
  }

  /**
   * Lists contents of a directory
   * @param {string} path The directory path to read
   * @returns {Promise<Map<string, FileSystemNode>>} Map of entry names to their FileSystemNode (File or MockFileSystem)
   * @throws {Error} If path doesn't exist or points to a file
   */
  async readdir(path: string): Promise<Map<string, FileSystemNode>> {
    if (!path || path === '/') return Promise.resolve(this.store);

    const [parent, dirname] = this.traverseToParent(path);
    const node = parent.get(dirname);

    if (!node) {
      throw new Error(`Directory not found: "${path}"`);
    }
    if (!(node instanceof MockFileSystem)) {
      throw new Error(`Path "${path}" points to a file, not a directory`);
    }

    return Promise.resolve(node.store);
  }

  /**
   * Creates a new directory at the specified path
   * Parent directories must exist
   * @param {string} path The directory path to create
   * @returns {Promise<void>}
   * @throws {Error} If path already exists
   */
  async mkdir(path: string): Promise<void> {
    const [parent, dirname] = this.traverseToParent(path);

    if (parent.has(dirname)) {
      throw new Error(`Path already exists: "${path}"`);
    }

    parent.set(dirname, new MockFileSystem());
    return Promise.resolve();
  }

  /**
   * Removes a file from the file system
   * @param {string} path The file path to remove
   * @returns {Promise<void>}
   * @throws {Error} If path doesn't exist or points to a directory
   */
  async unlink(path: string): Promise<void> {
    const [parent, filename] = this.traverseToParent(path);
    const node = parent.get(filename);

    if (!node) {
      throw new Error(`File not found: "${path}"`);
    }
    if (node instanceof MockFileSystem) {
      throw new Error(`Path "${path}" points to a directory, use rmdir instead`);
    }

    parent.delete(filename);
    return Promise.resolve();
  }

  /**
   * Removes an empty directory
   * @param {string} path The directory path to remove
   * @returns {Promise<void>}
   * @throws {Error} If path doesn't exist or points to a file
   * @throws {Error} If directory is not empty
   */
  async rmdir(path: string): Promise<void> {
    const [parent, dirname] = this.traverseToParent(path);
    const node = parent.get(dirname);

    if (!node) {
      throw new Error(`Directory not found: "${path}"`);
    }
    if (!(node instanceof MockFileSystem)) {
      throw new Error(`Path "${path}" points to a file, use unlink instead`);
    }

    // Check if directory is empty
    if (node.store.size > 0) {
      throw new Error(`Directory "${path}" is not empty`);
    }

    parent.delete(dirname);
    return Promise.resolve();
  }

  /**
   * Renames a file or directory
   * Note: This only handles renaming within the same directory
   * @param {string} oldPath The current path of the file or directory
   * @param {string} newName The new name for the file or directory
   * @returns {Promise<void>}
   * @throws {Error} If oldPath doesn't exist
   * @throws {Error} If newName already exists in the same directory
   */
  async rename(oldPath: string, newName: string): Promise<void> {
    const [parent, oldName] = this.traverseToParent(oldPath);
    const node = parent.get(oldName);

    if (!node) {
      throw new Error(`Path not found: "${oldPath}"`);
    }
    if (parent.has(newName)) {
      throw new Error(`Target already exists: "${newName}"`);
    }

    parent.delete(oldName);
    parent.set(newName, node);
    return Promise.resolve();
  }

  /**
   * Clears all contents of the file system
   */
  clear(): void {
    this.store.clear();
  }
}
