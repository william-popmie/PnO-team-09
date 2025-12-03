// @author William Ragnarsson
// @date 2025-12-03

import { describe, it, expect } from 'vitest';
import { generateToken } from './authentication.mjs';

describe('Authentication Module', () => {
  describe('generateToken', () => {
    it('should generate a valid JWT token', () => {
      const token = generateToken('user123', 'testuser');

      expect(token).toBeDefined();
      expect(typeof token).toBe('string');
      expect(token.split('.')).toHaveLength(3); // JWT has 3 parts
    });

    it('should generate different tokens for different users', () => {
      const token1 = generateToken('user1', 'alice');
      const token2 = generateToken('user2', 'bob');

      expect(token1).not.toBe(token2);
    });
  });
});
