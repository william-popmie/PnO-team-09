// @author Tijn Gommers
// @date 2025-11-12

// =========================
// Constants
// =========================

export const API_BASE = 'http://localhost:3000';

// =========================
// Utility Functions
// =========================

/**
 * Handles token expiration by clearing session and redirecting to login
 * @return {void}
 */
export function handleTokenExpiration(): void {
  console.warn('Token expired or invalid, redirecting to login');
  localStorage.removeItem('sessionToken');
  localStorage.removeItem('username');
  window.location.href = 'login.html';
}

/**
 * Extracts error message from unknown error type
 * @param {unknown} e - Error object or value of unknown type
 * @return {string} Error message string or stringified value
 */
export function getErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// =========================
// Authenticated Fetch Wrapper
// =========================

/**
 * Wrapper function for authenticated API calls that handles token management
 * Automatically:
 * 1. Adds the sessionToken to Authorization header
 * 2. Redirects to login if token is missing or authentication fails
 * 3. Updates localStorage with new token if returned by the server
 *
 * @param {string} url - The API endpoint URL
 * @param {RequestInit} options - Fetch options (method, body, etc.)
 * @return {Promise<Response>} The fetch response
 * @throws {Error} When token is missing or API request fails
 */
export async function authenticatedFetch(url: string, options: RequestInit = {}): Promise<Response> {
  // Get session token
  const sessionToken = localStorage.getItem('sessionToken');

  // If no token, redirect to login
  if (!sessionToken) {
    handleTokenExpiration();
    throw new Error('No authentication token found');
  }

  // Add Authorization header
  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
    Authorization: `Bearer ${sessionToken}`,
  };

  // Make the request
  const response = await fetch(url, {
    ...options,
    headers,
  });

  // Handle authentication errors
  if (response.status === 401 || response.status === 403) {
    handleTokenExpiration();
    throw new Error('Authentication failed');
  }

  // Check for updated token in response
  if (response.ok) {
    try {
      // Clone response to read it without consuming the original
      const clonedResponse = response.clone();
      const data = (await clonedResponse.json()) as { token?: string };

      // Update token if provided
      if (data.token && typeof data.token === 'string' && data.token.length > 0) {
        localStorage.setItem('sessionToken', data.token);
        console.log('Session token refreshed and cached');
      }
    } catch {
      // If response is not JSON, ignore token update
    }
  }

  return response;
}
