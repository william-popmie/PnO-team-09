// @author Tijn Gommers
// @date 2025-19-11

// ==========================
//   Imports
// ==========================

import { isValidUsername, isValidPassword } from './signup.mjs';

/// <reference lib="dom" />

// ==========================
//   Types & Interfaces
// ==========================

interface LoginRequest {
  username: string;
  password: string;
  token: string;
}

interface LoginResponse {
  success: boolean;
  message: string;
  token?: string;
}

interface ErrorResponse {
  message?: string;
}

// ==========================
//   DOM Elements
// ==========================

const authForm = document.getElementById('authForm') as HTMLFormElement;
const usernameInput = document.getElementById('username') as HTMLInputElement;
const passwordInput = document.getElementById('password') as HTMLInputElement;
const submitBtn = document.getElementById('submitBtn') as HTMLButtonElement;
const errorDiv = document.getElementById('error') as HTMLDivElement;

// ==========================
//   Utility Functions
// ==========================

/**
 * Displays an error message to the user in red text
 * @param {string} message - The error message to display
 * @return {void}
 */
function showError(message: string): void {
  errorDiv.style.color = 'var(--error, #ef4444)';
  errorDiv.textContent = message;
}

/**
 * Clears any displayed error message by setting it to a non-breaking space
 * @return {void}
 */
function clearError(): void {
  errorDiv.innerHTML = '&nbsp;';
}

/**
 * Validates that all required form fields have been filled in
 * @return {boolean} True if username and password fields are not empty
 */
function allRequiredFields(): boolean {
  return (usernameInput.value.trim() && passwordInput.value) !== '';
}

// ==========================
//   Event Handlers
// ==========================

/**
 * Handles login form submission, validates input, and authenticates via API
 * @param {Event} e - The form submit event
 * @return {Promise<void>}
 */
authForm.addEventListener('submit', (e) => {
  e.preventDefault();

  void (async () => {
    const username = usernameInput.value.trim();
    const password = passwordInput.value;

    clearError();

    if (!allRequiredFields()) {
      showError('All fields are required');
      return;
    }

    if (!isValidUsername(username)) {
      showError('Username must be between 3 and 20 characters and contain only letters, numbers, and underscores');
      return;
    }

    if (!isValidPassword(password)) {
      showError(
        'Password must be at least 8 characters long and include uppercase, lowercase, number, and special character',
      );
      return;
    }
    const token = localStorage.getItem('sessionToken') || '';

    submitBtn.disabled = true;
    submitBtn.textContent = 'Logging in...';

    try {
      const response = await fetch('http://localhost:3000/api/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password, token } as LoginRequest),
      });

      if (!response.ok) {
        const errorData = (await response.json()) as ErrorResponse;
        showError(errorData.message || 'Login failed');
        return;
      }

      const result = (await response.json()) as LoginResponse;
      if (result.success) {
        if (result.token) {
          localStorage.setItem('sessionToken', result.token);
        }

        showError('Login successful! Redirecting...');
        errorDiv.style.color = 'var(--success, #22c55e)';

        // Redirect to main app
        setTimeout(() => {
          window.location.href = 'simpledbmswebclient.html';
        }, 1000);
      } else {
        showError(result.message || 'Login failed');
      }
    } catch (error) {
      console.error('Login error:', error);
      showError(error instanceof Error ? error.message : 'An unexpected error occurred');
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = 'Log In';
    }
  })(); // End async IIFE
}); // End event listener

// ==========================
//   Init
// ==========================

console.log('✅ Login script loaded');
