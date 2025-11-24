// @author Tijn Gommers
// @date 2025-19-11

import type { User, Session } from './signup.mjs';
import { isValidUsername, isValidPassword } from './signup.mjs';

export interface LoginRequest {
  username: string;
  password: string;
}

/// <reference lib="dom" />

// DOM elements from login.html
const formTitle = document.getElementById('formTitle') as HTMLHeadingElement;
const authForm = document.getElementById('authForm') as HTMLFormElement;
const usernameInput = document.getElementById('username') as HTMLInputElement;
const passwordInput = document.getElementById('password') as HTMLInputElement;
const submitBtn = document.getElementById('submitBtn') as HTMLButtonElement;
const errorDiv = document.getElementById('error') as HTMLDivElement;

// Utility functions

function showError(message: string) {
  errorDiv.style.color = 'var(--error, #ef4444)';
  errorDiv.textContent = message;
}

function showSuccess(message: string) {
  errorDiv.style.color = 'var(--success, #22c55e)';
  errorDiv.textContent = message;
}

function clearError() {
  errorDiv.innerHTML = '&nbsp;';
}

function allRequiredFields(): boolean {
  return (usernameInput.value.trim() && passwordInput.value) !== '';
}

submitBtn.addEventListener('click', async (e) => {
  e.preventDefault();

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
});
