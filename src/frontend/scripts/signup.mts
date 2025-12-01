// @author Tijn Gommers
// @date 2025-24-11

/// <reference lib="dom" />

const API_BASE = 'http://localhost:3000';

// Types

export interface SignupRequest {
  username: string;
  password: string;
}

export interface SignupResponse {
  success: boolean;
  message: string;
  token: string;
}

// DOM Elements
const signupForm = document.getElementById('signupForm') as HTMLFormElement;
const usernameInput = document.getElementById('username') as HTMLInputElement;
const passwordInput = document.getElementById('password') as HTMLInputElement;
const confirmPasswordInput = document.getElementById('confirmPassword') as HTMLInputElement;
const signupBtn = document.getElementById('signupBtn') as HTMLButtonElement;
const errorDiv = document.getElementById('error') as HTMLDivElement;

// Utility functions
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
 * Displays a success message to the user in green text
 * @param {string} message - The success message to display
 * @return {void}
 */
function showSuccess(message: string): void {
  errorDiv.style.color = 'var(--success, #22c55e)';
  errorDiv.textContent = message;
}

/**
 * Clears any displayed error or success message
 * @return {void}
 */
function clearError(): void {
  errorDiv.innerHTML = '&nbsp;';
}

/**
 * Validates username according to business rules
 * @param {string} username - The username to validate
 * @return {boolean} True if username is 3-20 chars and contains only letters, numbers, underscores
 */
export function isValidUsername(username: string): boolean {
  // Username validation: 3-20 characters, alphanumeric and underscore only
  if (!username || username.length < 3 || username.length > 20) {
    return false;
  }

  // Only allow letters, numbers, and underscores
  return /^[a-zA-Z0-9_]+$/.test(username);
}

/**
 * Validates password according to security requirements
 * @param {string} password - The password to validate
 * @return {boolean} True if password is at least 8 chars with letters and numbers
 */
export function isValidPassword(password: string): boolean {
  // Password validation: minimum 8 characters with complexity requirements
  if (!password || password.length < 8) {
    return false;
  }

  // Check for at least one letter and one number (basic complexity)
  const hasLetter = /[a-zA-Z]/.test(password);
  const hasNumber = /[0-9]/.test(password);

  return hasLetter && hasNumber;
}

/**
 * Validates that all required signup form fields are filled
 * @return {boolean} True if username, password, and confirm password fields are not empty
 */
function allRequiredFields(): boolean {
  return (usernameInput.value.trim() && passwordInput.value && confirmPasswordInput.value) !== '';
}

/**
 * Calls the backend API to register a new user account
 * @param {LoginRequest} request - Object containing username, password, confirmPassword
 * @return {Promise<SignupResponse>} Promise resolving to signup result with success status and message
 * @throws {Error} When API request fails or returns error response
 */
async function signupUser(request: SignupRequest): Promise<SignupResponse> {
  const response = await fetch(`${API_BASE}/api/signup`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(request),
  });

  const data = (await response.json()) as SignupResponse;

  if (!response.ok) {
    throw new Error(data.message || 'Signup failed');
  }

  if (data.success) {
    // Store session info in cache (localStorage)
    if (data.token && data.token.length > 0) {
      localStorage.setItem('sessionToken', data.token);
    }
  }

  return data;
}

// Handle signup form submission with validation and user registration
signupForm.addEventListener('submit', (e) => {
  void (async () => {
    e.preventDefault();

    const username = usernameInput.value.trim();
    const password = passwordInput.value;
    const confirmPassword = confirmPasswordInput.value;

    // Clear previous error
    clearError();

    // Client-side validation
    if (!allRequiredFields()) {
      showError('All fields are required');
      return;
    }

    if (password !== confirmPassword) {
      showError('Passwords do not match');
      return;
    }

    if (!isValidUsername(username)) {
      showError('Username must be between 3 and 20 characters and contain only letters, numbers, and underscores');
      return;
    }

    if (!isValidPassword(password)) {
      showError('Password must be at least 8 characters long and contain at least one letter and one number');
      return;
    }

    // Disable form while submitting
    signupBtn.disabled = true;
    signupBtn.textContent = 'Creating account...';

    try {
      const result = await signupUser({
        username,
        password,
      });

      if (result.success) {
        showSuccess(result.message + ' Redirecting to collections...');

        // Redirect to collections page after successful signup
        setTimeout(() => {
          window.location.href = 'simpledbmswebclient.html';
        }, 2000);
      } else {
        showError(result.message);
      }
    } catch (error) {
      console.error('Signup error:', error);
      showError(error instanceof Error ? error.message : 'An unexpected error occurred');
    } finally {
      // Re-enable form
      signupBtn.disabled = false;
      signupBtn.textContent = 'Sign Up';
    }
  })(); // End of async IIFE
}); // End of event listener

console.log('✅ Signup script loaded');
