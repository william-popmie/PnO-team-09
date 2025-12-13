// @author William Ragnarsson
// @date 2025-12-12

// ==========================
//   Imports
// ==========================

import { API_BASE, handleTokenExpiration, getErrorMessage, authenticatedFetch } from './utils.mjs';

// ==========================
//   Constants & Initialization
// ==========================

console.log('Dashboard loading...', 'API:', API_BASE);

// ==========================
//   Types & Interfaces
// ==========================

interface UserDataResponse {
  success: boolean;
  message: string;
  userData?: {
    userId: string;
    username: string;
    hashedPassword: string;
  };
  token?: string;
}

// ==========================
//   DOM Elements
// ==========================

const welcomeUser = document.getElementById('welcomeUser') as HTMLSpanElement;
const logoutBtn = document.getElementById('logoutBtn') as HTMLButtonElement;
const refreshData = document.getElementById('refreshData') as HTMLButtonElement;
const userDataView = document.getElementById('userDataView') as HTMLDivElement;
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
 * Displays a success message to the user in green text
 * @param {string} message - The success message to display
 * @return {void}
 */
function showSuccess(message: string): void {
  errorDiv.style.color = 'var(--success, #22c55e)';
  errorDiv.textContent = message;
}

/**
 * Clears any displayed message by setting it to a non-breaking space
 * @return {void}
 */
function clearMessage(): void {
  errorDiv.innerHTML = '&nbsp;';
}

// ==========================
//   API Functions
// ==========================

/**
 * Fetches the user's personal data from the backend
 * @return {Promise<void>}
 */
async function fetchUserData(): Promise<void> {
  try {
    clearMessage();
    userDataView.innerHTML = '<div style="color: var(--muted); font-size: 14px; padding: 16px">Loading...</div>';

    const response = await authenticatedFetch(`${API_BASE}/api/getUserData`, {
      method: 'GET',
    });

    if (!response.ok) {
      const data = (await response.json()) as UserDataResponse;
      throw new Error(data.message || 'Failed to fetch user data');
    }

    const data = (await response.json()) as UserDataResponse;

    if (data.success && data.userData) {
      displayUserData(data.userData);
      showSuccess('User data loaded successfully');
    } else {
      throw new Error(data.message || 'Failed to load user data');
    }
  } catch (error) {
    console.error('Error fetching user data:', error);
    showError(`Error: ${getErrorMessage(error)}`);
    userDataView.innerHTML =
      '<div style="color: var(--error); font-size: 14px; padding: 16px">Failed to load user data</div>';
  }
}

/**
 * Displays the user data in a formatted view
 * @param {Object} userData - The user data object containing userId, username, and hashedPassword
 * @return {void}
 */
function displayUserData(userData: { userId: string; username: string; hashedPassword: string }): void {
  userDataView.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 24px">
      <div class="user-data-item">
        <label style="font-weight: 600; color: var(--muted); font-size: 14px; margin-bottom: 8px; display: block">
          User ID
        </label>
        <div style="padding: 14px; background: #1e293b; border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1); font-family: 'Courier New', monospace; color: var(--accent)">
          ${escapeHtml(userData.userId)}
        </div>
      </div>

      <div class="user-data-item">
        <label style="font-weight: 600; color: var(--muted); font-size: 14px; margin-bottom: 8px; display: block">
          Username
        </label>
        <div style="padding: 14px; background: #1e293b; border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1)">
          ${escapeHtml(userData.username)}
        </div>
      </div>

      <div class="user-data-item">
        <label style="font-weight: 600; color: var(--muted); font-size: 14px; margin-bottom: 8px; display: block">
          Password (Hashed)
        </label>
        <div style="padding: 14px; background: #1e293b; border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.1); font-family: 'Courier New', monospace; word-break: break-all; font-size: 12px; color: var(--muted)">
          ${escapeHtml(userData.hashedPassword)}
        </div>
        <p style="color: var(--muted); font-size: 12px; margin-top: 8px; font-style: italic">
          Your password is securely hashed and cannot be reversed to the original text.
        </p>
      </div>
    </div>
  `;
}

/**
 * Escapes HTML special characters to prevent XSS attacks
 * @param {string} text - The text to escape
 * @return {string} The escaped text
 */
function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// ==========================
//   Event Handlers
// ==========================

/**
 * Initialize the dashboard on page load
 */
document.addEventListener('DOMContentLoaded', () => {
  const username = localStorage.getItem('username');
  if (!username) {
    handleTokenExpiration();
    return;
  }

  welcomeUser.textContent = `Welcome, ${username}`;

  // Load user data on page load
  void fetchUserData();
});

/**
 * Refresh button handler
 */
refreshData.addEventListener('click', () => {
  void fetchUserData();
});

/**
 * Logout button handler
 */
logoutBtn.addEventListener('click', () => {
  localStorage.removeItem('sessionToken');
  localStorage.removeItem('username');
  window.location.href = 'login.html';
});
