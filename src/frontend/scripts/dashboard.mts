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
const downloadAllData = document.getElementById('downloadAllData') as HTMLButtonElement;
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
    <div style="display: flex; flex-direction: column; gap: 20px">
      <div>
        <label style="font-weight: 600; color: var(--muted); font-size: 13px; margin-bottom: 6px; display: block">
          User ID
        </label>
        <div style="padding: 12px; background: #1e293b; border-radius: 6px; border: 1px solid rgba(255, 255, 255, 0.1); font-family: monospace; color: var(--accent); font-size: 14px">
          ${escapeHtml(userData.userId)}
        </div>
      </div>

      <div>
        <label style="font-weight: 600; color: var(--muted); font-size: 13px; margin-bottom: 6px; display: block">
          Username
        </label>
        <div style="padding: 12px; background: #1e293b; border-radius: 6px; border: 1px solid rgba(255, 255, 255, 0.1); font-size: 14px">
          ${escapeHtml(userData.username)}
        </div>
      </div>

      <div>
        <label style="font-weight: 600; color: var(--muted); font-size: 13px; margin-bottom: 6px; display: block">
          Password (Hashed)
        </label>
        <div style="position: relative">
          <div id="passwordField" style="padding: 12px 68px 12px 12px; background: #1e293b; border-radius: 6px; border: 1px solid rgba(255, 255, 255, 0.1); font-family: monospace; word-break: break-all; font-size: 11px; color: var(--muted)" data-password="${escapeHtml(userData.hashedPassword)}">
            ••••••••••••••••••••••••••••••••••••••••••••••••
          </div>
          <button id="togglePassword" style="position: absolute; right: 8px; top: 50%; transform: translateY(-50%); background: transparent; border: none; cursor: pointer; padding: 6px 10px; color: var(--accent); font-size: 12px; font-weight: 600; text-decoration: underline" title="Toggle password visibility">
            <span id="toggleText">Show</span>
          </button>
        </div>
        <p style="color: var(--muted); font-size: 12px; margin-top: 8px; font-style: italic">
          Note: We do not store your original password. This is a securely hashed version that cannot be reversed.
        </p>
      </div>
    </div>
  `;

  // Add event listener for password toggle
  const toggleBtn = document.getElementById('togglePassword');
  const passwordField = document.getElementById('passwordField');
  const toggleText = document.getElementById('toggleText');
  let isPasswordVisible = false;

  if (toggleBtn && passwordField && toggleText) {
    toggleBtn.addEventListener('click', () => {
      isPasswordVisible = !isPasswordVisible;
      const actualPassword = passwordField.getAttribute('data-password') || '';

      if (isPasswordVisible) {
        passwordField.textContent = actualPassword;
        toggleText.textContent = 'Hide';
        toggleBtn.setAttribute('title', 'Hide password');
      } else {
        passwordField.textContent = '••••••••••••••••••••••••••••••••••••••••••••••••';
        toggleText.textContent = 'Show';
        toggleBtn.setAttribute('title', 'Show password');
      }
    });
  }
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

/**
 * Downloads all user data including collections and documents as a JSON file
 * @return {Promise<void>}
 */
async function downloadAllStoredData(): Promise<void> {
  try {
    clearMessage();
    showSuccess('Preparing your data for download...');

    const response = await authenticatedFetch(`${API_BASE}/api/getAllUserData`, {
      method: 'GET',
    });

    if (!response.ok) {
      const data = (await response.json()) as { message?: string };
      throw new Error(data.message || 'Failed to fetch all user data');
    }

    const allData = (await response.json()) as Record<string, unknown>;

    // Create a blob from the JSON data
    const jsonString = JSON.stringify(allData, null, 2);
    const blob = new Blob([jsonString], { type: 'application/json' });

    // Create download link and trigger download
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `user-data-${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);

    showSuccess('Your data has been downloaded successfully!');
  } catch (error) {
    console.error('Error downloading data:', error);
    showError(`Error: ${getErrorMessage(error)}`);
  }
}

// ==========================
//   Event Handlers
// ==========================

/**
 * Initialize the dashboard on page load
 */
document.addEventListener('DOMContentLoaded', () => {
  const sessionToken = localStorage.getItem('sessionToken');
  const username = localStorage.getItem('username');
  if (!sessionToken || !username) {
    handleTokenExpiration();
    return;
  }

  welcomeUser.textContent = `Welcome, ${username}`;

  // Load user data on page load
  void fetchUserData();
});

/**
 * Download all data button handler
 */
downloadAllData.addEventListener('click', () => {
  void downloadAllStoredData();
});

/**
 * Logout button handler
 */
logoutBtn.addEventListener('click', () => {
  localStorage.removeItem('sessionToken');
  localStorage.removeItem('username');
  window.location.href = 'login.html';
});
