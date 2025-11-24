// @author Tijn Gommers
// @date 2025-19-11

import * as bcrypt from 'bcryptjs';

// Bcrypt interface to handle type safety
interface BcryptModule {
  hash(data: string, saltOrRounds: string | number): Promise<string>;
  compare(data: string, encrypted: string): Promise<boolean>;
}

const bcryptSafe = bcrypt as unknown as BcryptModule;

// Types
interface User {
  id: string;
  username: string;
  passwordHash: string;
  createdAt: string;
  lastLogin?: string;
}

interface Session {
  id: string;
  userId: string;
  createdAt: string;
  expiresAt: string;
}

interface LoginRequest {
  username: string;
  password: string;
}

interface RegisterRequest {
  username: string;
  password: string;
  confirmPassword: string;
}

// Configuration
const SESSION_DURATION = 24 * 60 * 60 * 1000; // 24 hours
const SALT_ROUNDS = 12;
const STORAGE_KEYS = {
  users: 'simpledbms_users',
  sessions: 'simpledbms_sessions',
};

// Utility functions
function generateId(): string {
  return 'user_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

function generateSessionToken(): string {
  return 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

function isValidUsername(username: string): boolean {
  // Username validation: 3-20 characters, alphanumeric and underscore only
  if (!username || username.length < 3 || username.length > 20) {
    return false;
  }

  // Only allow letters, numbers, and underscores
  return /^[a-zA-Z0-9_]+$/.test(username);
}

function isValidPassword(password: string): boolean {
  // Password validation: minimum 8 characters with complexity requirements
  if (!password || password.length < 8) {
    return false;
  }

  // Check for at least one letter and one number (basic complexity)
  const hasLetter = /[a-zA-Z]/.test(password);
  const hasNumber = /[0-9]/.test(password);

  return hasLetter && hasNumber;
}

// Storage operations
// ⚠️  SECURITY WARNING: This demo stores hashed passwords in localStorage
// In production, use a secure backend database and HTTPS!
function ensureDataDirectory(): void {
  // Initialize localStorage if needed
  if (!localStorage.getItem(STORAGE_KEYS.users)) {
    localStorage.setItem(STORAGE_KEYS.users, JSON.stringify([]));
  }
  if (!localStorage.getItem(STORAGE_KEYS.sessions)) {
    localStorage.setItem(STORAGE_KEYS.sessions, JSON.stringify([]));
  }
}

function loadUsers(): User[] {
  try {
    const data = localStorage.getItem(STORAGE_KEYS.users);
    return data ? (JSON.parse(data) as User[]) : [];
  } catch (error) {
    console.error('Failed to load users:', error);
    return [];
  }
}

function saveUsers(users: User[]): void {
  try {
    localStorage.setItem(STORAGE_KEYS.users, JSON.stringify(users));
  } catch (error) {
    console.error('Failed to save users:', error);
    throw new Error('Failed to save users');
  }
}

function loadSessions(): Session[] {
  try {
    const data = localStorage.getItem(STORAGE_KEYS.sessions);
    return data ? (JSON.parse(data) as Session[]) : [];
  } catch (error) {
    console.error('Failed to load sessions:', error);
    return [];
  }
}

function saveSessions(sessions: Session[]): void {
  try {
    localStorage.setItem(STORAGE_KEYS.sessions, JSON.stringify(sessions));
  } catch (error) {
    console.error('Failed to save sessions:', error);
    throw new Error('Failed to save sessions');
  }
}

// User management
async function createUser(username: string, password: string): Promise<User> {
  const id = generateId();

  try {
    const passwordHash = await bcryptSafe.hash(password, SALT_ROUNDS);
    if (typeof passwordHash !== 'string') {
      throw new Error('Hash function returned invalid type');
    }

    return {
      id,
      username,
      passwordHash,
      createdAt: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Password hashing failed:', error);
    throw new Error('Failed to create user: password hashing failed');
  }
}

function findUserByUsername(username: string): User | null {
  const users = loadUsers();
  return users.find((user) => user.username.toLowerCase() === username.toLowerCase()) || null;
}

function findUserById(id: string): User | null {
  const users = loadUsers();
  return users.find((user) => user.id === id) || null;
}

async function verifyPassword(password: string, hash: string): Promise<boolean> {
  try {
    const result = await bcryptSafe.compare(password, hash);
    if (typeof result !== 'boolean') {
      console.error('Compare function returned invalid type:', typeof result);
      return false;
    }
    return result;
  } catch (error) {
    console.error('Password verification error:', error);
    return false;
  }
}

// Session management
function createSession(userId: string): Session {
  const id = generateSessionToken();
  const createdAt = new Date().toISOString();
  const expiresAt = new Date(Date.now() + SESSION_DURATION).toISOString();

  return {
    id,
    userId,
    createdAt,
    expiresAt,
  };
}

function findSessionById(sessionId: string): Session | null {
  const sessions = loadSessions();
  const session = sessions.find((s) => s.id === sessionId);

  if (!session) {
    return null;
  }

  // Check if session is expired
  if (new Date(session.expiresAt) < new Date()) {
    deleteSession(sessionId);
    return null;
  }

  return session;
}

function cleanupExpiredSessions(): void {
  const sessions = loadSessions();
  const now = new Date();
  const validSessions = sessions.filter((session) => new Date(session.expiresAt) > now);

  if (validSessions.length !== sessions.length) {
    saveSessions(validSessions);
  }
}

function deleteSession(sessionId: string): void {
  const sessions = loadSessions();
  const filteredSessions = sessions.filter((session) => session.id !== sessionId);
  saveSessions(filteredSessions);
}

// Authentication API
export async function registerUser(
  request: RegisterRequest,
): Promise<{ success: boolean; message: string; user?: User }> {
  console.log('🔐 Attempting to register user:', request.username);

  try {
    // Validate input
    if (!isValidUsername(request.username)) {
      return {
        success: false,
        message: 'Invalid username. Must be 3-20 characters, letters, numbers and underscore only.',
      };
    }

    if (!isValidPassword(request.password)) {
      return {
        success: false,
        message: 'Password must be at least 8 characters with letters and numbers.',
      };
    }

    if (request.password !== request.confirmPassword) {
      return { success: false, message: 'Passwords do not match.' };
    }

    // Check if user exists
    const existingUser = findUserByUsername(request.username);
    if (existingUser) {
      return { success: false, message: 'Username already exists.' };
    }

    // Create user
    ensureDataDirectory();
    const user = await createUser(request.username, request.password);
    const users = loadUsers();
    users.push(user);
    saveUsers(users);

    console.log('✅ User registered successfully:', user.username);
    logUserStats();

    return { success: true, message: 'User created successfully.', user };
  } catch (error) {
    console.error('Registration error:', error);
    return { success: false, message: 'Registration failed. Please try again.' };
  }
}

export async function loginUser(
  request: LoginRequest,
): Promise<{ success: boolean; message: string; sessionId?: string; user?: User }> {
  console.log('🔑 Attempting to login user:', request.username);

  try {
    const user = findUserByUsername(request.username);
    if (!user) {
      return { success: false, message: 'Invalid username or password.' };
    }

    const isValid = await verifyPassword(request.password, user.passwordHash);
    if (!isValid) {
      return { success: false, message: 'Invalid username or password.' };
    }

    // Create session
    const session = createSession(user.id);
    const sessions = loadSessions();
    sessions.push(session);
    saveSessions(sessions);

    // Update last login
    user.lastLogin = new Date().toISOString();
    const users = loadUsers();
    const userIndex = users.findIndex((u) => u.id === user.id);
    if (userIndex >= 0) {
      users[userIndex] = user;
      saveUsers(users);
    }

    // Clean up expired sessions
    cleanupExpiredSessions();

    console.log('✅ User logged in successfully:', user.username, 'Session:', session.id);

    return { success: true, message: 'Login successful.', sessionId: session.id, user };
  } catch (error) {
    console.error('Login error:', error);
    return { success: false, message: 'Login failed. Please try again.' };
  }
}

export function logoutUser(sessionId: string): { success: boolean; message: string } {
  try {
    deleteSession(sessionId);
    return { success: true, message: 'Logout successful.' };
  } catch (error) {
    console.error('Logout error:', error);
    return { success: false, message: 'Logout failed.' };
  }
}

export function validateSession(sessionId: string): { valid: boolean; user?: User } {
  try {
    const session = findSessionById(sessionId);
    if (!session) {
      return { valid: false };
    }

    const user = findUserById(session.userId);
    if (!user) {
      deleteSession(sessionId);
      return { valid: false };
    }

    return { valid: true, user };
  } catch (error) {
    console.error('Session validation error:', error);
    return { valid: false };
  }
}

// Debugging helper function
function logUserStats(): void {
  const users = loadUsers();
  const sessions = loadSessions();
  console.log('📊 User Stats:', {
    totalUsers: users.length,
    activeSessions: sessions.length,
    usernames: users.map((u) => u.username),
  });
}

// Initialize localStorage on module load
ensureDataDirectory();
console.log('🚀 SimpleDBMS Authentication System initialized');
logUserStats();

// DOM Handling for both login and signup pages
document.addEventListener('DOMContentLoaded', () => {
  console.log('🚀 Login script loaded with bcrypt support');

  // Check which page we're on
  const isSignupPage = document.getElementById('signupForm') !== null;
  const isLoginPage = document.getElementById('authForm') !== null;

  console.log('📄 Page type:', isSignupPage ? 'Signup' : isLoginPage ? 'Login' : 'Unknown');

  if (isSignupPage) {
    setupSignupPage();
  } else if (isLoginPage) {
    setupLoginPage();
  }
});

// Signup page setup
function setupSignupPage(): void {
  console.log('🔧 Setting up signup page...');

  const signupForm = document.getElementById('signupForm') as HTMLFormElement;
  const usernameInput = document.getElementById('username') as HTMLInputElement;
  const passwordInput = document.getElementById('password') as HTMLInputElement;
  const confirmPasswordInput = document.getElementById('confirmPassword') as HTMLInputElement;
  const signupBtn = document.getElementById('signupBtn') as HTMLButtonElement;
  const errorDiv = document.getElementById('error') as HTMLDivElement;

  if (!signupForm || !usernameInput || !passwordInput || !confirmPasswordInput || !signupBtn || !errorDiv) {
    console.error('❌ Missing required elements for signup page');
    return;
  }

  signupForm.addEventListener('submit', (event) => {
    event.preventDefault();
    console.log('📝 Signup form submitted');

    const username = usernameInput.value.trim();
    const password = passwordInput.value;
    const confirmPassword = confirmPasswordInput.value;

    console.log('👤 Attempting to register user:', username);

    // Disable form
    signupBtn.disabled = true;
    signupBtn.textContent = 'Creating account...';
    errorDiv.innerHTML = '&nbsp;';

    // Use async immediately invoked function expression (IIFE)
    void (async () => {
      try {
        const result = await registerUser({ username, password, confirmPassword });
        console.log('📊 Registration result:', result);

        if (result.success) {
          errorDiv.style.color = 'var(--success, #22c55e)';
          errorDiv.textContent = result.message + ' Redirecting...';

          // Auto-login after registration
          const loginResult = await loginUser({ username, password });
          console.log('🔑 Auto-login result:', loginResult);

          if (loginResult.success && loginResult.sessionId) {
            localStorage.setItem('sessionToken', loginResult.sessionId);
            localStorage.setItem('username', username);

            setTimeout(() => {
              console.log('🔄 Redirecting to main application...');
              window.location.href = 'simpledbmswebclient.html';
            }, 1500);
          }
        } else {
          errorDiv.style.color = 'var(--error, #ef4444)';
          errorDiv.textContent = result.message;
          console.log('❌ Registration failed:', result.message);
        }
      } catch (error) {
        console.error('💥 Signup error:', error);
        errorDiv.style.color = 'var(--error, #ef4444)';
        errorDiv.textContent = 'Registration failed. Please try again.';
      } finally {
        signupBtn.disabled = false;
        signupBtn.textContent = 'Sign Up';
      }
    })();
  });
}

// Login page setup
function setupLoginPage(): void {
  console.log('🔧 Setting up login page...');

  const authForm = document.getElementById('authForm') as HTMLFormElement;
  const usernameInput = document.getElementById('username') as HTMLInputElement;
  const passwordInput = document.getElementById('password') as HTMLInputElement;
  const submitBtn = document.getElementById('submitBtn') as HTMLButtonElement;
  const errorDiv = document.getElementById('error') as HTMLDivElement;

  if (!authForm || !usernameInput || !passwordInput || !submitBtn || !errorDiv) {
    console.error('❌ Missing required elements for login page');
    return;
  }

  // Check for existing session
  const sessionToken = localStorage.getItem('sessionToken');
  const storedUsername = localStorage.getItem('username');

  if (sessionToken && storedUsername) {
    console.log('🔍 Checking existing session for:', storedUsername);
    const validation = validateSession(sessionToken);
    if (validation.valid) {
      console.log('✅ Valid session found, redirecting...');
      window.location.href = 'simpledbmswebclient.html';
      return;
    } else {
      console.log('❌ Invalid session, clearing storage');
      localStorage.removeItem('sessionToken');
      localStorage.removeItem('username');
    }
  }

  authForm.addEventListener('submit', (event) => {
    event.preventDefault();
    console.log('🔑 Login form submitted');

    const username = usernameInput.value.trim();
    const password = passwordInput.value;

    console.log('👤 Attempting to login user:', username);

    // Disable form
    submitBtn.disabled = true;
    submitBtn.textContent = 'Logging in...';
    errorDiv.innerHTML = '&nbsp;';

    // Use async immediately invoked function expression (IIFE)
    void (async () => {
      try {
        const result = await loginUser({ username, password });
        console.log('📊 Login result:', result);

        if (result.success && result.sessionId) {
          errorDiv.style.color = 'var(--success, #22c55e)';
          errorDiv.textContent = result.message + ' Redirecting...';

          localStorage.setItem('sessionToken', result.sessionId);
          localStorage.setItem('username', username);

          setTimeout(() => {
            console.log('🔄 Redirecting to main application...');
            window.location.href = 'simpledbmswebclient.html';
          }, 1000);
        } else {
          errorDiv.style.color = 'var(--error, #ef4444)';
          errorDiv.textContent = result.message;
          console.log('❌ Login failed:', result.message);
        }
      } catch (error) {
        console.error('💥 Login error:', error);
        errorDiv.style.color = 'var(--error, #ef4444)';
        errorDiv.textContent = 'Login failed. Please try again.';
      } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Login';
      }
    })();
  });
}
