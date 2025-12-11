// @author Tijn Gommers
// @date 2025-19-11

/// <reference lib="dom" />

import { API_BASE, authenticatedFetch, getErrorMessage } from './utils.mjs';

// =========================
// Constants & Initialization
// =========================

// =========================
// DOM Element Selectors
// =========================

let collectionSearch: HTMLInputElement;
let collectionsList: HTMLDivElement;
let refreshCollections: HTMLButtonElement;
let createCollectionButton: HTMLButtonElement;
let deleteCollectionBtn: HTMLButtonElement;
let confirmCreate: HTMLButtonElement;
let confirmDelete: HTMLButtonElement | null;
let collectionNameInput: HTMLInputElement;
let errorDiv: HTMLDivElement;

// =========================
// State Management
// =========================

let allCollections: string[] = [];
let currentlySelectedCollection: string | null = null;

// =========================
// Utility Functions
// =========================

/**
 * Displays an error message that auto-clears after 5 seconds
 * @param {string} msg - The error message to display
 * @return {void}
 */
function showError(msg: string): void {
  errorDiv.textContent = msg;
  setTimeout(() => {
    errorDiv.textContent = '';
  }, 5000);
}

/**
 * Immediately clears any displayed error message
 * @return {void}
 */
function clearError(): void {
  errorDiv.textContent = '';
}

/**
 * Updates the delete button visibility based on collection selection
 * @return {void}
 */
function updateDeleteButtonVisibility(): void {
  deleteCollectionBtn.style.display = currentlySelectedCollection ? 'block' : 'none';
}

// =========================
// API Functions
// =========================

/**
 * Fetches all collection names from the backend API
 * @return {Promise<string[]>} Promise resolving to array of collection names
 * @throws {Error} When API request fails or returns non-ok response
 */
async function fetchCollections(): Promise<string[]> {
  try {
    console.log('Fetching collections from API...');
    const response = await authenticatedFetch(`${API_BASE}/api/fetchCollections`, {
      method: 'GET',
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const collections = (await response.json()) as { collections: string[] };
    console.log('Collections received:', collections);

    return collections.collections;
  } catch (error) {
    console.error('Failed to fetch collections:', error);
    throw error;
  }
}

/**
 * Creates a new collection via API and refreshes the collections list
 * @param {string} name - Name of the collection to create
 * @return {Promise<boolean>} Promise resolving to true if creation successful
 * @throws {Error} When API request fails or collection name is invalid
 */
async function createCollection(name: string): Promise<boolean> {
  try {
    console.log('Creating collection via API:', name);
    const response = await authenticatedFetch(`${API_BASE}/api/createCollection`, {
      method: 'POST',
      body: JSON.stringify({ collectionName: name }),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message?: string };
      throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string };
    console.log('Collection created:', result);

    // Refresh the collections list after successful creation
    await handleRefreshCollections();
    return true;
  } catch (error) {
    console.error('Failed to create collection:', error);
    throw error;
  }
}

/**
 * Deletes a collection via API
 * @param {string} name - Collection name to delete
 * @return {Promise<boolean>} Promise resolving to true if deletion successful
 * @throws {Error} When collection deletion fails
 */
async function deleteCollection(name: string): Promise<boolean> {
  try {
    console.log('Deleting collection via API:', name);

    const response = await authenticatedFetch(`${API_BASE}/api/deleteCollection`, {
      method: 'DELETE',
      body: JSON.stringify({ collectionName: name }),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message?: string };
      throw new Error(errorData.message || `Failed to delete collection ${name}: HTTP ${response.status}`);
    }

    const result = (await response.json()) as { success: boolean; message: string };
    console.log('Collection deleted:', result.message);

    // Clear current selection if this was the selected collection
    if (currentlySelectedCollection === name) {
      currentlySelectedCollection = null;
    }

    // Refresh the collections list
    await handleRefreshCollections();
    return true;
  } catch (error) {
    console.error('Failed to delete collection:', error);
    throw error;
  }
}

// =========================
// Rendering & Selection
// =========================

/**
 * Renders the collections list in the UI without checkboxes, just clickable names
 * @param {string[]} collections - Array of collection names to render
 * @return {void}
 */
function renderCollections(collections: string[]): void {
  collectionsList.innerHTML = '';

  if (collections.length === 0) {
    collectionsList.innerHTML = '<div class="no-results">No collections found</div>';
    return;
  }

  collections.forEach((name) => {
    const item = document.createElement('div');
    item.className = 'collection-item';

    // Add 'selected' class if this is the currently selected collection
    if (currentlySelectedCollection === name) {
      item.classList.add('selected');
    }

    const nameSpan = document.createElement('span');
    nameSpan.className = 'collection-name';
    nameSpan.textContent = name;
    nameSpan.addEventListener('click', () => {
      void handleCollectionClick(name);
    });

    item.appendChild(nameSpan);
    collectionsList.appendChild(item);
  });

  updateDeleteButtonVisibility();
}

/**
 * Handles collection click - toggles selection or navigates to documents
 * @param {string} name - Name of the collection clicked
 * @return {void}
 */
function handleCollectionClick(name: string): void {
  // If already selected, navigate to documents page
  if (currentlySelectedCollection === name) {
    void selectCollection(name);
  } else {
    // Otherwise, select this collection
    currentlySelectedCollection = name;
    renderCollections(allCollections);
  }
}

/**
 * Selects a collection and navigates to documents page after verifying existence
 * @param {string} name - Name of the collection to select
 * @return {void}
 * @throws {Error} Handled internally with fallback navigation to documents page
 */
async function selectCollection(name: string): Promise<void> {
  try {
    console.log('Selecting collection via API:', name);

    // First, verify the collection exists and optionally get document count
    const response = await authenticatedFetch(
      `${API_BASE}/api/fetchDocuments?collectionName=${encodeURIComponent(name)}`,
      {
        method: 'GET',
      },
    );

    if (!response.ok) {
      if (response.status === 404) {
        showError(`Collection '${name}' not found`);
        return;
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const collectionInfo = (await response.json()) as {
      success: boolean;
      message: string;
      documentNames: string[];
    };

    if (collectionInfo.success) {
      console.log(collectionInfo.message);
      // Navigate to documents page with collection parameter
      window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
    } else {
      showError(`Collection '${name}' does not exist`);
    }
  } catch (error) {
    console.error('Failed to select collection:', error);
    // Fallback - still navigate to documents page
    console.log('Falling back to direct navigation');
    window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
  }
}

// =========================
// Event Handlers
// =========================

/**
 * Handles refresh collections button click - loads collections from API and renders them
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleRefreshCollections(): Promise<void> {
  try {
    clearError();
    const collections = await fetchCollections();
    allCollections = collections;
    renderCollections(collections);
  } catch (error) {
    showError('Failed to refresh collections: ' + getErrorMessage(error));
  }
}

/**
 * Handles create collection form submission - validates input and creates collection via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleCreateCollection(): Promise<void> {
  const name = collectionNameInput.value.trim();
  if (!name) {
    showError('Please enter a collection name');
    return;
  }

  try {
    const success = await createCollection(name);
    if (success) {
      collectionNameInput.value = '';
      clearError();
    } else {
      showError('Failed to create collection');
    }
  } catch (error) {
    showError('Error creating collection: ' + getErrorMessage(error));
  }
}

/**
 * Handles delete collection button click - deletes the currently selected collection via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleDeleteCollection(): Promise<void> {
  if (!currentlySelectedCollection) {
    showError('No collection selected');
    return;
  }

  try {
    const success = await deleteCollection(currentlySelectedCollection);
    if (success) {
      clearError();
    } else {
      showError('Failed to delete collection');
    }
  } catch (error) {
    showError('Error deleting collection: ' + getErrorMessage(error));
  }
}

/**
 * Handles search input changes - filters collections list based on search query
 * @return {void}
 */
function handleSearchCollections(): void {
  const query = collectionSearch.value.toLowerCase();
  const filtered = allCollections.filter((name) => name.toLowerCase().includes(query));
  renderCollections(filtered);
}

// =========================
// Event Listeners & Init
// =========================

function initializeApp(): void {
  // Get DOM elements
  collectionSearch = document.getElementById('collectionSearch') as HTMLInputElement;
  collectionsList = document.getElementById('collectionsList') as HTMLDivElement;
  refreshCollections = document.getElementById('refreshCollections') as HTMLButtonElement;
  createCollectionButton = document.getElementById('createCollection') as HTMLButtonElement;
  deleteCollectionBtn = document.getElementById('deleteCollection') as HTMLButtonElement;
  confirmCreate = document.getElementById('confirmCreate') as HTMLButtonElement;
  confirmDelete = document.getElementById('confirmDelete') as HTMLButtonElement | null;
  collectionNameInput = document.getElementById('collectionNameInput') as HTMLInputElement;
  errorDiv = document.getElementById('error') as HTMLDivElement;

  if (!collectionsList || !refreshCollections || !errorDiv) {
    console.error('Required DOM elements not found');
    return;
  }

  console.log('DOM elements loaded');

  // Auth guard and welcome text
  const sessionToken = localStorage.getItem('sessionToken');
  const username = localStorage.getItem('username');
  if (!sessionToken || !username) {
    window.location.href = 'login.html';
    return;
  }

  const welcomeUser = document.getElementById('welcomeUser');
  if (welcomeUser) {
    welcomeUser.textContent = `Welcome, ${username}`;
  }

  const logoutBtn = document.getElementById('logoutBtn');
  logoutBtn?.addEventListener('click', () => {
    localStorage.removeItem('sessionToken');
    localStorage.removeItem('username');
    window.location.href = 'login.html';
  });

  // Set up event listeners
  refreshCollections.addEventListener('click', () => {
    void handleRefreshCollections();
  });

  const modalOverlay = document.getElementById('modalOverlay');
  const deleteModal = document.getElementById('deleteModalOverlay');
  const nameInput = document.getElementById('collectionNameInput') as HTMLInputElement;

  createCollectionButton?.addEventListener('click', () => {
    modalOverlay?.classList.add('show');
    nameInput?.focus();
  });

  confirmCreate?.addEventListener('click', () => {
    void handleCreateCollection();
    // Close the create modal after confirmation
    modalOverlay?.classList.remove('show');
    if (collectionNameInput) collectionNameInput.value = '';
  });

  // Allow Enter key to submit collection creation
  collectionNameInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      confirmCreate?.click();
    }
  });

  if (confirmDelete) {
    confirmDelete.addEventListener('click', () => {
      void handleDeleteCollection();
      // Close the delete modal after confirmation
      deleteModal?.classList.remove('show');
    });
  }

  deleteCollectionBtn?.addEventListener('click', () => {
    deleteModal?.classList.add('show');
  });

  // Close modals when clicking overlay
  modalOverlay?.addEventListener('click', (e) => {
    if (e.target === modalOverlay) {
      modalOverlay.classList.remove('show');
      if (nameInput) nameInput.value = '';
    }
  });

  deleteModal?.addEventListener('click', (e) => {
    if (e.target === deleteModal) {
      deleteModal.classList.remove('show');
    }
  });

  // Close modal on Escape key, confirm delete on Enter key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (modalOverlay?.classList.contains('show')) {
        modalOverlay.classList.remove('show');
        if (nameInput) nameInput.value = '';
      }
      if (deleteModal?.classList.contains('show')) {
        deleteModal.classList.remove('show');
      }
    }
    if (e.key === 'Enter' && deleteModal?.classList.contains('show')) {
      e.preventDefault();
      document.getElementById('confirmDelete')?.click();
    }
  });

  collectionSearch?.addEventListener('input', () => {
    handleSearchCollections();
  });

  // Initialize page
  void (async () => {
    try {
      console.log('Initializing webclient...');
      await handleRefreshCollections();
    } catch (error) {
      console.error('Failed to initialize webclient:', error);
      showError('Failed to load collections. Using offline mode.');

      // Fallback to demo data
      allCollections = ['users', 'sessions', 'products', 'orders'];
      renderCollections(allCollections);
    }
  })();

  console.log('WebClient ready');
}

// Wait for DOM to be ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeApp);
} else {
  initializeApp();
}
