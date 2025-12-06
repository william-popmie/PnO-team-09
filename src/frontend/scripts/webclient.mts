// @author Tijn Gommers
// @date 2025-19-11

/// <reference lib="dom" />

// =========================
// Constants & Initialization
// =========================

const API_BASE = 'http://localhost:3000';

// =========================
// DOM Element Selectors
// =========================

let collectionSearch: HTMLInputElement;
let collectionsList: HTMLDivElement;
let refreshCollections: HTMLButtonElement;
let createCollectionButton: HTMLButtonElement;
let selectedCount: HTMLSpanElement;
let deleteSelected: HTMLButtonElement;
let confirmCreate: HTMLButtonElement;
let confirmDelete: HTMLButtonElement | null;
let collectionNameInput: HTMLInputElement;
let errorDiv: HTMLDivElement;

// =========================
// State Management
// =========================

const selectedCollections = new Set<string>();
let allCollections: string[] = [];

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
 * Extracts error message from unknown error type
 * @param {unknown} e - Error object or value of unknown type
 * @return {string} Error message string or stringified value
 */
function getErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * Updates the UI to reflect current collection selection state
 * Shows/hides selection count and delete button based on selected items
 * @return {void}
 */
function updateSelectionUI(): void {
  const count = selectedCollections.size;
  selectedCount.textContent = count > 0 ? `${count} selected` : '';
  deleteSelected.style.display = count > 0 ? 'block' : 'none';
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
    console.log('🔄 Fetching collections from API...');
    const response = await fetch(`${API_BASE}/api/fetchCollections`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const collections = (await response.json()) as { collections: string[]; token?: string };
    console.log('✅ Collections received:', collections);

    // If a new token is returned, update it in localStorage
    if (collections.token && typeof collections.token === 'string' && collections.token.length > 0) {
      localStorage.setItem('sessionToken', collections.token);
      console.log('🔑 Session token refreshed and cached');
    }

    return collections.collections;
  } catch (error) {
    console.error('❌ Failed to fetch collections:', error);
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
    console.log('🔧 Creating collection via API:', name);
    const response = await fetch(`${API_BASE}/api/createCollection`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
      body: JSON.stringify({ collectionName: name }),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message?: string };
      throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string; token?: string };
    console.log('✅ Collection created:', result);

    // If a new token is returned, update it in localStorage
    if (result.token && typeof result.token === 'string' && result.token.length > 0) {
      localStorage.setItem('sessionToken', result.token);
      console.log('🔑 Session token refreshed and cached');
    }

    // Refresh the collections list after successful creation
    await handleRefreshCollections();
    return true;
  } catch (error) {
    console.error('❌ Failed to create collection:', error);
    throw error;
  }
}

/**
 * Deletes multiple collections via API using parallel requests
 * @param {string[]} names - Array of collection names to delete
 * @return {Promise<boolean>} Promise resolving to true if all deletions successful
 * @throws {Error} When any collection deletion fails
 */
async function deleteCollections(names: string[]): Promise<boolean> {
  try {
    console.log('🗑️ Deleting collections via API:', names);

    // Delete each collection via API
    const deletePromises = names.map(async (name) => {
      const response = await fetch(`${API_BASE}/api/deleteCollection}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
        },
        body: JSON.stringify({ collectionName: name }),
      });

      const result = (await response.json()) as { success: boolean; message: string; token?: string };
      console.log('✅ Document deleted:', result.message);
      // If a new token is returned, update it in localStorage
      if (result.token && typeof result.token === 'string' && result.token.length > 0) {
        localStorage.setItem('sessionToken', result.token);
        console.log('🔑 Session token refreshed and cached');
      }

      if (!response.ok) {
        const errorData = (await response.json()) as { message?: string };
        throw new Error(errorData.message || `Failed to delete collection ${name}: HTTP ${response.status}`);
      }

      return response.json() as Promise<{ success: boolean; message: string; token?: string }>;
    });

    await Promise.all(deletePromises);
    console.log('✅ All collections deleted successfully');

    // Clear selection and refresh the collections list
    selectedCollections.clear();
    await handleRefreshCollections();
    return true;
  } catch (error) {
    console.error('❌ Failed to delete collections:', error);
    throw error;
  }
}

// =========================
// Rendering & Selection
// =========================

/**
 * Renders the collections list in the UI with checkboxes and clickable names
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

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'collection-checkbox';
    checkbox.checked = selectedCollections.has(name);
    checkbox.addEventListener('change', () => {
      toggleCollectionSelection(name, checkbox, item);
    });

    const nameSpan = document.createElement('span');
    nameSpan.className = 'collection-name';
    nameSpan.textContent = name;
    nameSpan.addEventListener('click', () => {
      void selectCollection(name);
    });

    item.appendChild(checkbox);
    item.appendChild(nameSpan);
    collectionsList.appendChild(item);
  });
}

/**
 * Selects a collection and navigates to documents page after verifying existence
 * @param {string} name - Name of the collection to select
 * @return {void}
 * @throws {Error} Handled internally with fallback navigation to documents page
 */
async function selectCollection(name: string): Promise<void> {
  try {
    console.log('📂 Selecting collection via API:', name);

    // First, verify the collection exists and optionally get document count
    const response = await fetch(`${API_BASE}/api/getDocuments`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
      body: JSON.stringify({ collectionName: name }),
    });

    if (!response.ok) {
      if (response.status === 404) {
        showError(`Collection '${name}' not found`);
        return;
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const collectionInfo = (await response.json()) as {
      succes: boolean;
      message: string;
      documentNames: string[];
      token?: string;
    };

    // If a new token is returned, update it in localStorage
    if (collectionInfo.token && typeof collectionInfo.token === 'string' && collectionInfo.token.length > 0) {
      localStorage.setItem('sessionToken', collectionInfo.token);
      console.log('🔑 Session token refreshed and cached');
    }

    if (collectionInfo.succes) {
      console.log(collectionInfo.message);
      // Navigate to documents page with collection parameter
      window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
    } else {
      showError(`Collection '${name}' does not exist`);
    }
  } catch (error) {
    console.error('❌ Failed to select collection:', error);
    // Fallback - still navigate to documents page
    console.log('📄 Falling back to direct navigation');
    window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
  }
}

/**
 * Toggles selection state for a collection and updates UI accordingly
 * @param {string} name - Name of the collection to toggle
 * @param {HTMLInputElement} checkbox - The checkbox element that was clicked
 * @param {HTMLElement} item - The collection item container element
 * @return {void}
 */
function toggleCollectionSelection(name: string, checkbox: HTMLInputElement, item: HTMLElement): void {
  if (checkbox.checked) {
    selectedCollections.add(name);
    item.classList.add('selected');
  } else {
    selectedCollections.delete(name);
    item.classList.remove('selected');
  }
  updateSelectionUI();
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
 * Handles delete selected collections button click - deletes all selected collections via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleDeleteSelected(): Promise<void> {
  const selected = Array.from(selectedCollections);
  if (selected.length === 0) {
    showError('No collections selected');
    return;
  }

  try {
    const success = await deleteCollections(selected);
    if (success) {
      clearError();
    } else {
      showError('Failed to delete collections');
    }
  } catch (error) {
    showError('Error deleting collections: ' + getErrorMessage(error));
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
  selectedCount = document.getElementById('selectedCount') as HTMLSpanElement;
  deleteSelected = document.getElementById('deleteSelected') as HTMLButtonElement;
  confirmCreate = document.getElementById('confirmCreate') as HTMLButtonElement;
  confirmDelete = document.getElementById('confirmDelete') as HTMLButtonElement | null;
  collectionNameInput = document.getElementById('collectionNameInput') as HTMLInputElement;
  errorDiv = document.getElementById('error') as HTMLDivElement;

  if (!collectionsList || !refreshCollections || !errorDiv) {
    console.error('❌ Required DOM elements not found');
    return;
  }

  console.log('✅ DOM elements loaded');

  // Set up event listeners
  refreshCollections.addEventListener('click', () => {
    void handleRefreshCollections();
  });

  createCollectionButton?.addEventListener('click', () => {
    /* Modal will open automatically via HTML */
  });

  confirmCreate?.addEventListener('click', () => {
    void handleCreateCollection();
  });

  if (confirmDelete) {
    confirmDelete.addEventListener('click', () => {
      void handleDeleteSelected();
    });
  }

  deleteSelected?.addEventListener('click', () => {
    void handleDeleteSelected();
  });

  collectionSearch?.addEventListener('input', () => {
    handleSearchCollections();
  });

  // Initialize page
  void (async () => {
    try {
      console.log('🚀 Initializing webclient...');
      await handleRefreshCollections();
    } catch (error) {
      console.error('❌ Failed to initialize webclient:', error);
      showError('Failed to load collections. Using offline mode.');

      // Fallback to demo data
      allCollections = ['users', 'sessions', 'products', 'orders'];
      renderCollections(allCollections);
    }
  })();

  console.log('✅ WebClient ready');
}

// Wait for DOM to be ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeApp);
} else {
  initializeApp();
}
