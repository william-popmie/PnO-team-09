// @author Tijn Gommers
// @date 2025-19-11

/// <reference lib="dom" />

const API_BASE = 'http://localhost:3000';

function getEl<T extends HTMLElement>(id: string): T {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element with id="${id}"`);
  return el as T;
}

// DOM elements
const collectionSearch = getEl<HTMLInputElement>('collectionSearch');
const collectionsList = getEl<HTMLDivElement>('collectionsList');
const refreshCollections = getEl<HTMLButtonElement>('refreshCollections');
const createCollectionButton = getEl<HTMLButtonElement>('createCollection');
const selectedCount = getEl<HTMLSpanElement>('selectedCount');
const deleteSelected = getEl<HTMLButtonElement>('deleteSelected');
const confirmCreate = getEl<HTMLButtonElement>('confirmCreate');
const confirmDelete = getEl<HTMLButtonElement>('confirmDelete');
const collectionNameInput = getEl<HTMLInputElement>('collectionNameInput');
const errorDiv = getEl<HTMLDivElement>('error');

// State management -- moet nog let worden maar pas na de implementatie van de functies
const selectedCollections = new Set<string>();
const allCollections: string[] = [];

// Utility functions
function showError(msg: string) {
  errorDiv.textContent = msg;
  setTimeout(() => {
    errorDiv.textContent = '';
  }, 5000);
}

function clearError() {
  errorDiv.textContent = '';
}

function getErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function updateSelectionUI() {
  // TODO: Update UI based on selected collections count
  // Show/hide selectedCount span and deleteSelected button
  // Update selectedCount text with number of selected collections
}

function getCollectionName(name: string): string {
  // TODO: Process collection name if needed (validation, formatting)
  // For now just return the name as-is
  return name;
}

// API functions
async function fetchCollections(): Promise<string[]> {
  // TODO: Fetch collections from API endpoint
  // GET ${API_BASE}/collections
  // Handle errors and return empty array on failure
  return Promise.resolve([]);
}

async function createCollection(name: string): Promise<boolean> {
  // TODO: Create new collection via API
  // POST ${API_BASE}/collections
  // Return true on success, false on failure
  return Promise.resolve(false);
}

async function deleteCollections(names: string[]): Promise<boolean> {
  // TODO: Delete multiple collections via API
  // DELETE ${API_BASE}/collections/${name} for each name
  // Return true if all deletions successful, false otherwise
  return Promise.resolve(false);
}

// Rendering functions
function renderCollections(collections: string[]) {
  // TODO: Render collections list with checkboxes (Gmail-style)
  // Clear collectionsList, create list items with checkboxes and names
  // Add click handlers for checkbox selection and navigation to documents
  // Update allCollections state
}

function selectCollection(name: string) {
  // TODO: Navigate to documents page with collection parameter
  // window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
}

function toggleCollectionSelection(name: string, checkbox: HTMLElement, item: HTMLElement) {
  // TODO: Toggle collection selection state
  // Update selectedCollections Set, checkbox visual state, item highlight
  // Call updateSelectionUI()
}

// Event handlers
async function handleRefreshCollections() {
  // TODO: Refresh collections list
  // Call fetchCollections() and renderCollections()
}

async function handleCreateCollection() {
  // TODO: Handle collection creation
  // Get value from collectionNameInput, call createCollection()
  // Refresh collections list on success, clear input
}

async function handleDeleteSelected() {
  // TODO: Handle batch collection deletion
  // Get selected collection names, call deleteCollections()
  // Clear selection, refresh collections list on success
}

async function handleSearchCollections() {
  // TODO: Handle collection search/filtering
  // Filter allCollections based on search input, call renderCollections()
}

// Event listeners
refreshCollections.addEventListener('click', () => {
  void handleRefreshCollections();
});
createCollectionButton.addEventListener('click', () => {
  /* Modal will open automatically via HTML */
});
confirmCreate.addEventListener('click', () => {
  void handleCreateCollection();
});
confirmDelete.addEventListener('click', () => {
  void handleDeleteSelected();
});
collectionSearch.addEventListener('input', () => {
  void handleSearchCollections();
});

// Initialize page
void (async () => {
  const collections = await fetchCollections();
  renderCollections(collections);
})();
