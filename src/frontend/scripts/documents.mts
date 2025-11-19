// @author Tijn Gommers
// @date 2025-19-11

const API_BASE = 'http://localhost:3000';

declare const document: Document;

function getEl<T extends HTMLElement>(id: string): T {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element with id="${id}"`);
  return el as T;
}

// DOM elements
const collectionNameSpan = getEl<HTMLSpanElement>('collectionName');
const documentsView = getEl<HTMLDivElement>('documentsView');
const refreshDocuments = getEl<HTMLButtonElement>('refreshDocuments');
const insertDocument = getEl<HTMLButtonElement>('insertDocument');
const selectedCount = getEl<HTMLSpanElement>('selectedCount');
const deleteSelected = getEl<HTMLButtonElement>('deleteSelected');
const confirmInsert = getEl<HTMLButtonElement>('confirmInsert');
const confirmDelete = getEl<HTMLButtonElement>('confirmDelete');
const insertIdInput = getEl<HTMLInputElement>('insertIdInput');
const insertJsonInput = getEl<HTMLTextAreaElement>('insertJsonInput');
const documentView = getEl<HTMLTextAreaElement>('documentView');
const errorDiv = getEl<HTMLDivElement>('error');

// State management -- moet nog let worden maar pas na de implementatie van de functies
const selectedDocuments = new Set<string>();
const allDocuments: Array<Record<string, unknown>> = [];
const currentCollection = new URLSearchParams(window.location.search).get('collection') || 'unknown';

// Initialize collection name
collectionNameSpan.textContent = currentCollection;

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
  // TODO: Update UI based on selected documents count
  // Show/hide selectedCount span and deleteSelected button
  // Update selectedCount text with number of selected documents
}

function getDocumentId(doc: Record<string, unknown>): string {
  // TODO: Extract document ID from document object
  // Handle both 'id' and '_id' fields, fallback to JSON string slice
  return '';
}

// API functions
async function fetchDocuments(): Promise<Array<Record<string, unknown>>> {
  // TODO: Fetch documents from API endpoint
  // GET ${API_BASE}/collections/${currentCollection}/documents
  // Handle errors and return empty array on failure
  return Promise.resolve([]);
}

async function createDocument(name: string, data: Record<string, unknown>): Promise<boolean> {
  // TODO: Create new document via API
  // POST ${API_BASE}/collections/${currentCollection}/documents
  // Return true on success, false on failure
  return Promise.resolve(false);
}

async function updateDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  // TODO: Update existing document via API
  // PUT ${API_BASE}/collections/${currentCollection}/documents/${id}
  // Return true on success, false on failure
  return Promise.resolve(false);
}

async function deleteDocuments(ids: string[]): Promise<boolean> {
  // TODO: Delete multiple documents via API
  // DELETE ${API_BASE}/collections/${currentCollection}/documents/${id} for each id
  // Return true if all deletions successful, false otherwise
  return Promise.resolve(false);
}

// Rendering functions
function renderDocuments(docs: Array<Record<string, unknown>>) {
  // TODO: Render documents list with checkboxes (Gmail-style)
  // Clear documentsView, create list items with checkboxes and names
  // Add click handlers for checkbox selection and document viewing
  // Update allDocuments state
}

function selectDocument(doc: Record<string, unknown>) {
  // TODO: Show document details in documentView textarea
  // Populate insertIdInput and insertJsonInput for editing
}

function toggleDocumentSelection(id: string, checkbox: HTMLElement, item: HTMLElement) {
  // TODO: Toggle document selection state
  // Update selectedDocuments Set, checkbox visual state, item highlight
  // Call updateSelectionUI()
}

// Event handlers
async function handleRefreshDocuments() {
  // TODO: Refresh documents list
  // Call fetchDocuments() and renderDocuments()
}

async function handleInsertDocument() {
  // TODO: Handle document creation/update
  // Get values from insertIdInput and insertJsonInput
  // Call createDocument() or updateDocument() based on whether ID exists
  // Refresh documents list on success, clear inputs
}

async function handleDeleteSelected() {
  // TODO: Handle batch document deletion
  // Get selected document IDs, call deleteDocuments()
  // Clear selection, refresh documents list on success
}

// Event listeners
refreshDocuments.addEventListener('click', () => {
  void handleRefreshDocuments();
});
insertDocument.addEventListener('click', () => {
  /* Modal will open automatically via HTML */
});
confirmInsert.addEventListener('click', () => {
  void handleInsertDocument();
});
confirmDelete.addEventListener('click', () => {
  void handleDeleteSelected();
});

// Initialize page
void (async () => {
  const docs = await fetchDocuments();
  renderDocuments(docs);
})();
