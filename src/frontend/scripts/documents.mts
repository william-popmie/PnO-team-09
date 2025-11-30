// @author Tijn Gommers
// @date 2025-19-11

const API_BASE = 'http://localhost:3000';

console.log('📝 Documents loading...', 'API:', API_BASE);

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
 * Updates the UI to reflect current document selection state
 * Shows/hides selection count and delete button based on selected items
 * @return {void}
 */
function updateSelectionUI(): void {
  const count = selectedDocuments.size;
  selectedCount.textContent = count > 0 ? `${count} selected` : '';
  deleteSelected.style.display = count > 0 ? 'block' : 'none';
}

/**
 * Extracts document ID from document object with fallback strategies
 * @param {Record<string, unknown>} doc - Document object to extract ID from
 * @return {string} Document ID string, using 'id', '_id', or JSON slice as fallback
 */
function getDocumentId(doc: Record<string, unknown>): string {
  // TODO: Extract document ID from document object
  // Handle both 'id' and '_id' fields, fallback to JSON string slice
  return (doc['id'] as string) || (doc['_id'] as string) || JSON.stringify(doc).slice(0, 10);
}

// API functions
/**
 * Fetches all documents from the current collection via API
 * @return {Promise<Array<Record<string, unknown>>>} Promise resolving to array of document objects
 * @throws {Error} When API request fails or returns non-ok response (except 404)
 */
async function fetchDocuments(): Promise<Array<Record<string, unknown>>> {
  try {
    console.log('🔄 Fetching documents from API for collection:', currentCollection);
    const response = await fetch(`${API_BASE}/db/${encodeURIComponent(currentCollection)}`);

    if (!response.ok) {
      if (response.status === 404) {
        console.log('📭 Collection not found, returning empty array');
        return [];
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const documents = (await response.json()) as Array<Record<string, unknown>>;
    console.log('✅ Documents received:', documents.length, 'documents');
    return documents;
  } catch (error) {
    console.error('❌ Failed to fetch documents:', error);
    throw error;
  }
}

/**
 * Creates a new document in the current collection via API
 * @param {string} id - Document ID to create
 * @param {Record<string, unknown>} data - Document data object
 * @return {Promise<boolean>} Promise resolving to true if creation successful
 * @throws {Error} When API request fails or document creation is rejected
 */
async function createDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  try {
    console.log('🔧 Creating document via API:', id, data);
    const documentData = { id, ...data };

    const response = await fetch(`${API_BASE}/db/${encodeURIComponent(currentCollection)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(documentData),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { error?: string };
      throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as Record<string, unknown>;
    console.log('✅ Document created:', result);

    // Refresh documents list after creation
    const documents = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...documents);
    renderDocuments(allDocuments);
    return true;
  } catch (error) {
    console.error('❌ Failed to create document:', error);
    throw error;
  }
}

/**
 * Updates an existing document in the current collection via API
 * @param {string} id - Document ID to update
 * @param {Record<string, unknown>} data - Updated document data object
 * @return {Promise<boolean>} Promise resolving to true if update successful
 * @throws {Error} When API request fails, document not found, or update is rejected
 */
async function updateDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  try {
    console.log('🔧 Updating document via API:', id, data);

    const response = await fetch(`${API_BASE}/db/${encodeURIComponent(currentCollection)}/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });

    if (!response.ok) {
      if (response.status === 404) {
        throw new Error(`Document with ID '${id}' not found`);
      }
      const errorData = (await response.json()) as { error?: string };
      throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as Record<string, unknown>;
    console.log('✅ Document updated:', result);

    // Refresh documents list after update
    const documents = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...documents);
    renderDocuments(allDocuments);
    return true;
  } catch (error) {
    console.error('❌ Failed to update document:', error);
    throw error;
  }
}

/**
 * Deletes multiple documents from the current collection via API using parallel requests
 * @param {string[]} ids - Array of document IDs to delete
 * @return {Promise<boolean>} Promise resolving to true if all deletions successful
 * @throws {Error} When any document deletion fails or document not found
 */
async function deleteDocuments(ids: string[]): Promise<boolean> {
  try {
    console.log('🗑️ Deleting documents via API:', ids);

    // Delete each document via API
    const deletePromises = ids.map(async (id) => {
      const response = await fetch(
        `${API_BASE}/db/${encodeURIComponent(currentCollection)}/${encodeURIComponent(id)}`,
        {
          method: 'DELETE',
        },
      );

      if (!response.ok) {
        if (response.status === 404) {
          console.warn(`Document ${id} not found (already deleted?)`);
          return { success: true, id };
        }
        const errorData = (await response.json()) as { error?: string };
        throw new Error(`Failed to delete ${id}: ${errorData.error || response.statusText}`);
      }

      return response.json() as Promise<{ success: boolean }>;
    });

    await Promise.all(deletePromises);
    console.log('✅ All documents deleted successfully');

    // Clear selection and refresh the documents list
    selectedDocuments.clear();
    const documents = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...documents);
    renderDocuments(allDocuments);
    updateSelectionUI();
    return true;
  } catch (error) {
    console.error('❌ Failed to delete documents:', error);
    throw error;
  }
}

// Rendering functions
/**
 * Renders the documents list in the UI with checkboxes and clickable content
 * @param {Array<Record<string, unknown>>} docs - Array of document objects to render
 * @return {void}
 */
function renderDocuments(docs: Array<Record<string, unknown>>): void {
  documentsView.innerHTML = '';
  allDocuments.splice(0, allDocuments.length, ...docs);

  if (docs.length === 0) {
    documentsView.innerHTML = '<p>No documents found</p>';
    return;
  }

  docs.forEach((doc) => {
    const id = getDocumentId(doc);
    const item = document.createElement('div');
    item.className = 'document-item';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.checked = selectedDocuments.has(id);
    checkbox.addEventListener('change', () => {
      toggleDocumentSelection(id, checkbox, item);
    });

    const content = document.createElement('div');
    content.className = 'document-content';
    const name = doc['name'] as string | undefined;
    const docId = doc['id'] as string | undefined;
    const displayName = name || docId || 'Unnamed';
    content.innerHTML = `<strong>${displayName}</strong><br>${JSON.stringify(doc, null, 2).slice(0, 100)}...`;
    content.addEventListener('click', () => {
      selectDocument(doc);
    });

    item.appendChild(checkbox);
    item.appendChild(content);
    documentsView.appendChild(item);
  });
}

/**
 * Selects a document and populates the edit form with its data
 * @param {Record<string, unknown>} doc - Document object to select
 * @return {void}
 */
function selectDocument(doc: Record<string, unknown>): void {
  const docJson = JSON.stringify(doc, null, 2);
  documentView.value = docJson;
  insertIdInput.value = getDocumentId(doc);
  insertJsonInput.value = docJson;
  console.log('Selected document:', doc);
}

/**
 * Toggles selection state for a document and updates UI accordingly
 * @param {string} id - Document ID to toggle
 * @param {HTMLElement} checkbox - The checkbox element that was clicked
 * @param {HTMLElement} item - The document item container element
 * @return {void}
 */
function toggleDocumentSelection(id: string, checkbox: HTMLElement, item: HTMLElement): void {
  if (selectedDocuments.has(id)) {
    selectedDocuments.delete(id);
    item.classList.remove('selected');
    (checkbox as HTMLInputElement).checked = false;
  } else {
    selectedDocuments.add(id);
    item.classList.add('selected');
    (checkbox as HTMLInputElement).checked = true;
  }
  updateSelectionUI();
  console.log('Selection updated:', Array.from(selectedDocuments));
}

// Event handlers
/**
 * Handles refresh documents button click - loads documents from API and renders them
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleRefreshDocuments(): Promise<void> {
  try {
    clearError();
    const docs = await fetchDocuments();
    renderDocuments(docs);
    console.log('Documents refreshed');
  } catch (e) {
    showError('Failed to refresh documents: ' + getErrorMessage(e));
  }
}

/**
 * Handles insert/update document form submission - validates input and creates or updates document via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleInsertDocument(): Promise<void> {
  try {
    clearError();
    const id = insertIdInput.value.trim();
    const jsonText = insertJsonInput.value.trim();

    if (!id || !jsonText) {
      showError('Please provide both ID and JSON data');
      return;
    }

    const data = JSON.parse(jsonText) as Record<string, unknown>;
    const exists = allDocuments.some((doc) => getDocumentId(doc) === id);

    const success = exists ? await updateDocument(id, data) : await createDocument(id, data);

    if (success) {
      insertIdInput.value = '';
      insertJsonInput.value = '';
      documentView.value = '';
      console.log(`Document ${exists ? 'updated' : 'created'} successfully`);
    } else {
      showError(`Failed to ${exists ? 'update' : 'create'} document`);
    }
  } catch (e) {
    showError('Invalid JSON or error: ' + getErrorMessage(e));
  }
}

/**
 * Handles delete selected documents button click - deletes all selected documents via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleDeleteSelected() {
  try {
    clearError();
    const selectedIds = Array.from(selectedDocuments);

    if (selectedIds.length === 0) {
      showError('No documents selected');
      return;
    }

    const success = await deleteDocuments(selectedIds);

    if (success) {
      console.log(`Deleted ${selectedIds.length} documents`);
    } else {
      showError('Failed to delete documents');
    }
  } catch (e) {
    showError('Error deleting documents: ' + getErrorMessage(e));
  }
}

// Event listeners
// Refresh documents list when refresh button is clicked
refreshDocuments.addEventListener('click', () => {
  void handleRefreshDocuments();
});
// Open insert/update document modal when insert button is clicked
insertDocument.addEventListener('click', () => {
  /* Modal will open automatically via HTML */
});
// Create or update document when modal confirm button is clicked
confirmInsert.addEventListener('click', () => {
  void handleInsertDocument();
});
// Delete selected documents when modal confirm button is clicked
confirmDelete.addEventListener('click', () => {
  void handleDeleteSelected();
});

// Initialize page
void (async () => {
  try {
    console.log('🚀 Initializing documents page for collection:', currentCollection);
    const docs = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...docs);
    renderDocuments(docs);
    console.log('✅ Documents page initialized successfully');
  } catch (error) {
    console.error('❌ Failed to initialize documents page:', error);
    showError('Failed to load documents. Check if the collection exists and the server is running.');

    // Show empty state
    documentsView.innerHTML = '<div class="no-results">Unable to load documents. Please try refreshing.</div>';
  }
})();

console.log('✅ Documents script loaded');
