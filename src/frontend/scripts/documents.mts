// @author Tijn Gommers
// @date 2025-19-11

import { API_BASE, authenticatedFetch, getErrorMessage } from './utils.mjs';

// =========================
// Constants & Initialization
// =========================

console.log('Documents loading...', 'API:', API_BASE);
declare const document: Document;

// =========================
// DOM Element Selectors
// =========================
function getEl<T extends HTMLElement>(id: string): T {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element with id="${id}"`);
  return el as T;
}

const collectionNameSpan = getEl<HTMLSpanElement>('collectionName');
const documentsView = getEl<HTMLDivElement>('documentsView');
const refreshDocuments = getEl<HTMLButtonElement>('refreshDocuments');
const insertDocument = getEl<HTMLButtonElement>('insertDocument');
const deleteDocumentBtn = getEl<HTMLButtonElement>('deleteDocument');
const confirmInsert = getEl<HTMLButtonElement>('confirmInsert');
const confirmDelete = getEl<HTMLButtonElement>('confirmDelete');
const insertIdInput = getEl<HTMLInputElement>('insertIdInput');
const insertJsonInput = getEl<HTMLTextAreaElement>('insertJsonInput');
const documentView = getEl<HTMLTextAreaElement>('documentView');
const errorDiv = getEl<HTMLDivElement>('error');

// =========================
// State Management
// =========================
const allDocuments: Array<Record<string, unknown>> = [];
let currentlyViewedDocument: string | null = null;
let originalDocumentName: string | null = null; // Track which document is being edited
const currentCollection = new URLSearchParams(window.location.search).get('collection') || 'unknown';
collectionNameSpan.textContent = currentCollection;

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
 * Clears the document view panel and resets the currently viewed document
 * @return {void}
 */
function clearDocumentView(): void {
  documentView.value = '';
  insertIdInput.value = '';
  insertJsonInput.value = '';
  currentlyViewedDocument = null;
  updateDeleteButtonVisibility();
  renderDocuments(allDocuments); // Re-render to remove viewing highlight
}

/**
 * Updates the delete button visibility based on document selection
 * @return {void}
 */
function updateDeleteButtonVisibility(): void {
  deleteDocumentBtn.style.display = currentlyViewedDocument ? 'block' : 'none';
  const editBtn = document.getElementById('editDocument');
  if (editBtn) editBtn.style.display = currentlyViewedDocument ? 'block' : 'none';
}

/**
 * Extracts document ID from document object with fallback strategies
 * @param {Record<string, unknown>} doc - Document object to extract ID from
 * @return {string} Document ID string, using 'id', 'name', '_id', or JSON slice as fallback
 */
function getDocumentId(doc: Record<string, unknown>): string {
  return (doc['id'] as string) || (doc['name'] as string) || (doc['_id'] as string) || JSON.stringify(doc).slice(0, 10);
}

// =========================
// API Functions
// =========================

/**
 * Fetches all documents from the current collection via API
 * @return {Promise<Array<Record<string, unknown>>>} Promise resolving to array of document objects
 * @throws {Error} When API request fails or returns non-ok response (except 404)
 */
async function fetchDocuments(): Promise<Array<Record<string, unknown>>> {
  try {
    console.log('Fetching documents from API for collection:', currentCollection);
    const response = await authenticatedFetch(
      `${API_BASE}/api/fetchDocuments?collectionName=${encodeURIComponent(currentCollection)}`,
      {
        method: 'GET',
      },
    );

    if (!response.ok) {
      if (response.status === 404) {
        console.log('Collection not found, returning empty array');
        return [];
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const documents = (await response.json()) as {
      success: boolean;
      message: string;
      documentNames: string[];
    };
    console.log(documents.message);

    // Map document names to objects to satisfy the return type expected by the UI
    return documents.documentNames.map((name) => ({ id: name, name: name }));
  } catch (error) {
    console.error('Failed to fetch documents:', error);
    throw error;
  }
}

/**
 * Fetches the full content of a single document by name
 * @param {string} documentName - Name/ID of the document to fetch
 * @return {Promise<Record<string, unknown>>} The document content object
 */
async function fetchDocumentContentByName(documentName: string): Promise<Record<string, unknown>> {
  const response = await authenticatedFetch(
    `${API_BASE}/api/fetchDocumentContent?collectionName=${encodeURIComponent(currentCollection)}&documentName=${encodeURIComponent(documentName)}`,
    {
      method: 'GET',
    },
  );

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const result = (await response.json()) as {
    success: boolean;
    message: string;
    documentContent: Record<string, unknown>;
  };

  if (!result.success) {
    throw new Error(result.message || 'Failed to fetch document content');
  }

  return result.documentContent || {};
}

/**
 * Creates a new document in the current collection via API
 * @param {string} id - Document ID to create
 * @param {Record<string, unknown>} content - Document content data
 * @return {Promise<boolean>} Promise resolving to true if creation successful
 * @throws {Error} When API request fails of document creation is rejected
 */
async function createDocument(id: string, content: Record<string, unknown>): Promise<boolean> {
  try {
    console.log('Creating document via API:', id, content);

    const response = await authenticatedFetch(`${API_BASE}/api/createDocument`, {
      method: 'POST',
      body: JSON.stringify({
        collectionName: currentCollection,
        documentName: id, // This will be used as the document identifier (stored in 'name' field)
        documentContent: content, // User content stored in nested 'content' field
      }),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message?: string };
      throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string };
    console.log('Document created:', result.message);

    return result.success;
  } catch (error) {
    console.error('Failed to create document:', error);
    throw error;
  }
}

/**
 * Updates an existing document in the current collection via API
 * @param {string} id - Document ID to update
 * @param {Record<string, unknown>} data - Updated document data object
 * @return {Promise<boolean>} Promise resolving to true if update successful
 * @throws {Error} When API request fails, document not found, of update is rejected
 */
async function updateDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  try {
    console.log('Updating document via API:', id, data);

    const response = await authenticatedFetch(`${API_BASE}/api/updateDocument`, {
      method: 'PUT',
      body: JSON.stringify({
        collectionName: currentCollection,
        documentName: id,
        newDocumentContent: data,
      }),
    });

    if (!response.ok) {
      if (response.status === 404) {
        throw new Error(`Document with ID '${id}' not found`);
      }
      const errorData = (await response.json()) as { error?: string };
      throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string };
    console.log('Document updated:', result);

    return true;
  } catch (error) {
    console.error('Failed to update document:', error);
    throw error;
  }
}

/**
 * Deletes a document from the current collection via API
 * @param {string} id - Document ID to delete
 * @return {Promise<boolean>} Promise resolving to true if deletion successful
 * @throws {Error} When document deletion fails or document not found
 */
async function deleteDocument(id: string): Promise<boolean> {
  try {
    console.log('Deleting document via API:', id);

    const response = await authenticatedFetch(`${API_BASE}/api/deleteDocument`, {
      method: 'DELETE',
      body: JSON.stringify({
        collectionName: currentCollection,
        documentName: id,
      }),
    });

    if (!response.ok) {
      if (response.status === 404) {
        console.warn(`Document ${id} not found (already deleted?)`);
        // Still refresh the list
        const documents = await fetchDocuments();
        allDocuments.splice(0, allDocuments.length, ...documents);
        renderDocuments(allDocuments);
        return true;
      }
      const errorData = (await response.json()) as { message?: string };
      throw new Error(`Failed to delete ${id}: ${errorData.message || response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string };
    console.log('Document deleted:', result.message);

    // Clear document view if the deleted document was being viewed
    if (currentlyViewedDocument === id) {
      clearDocumentView();
    }

    // Refresh the documents list
    const documents = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...documents);
    renderDocuments(allDocuments);
    updateDeleteButtonVisibility();
    return true;
  } catch (error) {
    console.error('Failed to delete document:', error);
    throw error;
  }
}

// =========================
// Rendering & Selection
// =========================

/**
 * Renders the documents list in the UI without checkboxes, just clickable content
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

    // Add 'viewing' class if this is the currently viewed document
    if (currentlyViewedDocument === id) {
      item.classList.add('viewing');
    }

    const content = document.createElement('div');
    content.className = 'document-content';
    const name = doc['name'] as string | undefined;
    const docId = doc['id'] as string | undefined;
    const displayName = name || docId || 'Unnamed';
    content.innerHTML = `<strong>${displayName}</strong>`;
    content.addEventListener('click', () => {
      void selectDocument(doc);
    });

    item.appendChild(content);
    documentsView.appendChild(item);
  });

  updateDeleteButtonVisibility();
}

/**
 * Selects a document and populates the edit form with its data
 * @param {Record<string, unknown>} doc - Document object to select
 * @return {void}
 */
async function selectDocument(doc: Record<string, unknown>): Promise<void> {
  try {
    const documentName = getDocumentId(doc);

    // If clicking the same document, unselect it
    if (currentlyViewedDocument === documentName) {
      console.log('Unselecting document:', documentName);
      clearDocumentView();
      return;
    }

    console.log('Fetching document content for:', documentName);

    const documentContent = await fetchDocumentContentByName(documentName);
    const docJson = JSON.stringify(documentContent, null, 2);
    documentView.value = docJson;
    insertIdInput.value = documentName;
    insertJsonInput.value = docJson;
    currentlyViewedDocument = documentName;
    renderDocuments(allDocuments); // Re-render to highlight viewed document
    updateDeleteButtonVisibility();
    console.log('Document content loaded:', documentContent);
  } catch (error) {
    console.error('Failed to fetch document content:', error);
    showError('Failed to load document content: ' + getErrorMessage(error));
  }
}

/**
 * Handles delete document button click - deletes the currently viewed document via API
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleDeleteDocument(): Promise<void> {
  try {
    clearError();

    if (!currentlyViewedDocument) {
      showError('No document selected');
      return;
    }

    const success = await deleteDocument(currentlyViewedDocument);

    if (success) {
      console.log(`Deleted document: ${currentlyViewedDocument}`);
    } else {
      showError('Failed to delete document');
    }
  } catch (e) {
    showError('Error deleting document: ' + getErrorMessage(e));
  }
}

/**
 * Handles edit document button click - opens modal with document data for editing
 * @return {void}
 * @throws {Error} Handled internally and displayed to user via showError
 */
async function handleEditDocument(): Promise<void> {
  try {
    clearError();

    if (!currentlyViewedDocument) {
      showError('No document selected to edit');
      return;
    }

    // Store original document name
    originalDocumentName = currentlyViewedDocument;

    // Update modal UI for edit mode
    const modalTitle = document.querySelector('#insertModalOverlay .modal-title') as HTMLElement;
    if (modalTitle) modalTitle.textContent = 'Edit Document';
    if (confirmInsert) confirmInsert.textContent = 'Edit';

    // Fetch the document content
    const content = await fetchDocumentContentByName(currentlyViewedDocument);

    // Prefill the modal with document data (name is read-only during edit)
    insertIdInput.value = currentlyViewedDocument;
    insertIdInput.disabled = true; // Disable name editing
    insertJsonInput.value = JSON.stringify(content, null, 2);

    // Open the modal
    const insertModal = document.getElementById('insertModalOverlay');
    insertModal?.classList.add('show');
    insertJsonInput.focus();

    console.log('Edit modal opened for document:', currentlyViewedDocument);
  } catch (e) {
    showError('Failed to load document for editing: ' + getErrorMessage(e));
  }
}

// =========================
// Event Handlers
// =========================

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

    // Check if we're in edit mode (originalDocumentName is set)
    const isEditMode = originalDocumentName !== null;
    const success =
      isEditMode && originalDocumentName
        ? await updateDocument(originalDocumentName, data)
        : await createDocument(id, data);

    if (success) {
      insertIdInput.value = '';
      insertIdInput.disabled = false; // Re-enable name input
      insertJsonInput.value = '';
      documentView.value = '';
      currentlyViewedDocument = null;
      originalDocumentName = null; // Reset edit state
      updateDeleteButtonVisibility();
      console.log(`Document ${isEditMode ? 'updated' : 'created'} successfully`);

      // Close the modal
      const insertModal = document.getElementById('insertModalOverlay');
      insertModal?.classList.remove('show');

      // Refresh the complete document list from backend
      await handleRefreshDocuments();
    } else {
      showError(`Failed to ${isEditMode ? 'update' : 'create'} document`);
    }
  } catch (e) {
    showError('Invalid JSON or error: ' + getErrorMessage(e));
  }
}

// =========================
// Event Listeners & Init
// =========================

refreshDocuments.addEventListener('click', () => {
  void handleRefreshDocuments();
});

insertDocument.addEventListener('click', () => {
  /* Modal will open automatically via HTML */
});

confirmInsert.addEventListener('click', () => {
  void handleInsertDocument();
});

// Allow Enter key to submit document insert
insertIdInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault();
    confirmInsert.click();
  }
});

confirmDelete.addEventListener('click', () => {
  void handleDeleteDocument();
  // Close the delete modal after confirmation
  const deleteModal = document.getElementById('deleteModalOverlay');
  deleteModal?.classList.remove('show');
});

// =========================
// Aggregate Functionality
// =========================

const runAggregateBtn = getEl<HTMLButtonElement>('runAggregate');
const groupByField = getEl<HTMLInputElement>('groupByField');
const operationField = getEl<HTMLInputElement>('operationField');
const operationType = getEl<HTMLSelectElement>('operationType');
const aggregateResults = getEl<HTMLDivElement>('aggregateResults');
const operationFieldContainer = getEl<HTMLDivElement>('operationFieldContainer');

// Toggle field visibility based on operation type
operationType.addEventListener('change', () => {
  if (operationType.value === 'count') {
    operationFieldContainer.style.display = 'none';
    operationField.value = ''; // Clear the field when hidden
  } else {
    operationFieldContainer.style.display = 'block';
  }
});

/**
 * Runs aggregate analysis on the collection
 * @return {void}
 */
async function handleRunAggregate(): Promise<void> {
  try {
    clearError();

    const groupBy = groupByField.value.trim();

    const opType = operationType.value;
    const fieldName = operationField.value.trim();

    if (opType !== 'count' && !fieldName) {
      showError('Please specify a field to analyze');
      return;
    }

    aggregateResults.textContent = 'Running analysis...';

    // Ensure we have the latest document list
    const docs = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...docs);

    // Fetch full content for each document so we can aggregate client-side
    const detailedDocs = await Promise.all(
      docs.map(async (doc) => {
        const name = getDocumentId(doc);
        try {
          const content = await fetchDocumentContentByName(name);
          return { name, content };
        } catch (error) {
          console.warn(`Skipping document ${name} due to fetch error:`, error);
          return { name, content: {} as Record<string, unknown> };
        }
      }),
    );

    if (detailedDocs.length === 0) {
      aggregateResults.textContent = 'No documents found to analyze.';
      return;
    }

    const toNumber = (value: unknown): number | null => {
      if (typeof value === 'number') return Number.isFinite(value) ? value : null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    type GroupState = {
      groupValue: unknown;
      count: number;
      sum: number;
      min: number | null;
      max: number | null;
      numericCount: number;
    };

    const groups = new Map<string, GroupState>();

    detailedDocs.forEach(({ content }) => {
      const record = content;
      const groupValue = groupBy ? record[groupBy] : 'all';
      const groupKey = groupBy
        ? groupValue === undefined || groupValue === null
          ? 'null'
          : JSON.stringify(groupValue)
        : 'all';

      const state: GroupState = groups.get(groupKey) || {
        groupValue: groupBy ? (groupValue ?? null) : 'all',
        count: 0,
        sum: 0,
        min: null,
        max: null,
        numericCount: 0,
      };

      state.count += 1;

      if (opType !== 'count') {
        const numeric = toNumber(record[fieldName]);
        if (numeric !== null) {
          state.sum += numeric;
          state.min = state.min === null ? numeric : Math.min(state.min, numeric);
          state.max = state.max === null ? numeric : Math.max(state.max, numeric);
          state.numericCount += 1;
        }
      }

      groups.set(groupKey, state);
    });

    const results = Array.from(groups.values()).map((state) => {
      const base: Record<string, unknown> = {
        group: state.groupValue,
        count: state.count,
      };

      if (opType === 'sum') {
        base[`sum_${fieldName}`] = state.sum;
      } else if (opType === 'avg') {
        base[`avg_${fieldName}`] = state.numericCount > 0 ? state.sum / state.numericCount : null;
      } else if (opType === 'min') {
        base[`min_${fieldName}`] = state.numericCount > 0 ? state.min : null;
      } else if (opType === 'max') {
        base[`max_${fieldName}`] = state.numericCount > 0 ? state.max : null;
      }

      return base;
    });

    aggregateResults.textContent = JSON.stringify(results, null, 2);
    console.log('Aggregate results (client-side):', results);
  } catch (e) {
    showError('Aggregate error: ' + getErrorMessage(e));
    aggregateResults.textContent = 'Error running analysis. See error message above.';
  }
}

runAggregateBtn.addEventListener('click', () => {
  void handleRunAggregate();
});

// =========================
// Modal Functionality
// =========================

/**
 * Initializes modal event listeners for delete and insert modals
 * @return {void}
 */
function initializeModals(): void {
  const deleteBtn = document.getElementById('deleteDocument');
  const deleteModal = document.getElementById('deleteModalOverlay');
  const cancelDeleteBtn = document.getElementById('cancelDelete');

  const insertBtn = document.getElementById('insertDocument');
  const insertModal = document.getElementById('insertModalOverlay');
  const cancelInsertBtn = document.getElementById('cancelInsert');
  const confirmInsertBtn = document.getElementById('confirmInsert');
  const nameInput = document.getElementById('insertIdInput') as HTMLInputElement;
  const jsonInput = document.getElementById('insertJsonInput') as HTMLTextAreaElement;

  // Delete modal
  deleteBtn?.addEventListener('click', () => {
    deleteModal?.classList.add('show');
  });

  cancelDeleteBtn?.addEventListener('click', () => {
    deleteModal?.classList.remove('show');
  });

  // Insert modal
  insertBtn?.addEventListener('click', () => {
    originalDocumentName = null; // Reset edit mode
    insertIdInput.disabled = false; // Enable name input for new document
    const modalTitle = document.querySelector('#insertModalOverlay .modal-title') as HTMLElement;
    if (modalTitle) modalTitle.textContent = 'Insert New Document';
    if (confirmInsertBtn) confirmInsertBtn.textContent = 'Insert';
    if (nameInput) nameInput.value = '';
    if (jsonInput) jsonInput.value = '';
    insertModal?.classList.add('show');
    nameInput?.focus();
  });

  cancelInsertBtn?.addEventListener('click', () => {
    originalDocumentName = null;
    insertIdInput.disabled = false;
    insertModal?.classList.remove('show');
    if (nameInput) nameInput.value = '';
    if (jsonInput) jsonInput.value = '';
  });

  // Close insert modal when confirm button is clicked
  confirmInsertBtn?.addEventListener('click', () => {
    originalDocumentName = null;
    insertIdInput.disabled = false;
    insertModal?.classList.remove('show');
    if (nameInput) nameInput.value = '';
    if (jsonInput) jsonInput.value = '';
  });

  // Edit button
  const editBtn = document.getElementById('editDocument');
  editBtn?.addEventListener('click', () => {
    void handleEditDocument();
  });

  // Close modals when clicking overlay
  deleteModal?.addEventListener('click', (e) => {
    if (e.target === deleteModal) {
      deleteModal.classList.remove('show');
    }
  });

  insertModal?.addEventListener('click', (e) => {
    if (e.target === insertModal) {
      originalDocumentName = null;
      insertIdInput.disabled = false;
      insertModal.classList.remove('show');
      if (nameInput) nameInput.value = '';
      if (jsonInput) jsonInput.value = '';
    }
  });

  // Close modals on Escape key or Enter key for delete modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (deleteModal?.classList.contains('show')) {
        deleteModal.classList.remove('show');
      }
      if (insertModal?.classList.contains('show')) {
        originalDocumentName = null;
        insertIdInput.disabled = false;
        insertModal.classList.remove('show');
        if (nameInput) nameInput.value = '';
        if (jsonInput) jsonInput.value = '';
      }
    }
    // Enter key confirms delete when delete modal is open
    if (e.key === 'Enter' && deleteModal?.classList.contains('show')) {
      e.preventDefault();
      document.getElementById('confirmDelete')?.click();
    }
  });
}

// Initialize modals when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeModals);
} else {
  initializeModals();
}

void (async () => {
  try {
    console.log('Initializing documents page for collection:', currentCollection);
    const docs = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...docs);
    renderDocuments(docs);
    console.log('Documents page initialized successfully');
  } catch (error) {
    console.error('Failed to initialize documents page:', error);
    showError('Failed to load documents. Check if the collection exists and the server is running.');
    documentsView.innerHTML = '<div class="no-results">Unable to load documents. Please try refreshing.</div>';
  }
})();

console.log('Documents script loaded');
