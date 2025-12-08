// @author Tijn Gommers
// @date 2025-19-11

// =========================
// Constants & Initialization
// =========================

const API_BASE = 'http://localhost:3000';
console.log('📝 Documents loading...', 'API:', API_BASE);
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
 * Handles token expiration by clearing session and redirecting to login
 * @return {void}
 */
function handleTokenExpiration(): void {
  console.warn('⚠️ Token expired or invalid, redirecting to login');
  localStorage.removeItem('sessionToken');
  localStorage.removeItem('username');
  window.location.href = 'login.html';
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
 * Extracts error message from unknown error type
 * @param {unknown} e - Error object or value of unknown type
 * @return {string} Error message string or stringified value
 */
function getErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * Updates the delete button visibility based on document selection
 * @return {void}
 */
function updateDeleteButtonVisibility(): void {
  deleteDocumentBtn.style.display = currentlyViewedDocument ? 'block' : 'none';
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
    console.log('🔄 Fetching documents from API for collection:', currentCollection);
    const response = await fetch(
      `${API_BASE}/api/fetchDocuments?collectionName=${encodeURIComponent(currentCollection)}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
        },
      },
    );

    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        handleTokenExpiration();
        return [];
      }
      if (response.status === 404) {
        console.log('📭 Collection not found, returning empty array');
        return [];
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const documents = (await response.json()) as {
      success: boolean;
      message: string;
      documentNames: string[];
      token?: string;
    };
    console.log(documents.message);

    // If a new token is returned, update it in localStorage
    if (documents.token && typeof documents.token === 'string' && documents.token.length > 0) {
      localStorage.setItem('sessionToken', documents.token);
      console.log('🔑 Session token refreshed and cached');
    }

    // Map document names to objects to satisfy the return type expected by the UI
    return documents.documentNames.map((name) => ({ id: name, name: name }));
  } catch (error) {
    console.error('❌ Failed to fetch documents:', error);
    throw error;
  }
}

/**
 * Fetches the full content of a single document by name
 * @param {string} documentName - Name/ID of the document to fetch
 * @return {Promise<Record<string, unknown>>} The document content object
 */
async function fetchDocumentContentByName(documentName: string): Promise<Record<string, unknown>> {
  const response = await fetch(
    `${API_BASE}/api/fetchDocumentContent?collectionName=${encodeURIComponent(currentCollection)}&documentName=${encodeURIComponent(documentName)}`,
    {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
    },
  );

  if (response.status === 401 || response.status === 403) {
    handleTokenExpiration();
    return {};
  }

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const result = (await response.json()) as {
    success: boolean;
    message: string;
    documentContent: Record<string, unknown>;
    token?: string;
  };

  if (result.token && typeof result.token === 'string' && result.token.length > 0) {
    localStorage.setItem('sessionToken', result.token);
    console.log('🔑 Session token refreshed and cached');
  }

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
    console.log('🔧 Creating document via API:', id, content);

    const response = await fetch(`${API_BASE}/api/createDocument`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
      body: JSON.stringify({
        collectionName: currentCollection,
        documentName: id, // This will be used as the document identifier (stored in 'name' field)
        documentContent: content, // User content stored in nested 'content' field
      }),
    });

    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        handleTokenExpiration();
        return false;
      }
      const errorData = (await response.json()) as { message?: string };
      throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string; token?: string };
    console.log('✅ Document created:', result.message);

    // If a new token is returned, update it in localStorage
    if (result.token && typeof result.token === 'string' && result.token.length > 0) {
      localStorage.setItem('sessionToken', result.token);
      console.log('🔑 Session token refreshed and cached');
    }

    // Refresh documents list after creation
    const documents = await fetchDocuments();
    allDocuments.splice(0, allDocuments.length, ...documents);
    renderDocuments(allDocuments);
    return result.success;
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
 * @throws {Error} When API request fails, document not found, of update is rejected
 */
async function updateDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  try {
    console.log('🔧 Updating document via API:', id, data);

    const response = await fetch(`${API_BASE}/api/updateDocument`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
      body: JSON.stringify({
        collectionName: currentCollection,
        documentData: data,
        id: id,
      }),
    });

    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        handleTokenExpiration();
        return false;
      }
      if (response.status === 404) {
        throw new Error(`Document with ID '${id}' not found`);
      }
      const errorData = (await response.json()) as { error?: string };
      throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
    }

    const result = (await response.json()) as { success: boolean; message: string; token?: string };
    console.log('✅ Document updated:', result);

    // If a new token is returned, update it in localStorage
    if (result.token && typeof result.token === 'string' && result.token.length > 0) {
      localStorage.setItem('sessionToken', result.token);
      console.log('🔑 Session token refreshed and cached');
    }

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
 * Deletes a document from the current collection via API
 * @param {string} id - Document ID to delete
 * @return {Promise<boolean>} Promise resolving to true if deletion successful
 * @throws {Error} When document deletion fails or document not found
 */
async function deleteDocument(id: string): Promise<boolean> {
  try {
    console.log('🗑️ Deleting document via API:', id);

    const response = await fetch(`${API_BASE}/api/deleteDocument`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${localStorage.getItem('sessionToken') || ''}`,
      },
      body: JSON.stringify({
        collectionName: currentCollection,
        documentName: id,
      }),
    });

    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        handleTokenExpiration();
        return false;
      }
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

    const result = (await response.json()) as { success: boolean; message: string; token?: string };
    console.log('✅ Document deleted:', result.message);

    // If a new token is returned, update it in localStorage
    if (result.token && typeof result.token === 'string' && result.token.length > 0) {
      localStorage.setItem('sessionToken', result.token);
      console.log('🔑 Session token refreshed and cached');
    }

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
    console.error('❌ Failed to delete document:', error);
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
      console.log('📄 Unselecting document:', documentName);
      clearDocumentView();
      return;
    }

    console.log('📄 Fetching document content for:', documentName);

    const documentContent = await fetchDocumentContentByName(documentName);
    const docJson = JSON.stringify(documentContent, null, 2);
    documentView.value = docJson;
    insertIdInput.value = documentName;
    insertJsonInput.value = docJson;
    currentlyViewedDocument = documentName;
    renderDocuments(allDocuments); // Re-render to highlight viewed document
    updateDeleteButtonVisibility();
    console.log('✅ Document content loaded:', documentContent);
  } catch (error) {
    console.error('❌ Failed to fetch document content:', error);
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
    const exists = allDocuments.some((doc) => getDocumentId(doc) === id);
    const success = exists ? await updateDocument(id, data) : await createDocument(id, data);

    if (success) {
      insertIdInput.value = '';
      insertJsonInput.value = '';
      documentView.value = '';
      currentlyViewedDocument = null;
      updateDeleteButtonVisibility();
      console.log(`Document ${exists ? 'updated' : 'created'} successfully`);
    } else {
      showError(`Failed to ${exists ? 'update' : 'create'} document`);
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
          console.warn(`⚠️ Skipping document ${name} due to fetch error:`, error);
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
    documentsView.innerHTML = '<div class="no-results">Unable to load documents. Please try refreshing.</div>';
  }
})();

console.log('✅ Documents script loaded');
