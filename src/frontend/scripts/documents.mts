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
const documentView = getEl<HTMLTextAreaElement>('documentView');
const errorDiv = getEl<HTMLDivElement>('error');
const documentFieldsContainer = getEl<HTMLDivElement>('documentFields');
const addFieldBtn = getEl<HTMLButtonElement>('addFieldBtn');

// =========================
// State Management
// =========================
const allDocuments: Array<Record<string, unknown>> = [];
let currentlyViewedDocument: string | null = null;
let originalDocumentName: string | null = null; // Track which document is being edited
const currentCollection = new URLSearchParams(window.location.search).get('collection') || 'unknown';
collectionNameSpan.textContent = currentCollection;

// Pagination state
const ITEMS_PER_PAGE = 10;
let currentPage = 1;
let totalPages = 1;

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
  clearDocumentFields();
  currentlyViewedDocument = null;
  updateDeleteButtonVisibility();
  renderDocuments(allDocuments); // Re-render to remove viewing highlight
}

/**
 * Adds a new key-value field row to the document fields container with nested object support
 * @param {string} key - The field key
 * @param {unknown} value - The field value (can be primitive or object)
 * @param {number} level - Nesting level for indentation
 * @param {HTMLElement} parent - Parent container element
 * @return {HTMLElement} The created field row element
 */
function addDocumentField(key = '', value: unknown = '', level = 0, parent?: HTMLElement): HTMLElement {
  const container = parent || documentFieldsContainer;

  // Main field row wrapper
  const fieldRow = document.createElement('div');
  fieldRow.className = 'field-row';
  fieldRow.style.paddingLeft = `${level * 24}px`;
  fieldRow.dataset['level'] = String(level);

  // Row content (expand button + inputs + action buttons)
  const rowContent = document.createElement('div');
  rowContent.className = 'field-row-content';

  // Expand/collapse button for nested objects
  const expandBtn = document.createElement('button');
  expandBtn.textContent = '▶';
  expandBtn.type = 'button';
  expandBtn.className = 'btn btn-ghost expand-btn';
  expandBtn.dataset['expanded'] = 'false';

  const keyInput = document.createElement('input');
  keyInput.type = 'text';
  keyInput.placeholder = 'Field name';
  keyInput.value = key;
  keyInput.className = 'field-key';

  const valueInput = document.createElement('input');
  valueInput.type = 'text';
  valueInput.placeholder = 'Value (or add nested fields)';
  valueInput.className = 'field-value';

  // Add nested field button
  const addNestedBtn = document.createElement('button');
  addNestedBtn.textContent = '+';
  addNestedBtn.type = 'button';
  addNestedBtn.title = 'Add nested field';
  addNestedBtn.className = 'btn add-nested-btn';

  // Remove field button
  const removeBtn = document.createElement('button');
  removeBtn.textContent = '✕';
  removeBtn.type = 'button';
  removeBtn.className = 'btn remove-field-btn';

  // Nested fields container
  const nestedContainer = document.createElement('div');
  nestedContainer.className = 'nested-fields';
  nestedContainer.dataset['nested'] = 'true';

  // Handle nested value if it's an object
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    // This field represents an object, show expand button
    expandBtn.classList.add('visible');
    expandBtn.dataset['expanded'] = 'true';
    expandBtn.style.transform = 'rotate(90deg)';
    nestedContainer.classList.add('visible');
    valueInput.value = ''; // Empty value for objects
    valueInput.disabled = true;
    valueInput.placeholder = '(nested object)';

    // Add nested fields recursively
    for (const [nestedKey, nestedValue] of Object.entries(value as Record<string, unknown>)) {
      addDocumentField(nestedKey, nestedValue, level + 1, nestedContainer);
    }
  } else if (value !== null && value !== undefined && value !== '') {
    // Primitive value
    if (typeof value === 'string') {
      valueInput.value = value;
    } else if (typeof value === 'boolean' || typeof value === 'number') {
      valueInput.value = String(value);
    } else {
      valueInput.value = JSON.stringify(value);
    }
  }

  // Expand/collapse functionality
  expandBtn.addEventListener('click', () => {
    const isExpanded = expandBtn.dataset['expanded'] === 'true';
    expandBtn.dataset['expanded'] = String(!isExpanded);
    expandBtn.style.transform = isExpanded ? 'rotate(0deg)' : 'rotate(90deg)';
    nestedContainer.classList.toggle('visible', !isExpanded);
  });

  // Add nested field functionality
  addNestedBtn.addEventListener('click', () => {
    // Show nested container and expand button
    if (!expandBtn.classList.contains('visible')) {
      expandBtn.classList.add('visible');
      expandBtn.dataset['expanded'] = 'true';
      expandBtn.style.transform = 'rotate(90deg)';
      nestedContainer.classList.add('visible');
      valueInput.value = '';
      valueInput.disabled = true;
      valueInput.placeholder = '(nested object)';
    }

    // Add a new empty nested field
    addDocumentField('', '', level + 1, nestedContainer);
  });

  // Remove field functionality
  removeBtn.addEventListener('click', () => {
    fieldRow.remove();
  });

  // Re-enable value input if nested container becomes empty
  const observer = new MutationObserver(() => {
    if (nestedContainer.children.length === 0) {
      expandBtn.classList.remove('visible');
      nestedContainer.classList.remove('visible');
      valueInput.disabled = false;
      valueInput.placeholder = 'Value (or add nested fields)';
    }
  });
  observer.observe(nestedContainer, { childList: true });

  // Assemble the field row
  rowContent.appendChild(expandBtn);
  rowContent.appendChild(keyInput);
  rowContent.appendChild(valueInput);
  rowContent.appendChild(addNestedBtn);
  rowContent.appendChild(removeBtn);

  fieldRow.appendChild(rowContent);
  fieldRow.appendChild(nestedContainer);
  container.appendChild(fieldRow);

  return fieldRow;
}

/**
 * Clears all document fields from the container
 * @return {void}
 */
function clearDocumentFields(): void {
  documentFieldsContainer.innerHTML = '';
}

/**
 * Converts document fields to JSON object recursively
 * @param {HTMLElement} container - The container element with field rows
 * @return {Record<string, unknown>} The JSON object
 */
function fieldsToJson(container: HTMLElement = documentFieldsContainer): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  // Get direct children field rows (not nested ones)
  const fieldRows = Array.from(container.children).filter((child) => child.classList.contains('field-row'));

  fieldRows.forEach((row) => {
    const keyInput = row.querySelector('.field-key') as HTMLInputElement;
    const valueInput = row.querySelector('.field-value') as HTMLInputElement;
    const nestedContainer = row.querySelector('.nested-fields') as HTMLElement;

    if (keyInput && keyInput.value.trim()) {
      const key = keyInput.value.trim();

      // Check if this field has nested children
      if (nestedContainer && nestedContainer.children.length > 0) {
        // Recursively process nested fields
        result[key] = fieldsToJson(nestedContainer);
      } else if (valueInput) {
        // Process primitive value
        let value: unknown = valueInput.value;

        if (typeof value === 'string') {
          // Smart type conversion for primitives
          if (value === 'true') value = true;
          else if (value === 'false') value = false;
          else if (value === 'null') value = null;
          else if (value === '') value = '';
          else if (!isNaN(Number(value)) && value !== '') value = Number(value);
        }

        result[key] = value;
      }
    }
  });

  return result;
}

/**
 * Populates document fields from JSON object recursively
 * @param {Record<string, unknown>} data - The JSON object
 * @return {void}
 */
function jsonToFields(data: Record<string, unknown>): void {
  clearDocumentFields();

  for (const [key, value] of Object.entries(data)) {
    // addDocumentField now handles nested objects recursively
    addDocumentField(key, value);
  }

  // Add one empty field if no fields exist
  if (Object.keys(data).length === 0) {
    addDocumentField();
  }
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

  // Calculate pagination
  totalPages = Math.ceil(docs.length / ITEMS_PER_PAGE);
  if (currentPage > totalPages) currentPage = totalPages;
  if (currentPage < 1) currentPage = 1;

  const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
  const endIndex = Math.min(startIndex + ITEMS_PER_PAGE, docs.length);
  const paginatedDocs = docs.slice(startIndex, endIndex);

  // Render paginated documents
  paginatedDocs.forEach((doc) => {
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

  // Render pagination controls
  renderPaginationControls(docs.length, startIndex, endIndex);

  updateDeleteButtonVisibility();
}

/**
 * Renders pagination controls at the bottom of the document list
 * @param {number} totalDocs - Total number of documents
 * @param {number} startIndex - Starting index of current page
 * @param {number} endIndex - Ending index of current page
 * @return {void}
 */
function renderPaginationControls(totalDocs: number, startIndex: number, endIndex: number): void {
  const paginationDiv = document.createElement('div');
  paginationDiv.className = 'pagination-controls';
  paginationDiv.style.cssText =
    'display: flex; justify-content: space-between; align-items: center; margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--border);';

  // Page info
  const pageInfo = document.createElement('div');
  pageInfo.style.cssText = 'color: var(--muted); font-size: 14px;';
  pageInfo.textContent = `Showing ${startIndex + 1}-${endIndex} of ${totalDocs}`;

  // Navigation buttons
  const navButtons = document.createElement('div');
  navButtons.style.cssText = 'display: flex; gap: 8px;';

  // Previous button
  const prevBtn = document.createElement('button');
  prevBtn.className = 'btn btn-ghost';
  prevBtn.textContent = 'Previous';
  prevBtn.disabled = currentPage === 1;
  prevBtn.style.cssText = currentPage === 1 ? 'opacity: 0.5; cursor: not-allowed;' : '';
  prevBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage--;
      renderDocuments(allDocuments);
    }
  });

  // Page indicator
  const pageIndicator = document.createElement('span');
  pageIndicator.style.cssText =
    'color: var(--accent); font-weight: 600; padding: 0 12px; display: flex; align-items: center;';
  pageIndicator.textContent = `Page ${currentPage} of ${totalPages}`;

  // Next button
  const nextBtn = document.createElement('button');
  nextBtn.className = 'btn btn-ghost';
  nextBtn.textContent = 'Next';
  nextBtn.disabled = currentPage === totalPages;
  nextBtn.style.cssText = currentPage === totalPages ? 'opacity: 0.5; cursor: not-allowed;' : '';
  nextBtn.addEventListener('click', () => {
    if (currentPage < totalPages) {
      currentPage++;
      renderDocuments(allDocuments);
    }
  });

  navButtons.appendChild(prevBtn);
  navButtons.appendChild(pageIndicator);
  navButtons.appendChild(nextBtn);

  paginationDiv.appendChild(pageInfo);
  paginationDiv.appendChild(navButtons);
  documentsView.appendChild(paginationDiv);
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
    jsonToFields(documentContent); // Populate fields from content
    currentlyViewedDocument = documentName;
    originalDocumentName = null; // Reset when viewing a document (not editing yet)
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
    jsonToFields(content); // Populate fields from content

    // Open the modal
    const insertModal = document.getElementById('insertModalOverlay');
    insertModal?.classList.add('show');

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

    if (!id) {
      showError('Please provide a document name');
      return;
    }

    const data = fieldsToJson();

    if (Object.keys(data).length === 0) {
      showError('Please add at least one field');
      return;
    }

    // Check if we're in edit mode (originalDocumentName is set)
    const isEditMode = originalDocumentName !== null;
    const success =
      isEditMode && originalDocumentName
        ? await updateDocument(originalDocumentName, data)
        : await createDocument(id, data);

    if (success) {
      insertIdInput.value = '';
      insertIdInput.disabled = false; // Re-enable name input
      clearDocumentFields();
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
    showError('Invalid data or error: ' + getErrorMessage(e));
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

  // Add Field button
  addFieldBtn.addEventListener('click', () => {
    addDocumentField();
  });

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
    clearDocumentFields();
    addDocumentField(); // Start with one empty field
    insertModal?.classList.add('show');
    nameInput?.focus();
  });

  cancelInsertBtn?.addEventListener('click', () => {
    originalDocumentName = null;
    insertIdInput.disabled = false;
    insertModal?.classList.remove('show');
    if (nameInput) nameInput.value = '';
    clearDocumentFields();
  });

  // Close insert modal when confirm button is clicked
  confirmInsertBtn?.addEventListener('click', () => {
    originalDocumentName = null;
    insertIdInput.disabled = false;
    insertModal?.classList.remove('show');
    if (nameInput) nameInput.value = '';
    clearDocumentFields();
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
      clearDocumentFields();
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
        clearDocumentFields();
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
