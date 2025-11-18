// @author Tijn Gommers
// @date 2025-17-11

/// <reference lib="dom" />

// Base URL for the API - adjust port if needed
const API_BASE = 'http://localhost:3000';

// Get references to DOM elements
const collectionInput = document.getElementById('collectionInput') as HTMLInputElement;
const loadButton = document.getElementById('loadButton') as HTMLButtonElement;
const documentsView = document.getElementById('documentsView') as HTMLPreElement;
const insertIdInput = document.getElementById('insertIdInput') as HTMLInputElement;
const insertJsonInput = document.getElementById('insertJsonInput') as HTMLTextAreaElement;
const insertButton = document.getElementById('insertButton') as HTMLButtonElement;
const deleteIdInput = document.getElementById('deleteIdInput') as HTMLInputElement;
const deleteButton = document.getElementById('deleteButton') as HTMLButtonElement;
const errorDiv = document.getElementById('error') as HTMLDivElement;

/**
 * Display an error message to the user
 */
function showError(message: string): void {
  errorDiv.textContent = message;
  setTimeout(() => {
    errorDiv.textContent = '';
  }, 5000);
}

/**
 * Clear the error message
 */
function clearError(): void {
  errorDiv.textContent = '';
}

/**
 * Safely convert unknown error to string
 */
function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Load all documents from a collection
 */
async function loadDocuments(): Promise<void> {
  const collectionName = collectionInput.value.trim();

  if (!collectionName) {
    showError('Please enter a collection name');
    return;
  }

  try {
    clearError();
    const response = await fetch(`${API_BASE}/collections/${collectionName}`);

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const documents: unknown = await response.json();

    if (!Array.isArray(documents)) {
      throw new Error('Invalid response format: expected an array of documents');
    }

    documentsView.textContent = JSON.stringify(documents, null, 2);
  } catch (error: unknown) {
    showError(`Failed to load documents: ${getErrorMessage(error)}`);
    documentsView.textContent = '';
  }
}

/**
 * Insert a new document into the collection
 */
async function insertDocument(): Promise<void> {
  const collectionName = collectionInput.value.trim();
  const id = insertIdInput.value.trim();
  const jsonText = insertJsonInput.value.trim();

  if (!collectionName) {
    showError('Please enter a collection name');
    return;
  }

  if (!id) {
    showError('Please enter a document ID');
    return;
  }

  if (!jsonText) {
    showError('Please enter document fields as JSON');
    return;
  }

  try {
    clearError();
    // Parse the JSON input
    const fields: unknown = JSON.parse(jsonText);

    if (typeof fields !== 'object' || fields === null || Array.isArray(fields)) {
      throw new Error('Invalid JSON: expected an object');
    }

    // Combine id with the parsed fields
    const document = { id, ...fields };

    const response = await fetch(`${API_BASE}/collections/${collectionName}/documents`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(document),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Clear the input fields
    insertIdInput.value = '';
    insertJsonInput.value = '';

    // Reload the documents to show the new one
    await loadDocuments();
  } catch (error: unknown) {
    showError(`Failed to insert document: ${getErrorMessage(error)}`);
  }
}

/**
 * Delete a document by its ID
 */
async function deleteDocument(): Promise<void> {
  const collectionName = collectionInput.value.trim();
  const id = deleteIdInput.value.trim();

  if (!collectionName) {
    showError('Please enter a collection name');
    return;
  }

  if (!id) {
    showError('Please enter a document ID to delete');
    return;
  }

  try {
    clearError();
    const response = await fetch(`${API_BASE}/collections/${collectionName}/documents/${id}`, {
      method: 'DELETE',
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Clear the delete input
    deleteIdInput.value = '';

    // Reload the documents to show the updated list
    await loadDocuments();
  } catch (error: unknown) {
    showError(`Failed to delete document: ${getErrorMessage(error)}`);
  }
}

// Attach event listeners to buttons
loadButton.addEventListener('click', () => {
  void loadDocuments();
});
insertButton.addEventListener('click', () => {
  void insertDocument();
});
deleteButton.addEventListener('click', () => {
  void deleteDocument();
});

// Allow pressing Enter in the collection input to load documents
collectionInput.addEventListener('keypress', (e: KeyboardEvent) => {
  if (e.key === 'Enter') void loadDocuments();
});
