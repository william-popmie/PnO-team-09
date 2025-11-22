// @author Claude Sonnet 4.5
// this is a test file for testing with a front end, will not be used in the final product.
// @date 2025-11-22

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
const csvCollectionInput = document.getElementById('csvCollectionInput') as HTMLInputElement | null;
const dropzone = document.getElementById('dropzone') as HTMLDivElement;
const csvFileInput = document.getElementById('csvFileInput') as HTMLInputElement;
const uploadCsvButton = document.getElementById('uploadCsvButton') as HTMLButtonElement;
const csvStatus = document.getElementById('csvStatus') as HTMLSpanElement | null;

let selectedCsvFile: File | null = null;

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
 * Update CSV status area
 */
function setCsvStatus(msg: string, isError = false) {
  if (!csvStatus) return;
  csvStatus.textContent = msg;
  csvStatus.style.color = isError ? 'red' : 'black';
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

    const documents = await response.json();
    documentsView.textContent = JSON.stringify(documents, null, 2);
  } catch (error) {
    showError(`Failed to load documents: ${error}`);
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
    const fields = JSON.parse(jsonText);

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
  } catch (error) {
    showError(`Failed to insert document: ${error}`);
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
  } catch (error) {
    showError(`Failed to delete document: ${error}`);
  }
}

/**
 * CSV upload helpers
 */
function handleFileSelection(file: File | null) {
  selectedCsvFile = file;
  if (file) {
    setCsvStatus(`Selected: ${file.name} (${Math.round(file.size / 1024)} KB)`);
  } else {
    setCsvStatus('');
  }
}

dropzone.addEventListener('click', () => {
  csvFileInput.click();
});

csvFileInput.addEventListener('change', (e) => {
  const files = (e.target as HTMLInputElement).files;
  handleFileSelection(files && files.length > 0 ? files[0] : null);
});

// Drag & drop support
dropzone.addEventListener('dragover', (e) => {
  e.preventDefault();
  e.stopPropagation();
  dropzone.classList.add('dragover');
});

dropzone.addEventListener('dragleave', (e) => {
  e.preventDefault();
  e.stopPropagation();
  dropzone.classList.remove('dragover');
});

dropzone.addEventListener('drop', (e) => {
  e.preventDefault();
  e.stopPropagation();
  dropzone.classList.remove('dragover');
  const dt = e.dataTransfer;
  if (dt && dt.files && dt.files.length > 0) {
    handleFileSelection(dt.files[0]);
  }
});

/**
 * Upload selected CSV file to backend.
 * If an optional collection name is provided, the client will post to
 * /collections/{name}/import-csv, otherwise to /import/csv.
 *
 * Backend expectations:
 * - Form field "file" contains the CSV
 * - Optional field "collection" contains the collection name (if present)
 */
async function uploadCsv(): Promise<void> {
  if (!selectedCsvFile) {
    setCsvStatus('No CSV file selected', true);
    return;
  }

  const optionalCollection = (csvCollectionInput && csvCollectionInput.value.trim()) || '';

  try {
    clearError();
    setCsvStatus('Uploading...', false);
    uploadCsvButton.disabled = true;

    const form = new FormData();
    form.append('file', selectedCsvFile, selectedCsvFile.name);
    if (optionalCollection) form.append('collection', optionalCollection);

    // Choose endpoint depending on whether a collection name was supplied
    const endpoint = optionalCollection
      ? `${API_BASE}/collections/${encodeURIComponent(optionalCollection)}/import-csv`
      : `${API_BASE}/import/csv`;

    const resp = await fetch(endpoint, {
      method: 'POST',
      body: form,
    });

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Server responded ${resp.status}: ${text}`);
    }

    // If the backend returns JSON with details, show them
    let resultText = 'Upload successful';
    try {
      const json = await resp.json();
      resultText = `Upload succeeded: ${JSON.stringify(json)}`;
    } catch {
      // not JSON; ignore
    }

    setCsvStatus(resultText, false);

    // If user supplied a collection name, refresh the view for that collection
    if (optionalCollection) {
      await loadDocuments();
    }

    // clear selection
    selectedCsvFile = null;
    csvFileInput.value = '';
  } catch (err) {
    setCsvStatus(`Upload failed: ${err}`, true);
    showError(`CSV upload failed: ${err}`);
  } finally {
    uploadCsvButton.disabled = false;
  }
}

// Attach event listeners to buttons
loadButton.addEventListener('click', loadDocuments);
insertButton.addEventListener('click', insertDocument);
deleteButton.addEventListener('click', deleteDocument);
uploadCsvButton.addEventListener('click', uploadCsv);

// Allow pressing Enter in the collection input to load documents
collectionInput.addEventListener('keypress', (e) => {
  if (e.key === 'Enter') {
    loadDocuments();
  }
});
