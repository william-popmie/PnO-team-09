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
  const count = selectedDocuments.size;
  selectedCount.textContent = count > 0 ? `${count} selected` : '';
  deleteSelected.style.display = count > 0 ? 'block' : 'none';
}

function getDocumentId(doc: Record<string, unknown>): string {
  // TODO: Extract document ID from document object
  // Handle both 'id' and '_id' fields, fallback to JSON string slice
  return (doc['id'] as string) || (doc['_id'] as string) || JSON.stringify(doc).slice(0, 10);
}

// API functions
async function fetchDocuments(): Promise<Array<Record<string, unknown>>> {
  // TODO: Fetch documents from API endpoint
  // GET ${API_BASE}/collections/${currentCollection}/documents
  // Handle errors and return empty array on failure
  return Promise.resolve([]);
}

async function createDocument(name: string, data: Record<string, unknown>): Promise<boolean> {
  console.log('Creating document:', name, data);
  // Mock implementation
  const newDoc = { id: name, ...data, createdAt: new Date().toISOString() };
  allDocuments.push(newDoc);
  renderDocuments(allDocuments);
  return Promise.resolve(true);
}

async function updateDocument(id: string, data: Record<string, unknown>): Promise<boolean> {
  console.log('Updating document:', id, data);
  // Mock implementation
  const index = allDocuments.findIndex((doc) => getDocumentId(doc) === id);
  if (index > -1) {
    allDocuments[index] = { ...allDocuments[index], ...data, updatedAt: new Date().toISOString() };
    renderDocuments(allDocuments);
    return Promise.resolve(true);
  }
  return Promise.resolve(false);
}

async function deleteDocuments(ids: string[]): Promise<boolean> {
  console.log('Deleting documents:', ids);
  // Mock implementation
  ids.forEach((id) => {
    const index = allDocuments.findIndex((doc) => getDocumentId(doc) === id);
    if (index > -1) allDocuments.splice(index, 1);
  });
  selectedDocuments.clear();
  renderDocuments(allDocuments);
  updateSelectionUI();
  return Promise.resolve(true);
}

// Rendering functions
function renderDocuments(docs: Array<Record<string, unknown>>) {
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

function selectDocument(doc: Record<string, unknown>) {
  const docJson = JSON.stringify(doc, null, 2);
  documentView.value = docJson;
  insertIdInput.value = getDocumentId(doc);
  insertJsonInput.value = docJson;
  console.log('Selected document:', doc);
}

function toggleDocumentSelection(id: string, checkbox: HTMLElement, item: HTMLElement) {
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
async function handleRefreshDocuments() {
  try {
    clearError();
    const docs = await fetchDocuments();
    renderDocuments(docs);
    console.log('Documents refreshed');
  } catch (e) {
    showError('Failed to refresh documents: ' + getErrorMessage(e));
  }
}

async function handleInsertDocument() {
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
