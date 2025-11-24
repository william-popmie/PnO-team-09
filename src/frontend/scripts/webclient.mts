// @author Tijn Gommers
// @date 2025-19-11

/// <reference lib="dom" />

const API_BASE = 'http://localhost:3000';

console.log('🖥️ WebClient loading...', 'API:', API_BASE);

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
  const count = selectedCollections.size;
  selectedCount.textContent = count > 0 ? `${count} selected` : '';
  deleteSelected.style.display = count > 0 ? 'block' : 'none';
}

function getCollectionName(name: string): string {
  return name.trim().toLowerCase();
}

// API functions
async function fetchCollections(): Promise<string[]> {
  // TODO: Fetch collections from API endpoint
  // GET ${API_BASE}/collections
  // Handle errors and return empty array on failure
  return Promise.resolve([]);
}

async function createCollection(name: string): Promise<boolean> {
  console.log('Creating collection:', name);
  // Mock implementation
  allCollections.push(getCollectionName(name));
  renderCollections(allCollections);
  return Promise.resolve(true);
}

async function deleteCollections(names: string[]): Promise<boolean> {
  console.log('Deleting collections:', names);
  // Mock implementation
  names.forEach((name) => {
    const index = allCollections.indexOf(name);
    if (index > -1) allCollections.splice(index, 1);
  });
  selectedCollections.clear();
  renderCollections(allCollections);
  updateSelectionUI();
  return Promise.resolve(true);
}

// Rendering functions
function renderCollections(collections: string[]) {
  collectionsList.innerHTML = '';

  if (collections.length === 0) {
    collectionsList.innerHTML = '<div style="color: var(--muted);">No collections found</div>';
    return;
  }

  collections.forEach((name) => {
    const item = document.createElement('div');
    item.className = 'collection-item';
    item.innerHTML = `
      <input type="checkbox" class="collection-checkbox" data-name="${name}">
      <span class="collection-name">${name}</span>
    `;

    const checkbox = item.querySelector('.collection-checkbox') as HTMLInputElement;
    const nameSpan = item.querySelector('.collection-name') as HTMLSpanElement;

    checkbox.addEventListener('change', () => {
      toggleCollectionSelection(name, checkbox, item);
    });

    nameSpan.addEventListener('click', () => {
      selectCollection(name);
    });

    collectionsList.appendChild(item);
  });
}

function selectCollection(name: string) {
  console.log('Selecting collection:', name);
  window.location.href = `documents.html?collection=${encodeURIComponent(name)}`;
}

function toggleCollectionSelection(name: string, checkbox: HTMLElement, item: HTMLElement) {
  const isChecked = (checkbox as HTMLInputElement).checked;

  if (isChecked) {
    selectedCollections.add(name);
    item.classList.add('selected');
  } else {
    selectedCollections.delete(name);
    item.classList.remove('selected');
  }

  updateSelectionUI();
}

// Event handlers
async function handleRefreshCollections() {
  // TODO: Refresh collections list
  // Call fetchCollections() and renderCollections()
}

async function handleCreateCollection() {
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
    showError(getErrorMessage(error));
  }
}

async function handleDeleteSelected() {
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
    showError(getErrorMessage(error));
  }
}

function handleSearchCollections() {
  const query = collectionSearch.value.toLowerCase();
  const filtered = allCollections.filter((name) => name.toLowerCase().includes(query));
  renderCollections(filtered);
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

// Initialize page with demo data
void (async () => {
  try {
    const collections = await fetchCollections();
    renderCollections(collections);
  } catch (_error) {
    console.log('API not available, using demo data');
    // Demo collections
    allCollections.push('Users', 'Orders', 'Products', 'Categories');
    renderCollections(allCollections);
  }
})();

console.log('✅ WebClient ready');
