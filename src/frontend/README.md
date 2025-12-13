# SimpleDBMS Frontend

A lightweight web interface for SimpleDBMS - a document database management system.

## Getting Started

### Prerequisites
- Node.js (v18 or higher)
- npm

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd team-09
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment variables**
   
   Create a `.env` file in the project root:
   ```bash
   touch .env
   ```

   Add the following configuration to `.env`:
   ```env
   PORT=3000
   JWT_SECRET=my-super-secret-jwt-key-change-this-in-production-12345
   SESSION_SECRET=my-session-secret-key-also-change-this-67890
   ```

   **About the secrets:**
   - `JWT_SECRET`: Used to sign and verify JSON Web Tokens for API authentication. Should be a long, random string.
   - `SESSION_SECRET`: Used to sign session cookies for browser-based authentication. Should be a different random string.
   
   > ⚠️ In production, use strong random strings (e.g., generate with `openssl rand -base64 32`)

4. **Build and run the server**
   ```bash
   npm run dev
   ```

   The server will start on `http://localhost:3000` (or the port specified in `.env`).

## Usage

Once the server is running, open your browser and navigate to:
- **Main Dashboard**: `http://localhost:3000/`
- **Login**: `http://localhost:3000/login.html`
- **Signup**: `http://localhost:3000/signup.html`

### Demo Account

The project includes a pre-populated demo account for testing and demonstrations. When you create a new database, a demo account is automatically created with sample data including customers, products, orders, inventory, and analytics.

**Demo Credentials:**
- **Username**: `demo`
- **Password**: `demo123`

The demo account includes:
- 5 collections (customers, products, orders, inventory, analytics)
- 50+ documents with realistic data
- Nested objects and arrays to showcase complex data structures
- Various data types (strings, numbers, booleans, objects, arrays)
- 15 customer profiles with complete address information
- 20+ product listings across multiple categories
- 12 order records with detailed transaction information

> **Note:** The demo account is only created when initializing a new database. To customize the demo data, edit `data/dummy-account.json` before starting the server for the first time.

## Features

- User authentication (signup/login)
- Collection management (create, view, delete)
- Document operations (create, read, update, delete)
- Clean and responsive UI

## Project Structure

```
frontend/
├── components/     # HTML pages
├── scripts/        # TypeScript/JavaScript frontend logic
└── styles/         # CSS stylesheets
```

## Development

- **Build TypeScript**: `npm run build`
- **Run tests**: `npm test`
- **Lint code**: `npm run lint`

### Resetting the Database (Developer Use)

To start fresh with a blank database:

1. **Stop the server** (if running)
   ```bash
   # Press Ctrl+C in the terminal running the server
   ```

2. **Reset the database**
   ```bash
   npm run reset-db
   ```

3. **Restart the server**
   ```bash
   npm run dev
   ```

The database will be recreated automatically on startup. All users, collections, and documents will be cleared, and the demo account will be re-initialized with fresh sample data.
