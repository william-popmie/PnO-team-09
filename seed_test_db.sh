#!/usr/bin/env bash

# Base URL for the SimpleDBMS API
BASE_URL="http://localhost:3000/db"

echo "=========================================="
echo " Seeding SimpleDBMS with Test Data..."
echo "=========================================="

# Create 'users' collection
echo "Creating 'users' collection..."
curl -s -X POST "${BASE_URL}" \
  -H "Content-Type: application/json" \
  -d '{"name": "users"}' > /dev/null

# Insert data into 'users'
echo "Inserting users..."
curl -s -X POST "${BASE_URL}/users/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {
        "type": "insert",
        "document": { "id": "u1", "name": "Alice Smith", "age": 28, "email": "alice@example.com", "isActive": true }
      },
      {
        "type": "insert",
        "document": { "id": "u2", "name": "Bob Jones", "age": 34, "email": "bob@example.com", "isActive": false }
      },
      {
        "type": "insert",
        "document": { "id": "u3", "name": "Charlie Brown", "age": 22, "email": "charlie@example.com", "isActive": true }
      }
    ]
  }' > /dev/null

# Create 'products' collection
echo "Creating 'products' collection..."
curl -s -X POST "${BASE_URL}" \
  -H "Content-Type: application/json" \
  -d '{"name": "products"}' > /dev/null

# Insert data into 'products'
echo "Inserting products..."
curl -s -X POST "${BASE_URL}/products/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {
        "type": "insert",
        "document": { "id": "p1", "name": "Laptop", "price": 999.99, "stock": 50 }
      },
      {
        "type": "insert",
        "document": { "id": "p2", "name": "Smartphone", "price": 499.50, "stock": 200 }
      },
      {
        "type": "insert",
        "document": { "id": "p3", "name": "Headphones", "price": 199.99, "stock": 0 }
      }
    ]
  }' > /dev/null


# Create 'orders' collection
echo "Creating 'orders' collection..."
curl -s -X POST "${BASE_URL}" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders"}' > /dev/null

# Insert data into 'orders'
echo "Inserting orders..."
curl -s -X POST "${BASE_URL}/orders/bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {
        "type": "insert",
        "document": { "id": "o1", "userId": "u1", "productId": "p1", "quantity": 1, "status": "shipped" }
      },
      {
        "type": "insert",
        "document": { "id": "o2", "userId": "u2", "productId": "p2", "quantity": 2, "status": "processing" }
      },
      {
        "type": "insert",
        "document": { "id": "o3", "userId": "u1", "productId": "p3", "quantity": 1, "status": "delivered" }
      }
    ]
  }' > /dev/null

# Create an index on users 'age'
echo "Creating index on 'users.age'..."
curl -s -X POST "${BASE_URL}/users/indexes/age" > /dev/null

echo "=========================================="
echo "Seeding Complete!"
echo "You can now test these collections at http://localhost:3000/api-docs"
echo "Try running 'GET /db' or 'GET /db/users'"
echo "=========================================="
