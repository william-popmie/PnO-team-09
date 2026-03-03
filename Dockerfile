# @author Maarten Haine
# @date 2026-03-03

# Use Node.js based on Alpine Linux for a smaller image
FROM node:22-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json first to leverage Docker cache
COPY package*.json ./

# Install dependencies cleanly
RUN npm ci

# Copy the rest of the application code
COPY . .

# Build the TypeScript code
RUN npm run build

# Expose the API port used by simpledbmsd.mts
EXPOSE 3000

# Command to run the application
CMD ["node", "build/simpledbmsd.mjs"]
