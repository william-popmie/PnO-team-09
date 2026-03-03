# @author Maarten Haine
# @date 2026-03-03

.PHONY: all build dev test clean up down logs restart

# Default target
all: build

# NPM commands
build:
	npm run build

dev:
	npm run dev

test:
	npm run test

clean:
	npm run clean
	rm -rf node_modules build

# Docker Compose commands
up:
	docker-compose up -d --build

down:
	docker-compose down

logs:
	docker-compose logs -f

restart: down up
