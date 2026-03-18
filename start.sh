#!/bin/bash

# Portal JUST Downloader - Start Script

echo "=========================================="
echo "  Portal JUST Downloader - Docker Setup"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo ""
echo "🚀 Starting services..."
echo ""

# Use docker compose (v2) if available, otherwise docker-compose (v1)
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Build and start
$COMPOSE_CMD up -d --build

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check status
echo ""
echo "📊 Service Status:"
$COMPOSE_CMD ps

echo ""
echo "=========================================="
echo "  ✅ Setup Complete!"
echo "=========================================="
echo ""
echo "  🌐 Frontend:  http://localhost:3000"
echo "  🔌 Backend:   http://localhost:8001/api/"
echo "  🐘 PostgreSQL: localhost:5432"
echo "  🍃 MongoDB:    localhost:27017"
echo ""
echo "  📝 View logs:  $COMPOSE_CMD logs -f"
echo "  🛑 Stop:       $COMPOSE_CMD down"
echo ""
