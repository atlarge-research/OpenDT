#!/bin/bash
set -e

echo "🚀 OpenDT Digital Twin Deployment Script"
echo "========================================"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    log_success "Prerequisites check passed"
}

# Setup environment
setup_environment() {
    log_info "Setting up environment..."

    # Create necessary directories
    mkdir -p data logs config

    # Set permissions
    chmod -R 755 data logs config
    chmod +x entrypoint.sh 2>/dev/null || log_warning "entrypoint.sh not found, will be handled by Docker"

    # Check for OpenAI API key
    if [ -z "$OPENAI_API_KEY" ]; then
        log_warning "OPENAI_API_KEY not set. LLM optimization will be disabled."
        log_info "To enable LLM features, set: export OPENAI_API_KEY='your-key-here'"
    else
        log_success "OpenAI API key configured"
    fi

    log_success "Environment setup complete"
}

# Build and start services
start_services() {
    log_info "Building and starting OpenDT services..."

    # Stop any existing containers
    docker-compose down || true

    # Build and start services
    if docker-compose up --build -d; then
        log_success "Services started successfully"
    else
        log_error "Failed to start services"
        exit 1
    fi
}

# Health check
health_check() {
    log_info "Performing health checks..."

    # Wait for services to be ready
    sleep 10

    # Check Kafka
    if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list &>/dev/null; then
        log_success "Kafka is healthy"
    else
        log_warning "Kafka health check failed, but continuing..."
    fi

    # Check OpenDT application
    if curl -s http://localhost:8080/api/status &>/dev/null; then
        log_success "OpenDT application is responding"
    else
        log_warning "OpenDT application not yet ready, may need more time..."
    fi

    log_info "Services are starting up. Monitor with: docker-compose logs -f"
}

# Display status
show_status() {
    echo
    echo "🎉 OpenDT Digital Twin Deployment Complete!"
    echo "=========================================="
    echo
    echo "📊 Dashboard: http://localhost:8080"
    echo "📋 API Status: http://localhost:8080/api/status"
    echo
    echo "Useful commands:"
    echo "  View logs:     docker-compose logs -f"
    echo "  View specific: docker-compose logs -f opendt"
    echo "  Stop system:   docker-compose down"
    echo "  Restart:       docker-compose restart"
    echo
    echo "Next steps:"
    echo "  1. Open http://localhost:8080 in your browser"
    echo "  2. Click 'Start System' to begin simulation"
    echo "  3. Monitor metrics and LLM recommendations"
    echo
}

# Cleanup function
cleanup() {
    log_info "Cleaning up previous deployment..."
    docker-compose down --volumes --remove-orphans || true
    docker system prune -f || true
    log_success "Cleanup complete"
}

# Main deployment function
deploy() {
    echo
    log_info "Starting OpenDT deployment..."

    check_prerequisites
    setup_environment
    start_services
    health_check
    show_status

    log_success "Deployment completed successfully!"
}

# Handle command line arguments
case "$1" in
    "start"|"deploy"|"")
        deploy
        ;;
    "stop")
        log_info "Stopping OpenDT services..."
        docker-compose down
        log_success "Services stopped"
        ;;
    "restart")
        log_info "Restarting OpenDT services..."
        docker-compose restart
        log_success "Services restarted"
        ;;
    "logs")
        log_info "Showing logs..."
        docker-compose logs -f
        ;;
    "status")
        log_info "Service status:"
        docker-compose ps
        ;;
    "clean")
        cleanup
        ;;
    "help"|"-h"|"--help")
        echo "OpenDT Deployment Script"
        echo
        echo "Usage: $0 [command]"
        echo
        echo "Commands:"
        echo "  start/deploy  - Deploy and start services (default)"
        echo "  stop          - Stop all services"
        echo "  restart       - Restart services"
        echo "  logs          - View logs"
        echo "  status        - Show service status"
        echo "  clean         - Clean up containers and volumes"
        echo "  help          - Show this help message"
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac