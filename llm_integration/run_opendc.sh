#!/bin/bash

# OpenDC Script Launcher for macOS/Linux
# This script sets up the environment and runs the adapted OpenDC optimization script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}OpenDC Optimization Script Launcher${NC}"
echo "=========================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        echo -e "${RED}Error: Python is not installed or not in PATH${NC}"
        exit 1
    else
        PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi

echo -e "${GREEN}Using Python: $PYTHON_CMD${NC}"

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo -e "${YELLOW}Warning: Java not found in PATH. The script will attempt to find it automatically.${NC}"
else
    JAVA_VERSION=$(java -version 2>&1 | head -n 1)
    echo -e "${GREEN}Java found: $JAVA_VERSION${NC}"
fi

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source .venv/bin/activate
fi

# Install requirements if they don't exist
if [ -f "requirements.txt" ]; then
    echo -e "${YELLOW}Checking Python dependencies...${NC}"
    pip install -q -r requirements.txt
fi

# Check if OpenAI API key is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo -e "${YELLOW}Warning: OPENAI_API_KEY environment variable not set.${NC}"
    echo "You may need to set it or modify the script directly."
fi

# Create data directory if it doesn't exist
mkdir -p data/{topologies,outputs/raw-output/0/seed=0}

echo -e "${GREEN}Starting OpenDC optimization script...${NC}"
echo "=========================================="

# Run the script
$PYTHON_CMD llm_integrator.py

echo -e "${GREEN}Script execution completed.${NC}"
