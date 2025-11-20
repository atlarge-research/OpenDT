# ============================================================================
# Multi-stage Dockerfile for Next.js Frontend
# ============================================================================
# Development stage: Hot Module Replacement (HMR) enabled
# Production stage: Optimized build
# ============================================================================

# Base stage - install dependencies
FROM node:20-alpine AS base

WORKDIR /app

# Install dependencies based on package.json and package-lock.json
COPY package.json package-lock.json* ./
RUN npm ci

# ============================================================================
# Development stage
# ============================================================================
FROM node:20-alpine AS development

WORKDIR /app

# Copy dependencies from base
COPY --from=base /app/node_modules ./node_modules

# Copy application code (will be overridden by volume mounts in docker-compose)
COPY . .

# Expose port for development server
EXPOSE 3000

# Set environment
ENV NODE_ENV=development

# Start development server with HMR
CMD ["npm", "run", "dev"]

# ============================================================================
# Build stage (for production)
# ============================================================================
FROM base AS builder

WORKDIR /app

# Copy all application files
COPY . .

# Build the Next.js application
ENV NODE_ENV=production
RUN npm run build

# ============================================================================
# Production stage
# ============================================================================
FROM node:20-alpine AS production

WORKDIR /app

# Create a non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy built application
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 3000

ENV NODE_ENV=production
ENV PORT=3000

# Start production server
CMD ["node", "server.js"]

# ============================================================================
# Usage:
# 
# Development (via docker-compose):
#   - Target: development
#   - Hot reload enabled via volume mounts
#   - Command: npm run dev
#
# Production:
#   - Target: production
#   - Optimized build
#   - Command: node server.js
# ============================================================================
