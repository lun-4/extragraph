-- Migration: Add WASM plugin support to scripts table
-- Date: 2025-10-18
-- Description: Adds script_type and wasm_bytecode columns for dual Lua/WASM runtime support

-- Add script_type column (defaults to 'lua' for backward compatibility)
ALTER TABLE scripts ADD COLUMN script_type TEXT DEFAULT 'lua';

-- Add wasm_bytecode column for storing compiled WASM modules
ALTER TABLE scripts ADD COLUMN wasm_bytecode BLOB;
