# WASM Filter Examples

This directory contains example WASM plugins for the Extragraph feed generator. WASM plugins provide better memory isolation and support for multiple programming languages beyond Lua.

## Plugin Interface

All WASM plugins must export a `filter` function that:
- Takes JSON input with the post context
- Returns a JSON boolean (`true` or `false`)

### Input Format

```json
{
  "author_did": "did:plc:...",
  "post": {
    "text": "Post content here",
    "createdAt": "2025-10-18T...",
    ...
  },
  "repost": null,
  "follows": {
    "did:plc:user1": 1,
    "did:plc:user2": 1
  },
  "followed": {
    "did:plc:user3": 1
  }
}
```

### Output Format

Simply return `true` to include the post in the feed, or `false` to exclude it:

```json
true
```

## Examples

### JavaScript Example

See `filter.js` for a simple keyword filter in JavaScript.

### Rust Example

See `filter-rust/` for a high-performance filter written in Rust.

## Building Plugins

### JavaScript

Requires: [Extism JS PDK](https://github.com/extism/js-pdk)

```bash
npm install -g @extism/js-pdk
extism-js filter.js -o filter.wasm
```

### Rust

Requires: Rust toolchain with `wasm32-unknown-unknown` target

```bash
cd filter-rust
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/filter.wasm ../
```

## Deploying Plugins

Use the `feedgen-admin` tool to deploy a WASM plugin:

```bash
./feedgen-admin setscript-wasm <did> <slot> <path-to-wasm>
```

Example:
```bash
./feedgen-admin setscript-wasm did:plc:user123 1 ./filter.wasm
```

## Converting from Lua

If you have existing Lua scripts, you can still use them! The system supports both:

- **Lua scripts** (legacy): Use `setscript` command
- **WASM plugins** (recommended): Use `setscript-wasm` command

Both can coexist in the same deployment.
