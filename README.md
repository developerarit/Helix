# Helix

Helix is a terminal-native AI chat application powered by Bun, Rust, and local LLMs via Ollama.

## Structure

```text
helix/
├── relay/   # Bun terminal UI
└── core/    # Rust AI backend
```

## Features

- Local AI chat
- Streaming responses
- Terminal-native interface
- Ollama integration
- Bun + Rust architecture

## Development

### Start core

```bash
cd core
cargo run
```

### Start relay

```bash
cd relay
bun install
bun run dev
```

## Requirements

- Bun
- Rust
- Ollama

## Ollama

Start Ollama locally before running Helix.

```bash
ollama run llama3
```

## License

MIT
