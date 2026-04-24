# AGENTS.md — multiplayer-fabric-webtransport

Guidance for AI coding agents working in this submodule.

## What this is

Elixir library wrapping a Rust NIF (`wtransport` crate) to provide
WebTransport client and server primitives. Used by `multiplayer-fabric-zone-console`
for zone connections over QUIC/WebTransport (UDP port 7443).

## Build and test

```sh
mix compile     # compiles Rust NIF via rustler
mix test
```

Rust toolchain must be installed. The NIF is compiled into `priv/`.

## Key files

| Path | Purpose |
|------|---------|
| `mix.exs` | Deps: rustler, typed_struct, dialyxir |
| `lib/wtransport.ex` | Typed structs: `SessionRequest`, `Session`, `ConnectionRequest`, `StreamRequest` |
| `lib/wtransport_native.ex` | Rustler NIF loader |
| `native/wtransport_native/` | Rust crate implementing the NIF (`Cargo.toml`, `src/`) |

## Conventions

- Declared as a GitHub dep in `zone_console`: `{:wtransport, github: "V-Sekai-fire/multiplayer-fabric-webtransport"}`.
  Tag or SHA bumps in zone_console must stay in sync with this repo's main branch.
- All Elixir structs use `TypedStruct` — keep field types explicit.
- Every new `.ex` / `.exs` file needs SPDX headers:
  ```elixir
  # SPDX-License-Identifier: MIT
  # Copyright (c) 2026 K. S. Ernest (iFire) Lee
  ```
- Commit message style: sentence case, no `type(scope):` prefix.
  Example: `Expose datagram send channel in ConnectionRequest struct`
