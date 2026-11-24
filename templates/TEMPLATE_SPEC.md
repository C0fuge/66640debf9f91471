# Plugin templates: what `ch plugin new` does

`ch plugin new <plugin_id> [--lang python|rust] [--name <display name>] [--author <name>]
[--dir <path>] [--sdk-path <checkout>]` copies `templates/<lang>/` (default `python`) to
`<path>` (default `./<plugin_id>`) and fills in the placeholders. `templates/test_templates.sh`
does the same steps with sample values and runs the result; keep the two in step.

## Placeholders

| placeholder | default | rule |
| --- | --- | --- |
| `{{plugin_id}}` | the positional argument | `^[a-z][a-z0-9_]{1,31}$` (manifest `id`, `schemas/manifest.json`) |
| `{{display_name}}` | `plugin_id` with `_` → space, each word capitalised | non-empty, one line |
| `{{author}}` | `git config user.name`, else `$USER` | one line, may be empty |
| `{{module_name}}` (python) | `<plugin_id>_plugin` | `^[a-z_][a-z0-9_]*$`, not a Python keyword |
| `{{crate_name}}` (rust) | `<plugin_id>-plugin` with `_` → `-` | `^[a-z][a-z0-9_-]*$` |

Rules for every value:

- Substitution is plain text replacement of `{{name}}`, in file contents and in path
  components. There is no other template syntax. After substitution no `{{` may remain.
- Values land inside JSON and TOML strings verbatim, so reject `"`, `\` and control
  characters in `display_name` and `author` (or escape them per file type).
- Refuse a `plugin_id` that names a built-in plugin (`fake`, `telegram`, `whatsapp`, `max`)
  unless `--force`: a user plugin with the same id shadows the built-in one (spec 3.1).
- Refuse a destination that exists and is not empty.

## Files

```
python/                               rust/
  plugin.json                           plugin.json
  pyproject.toml          (sdk line)    Cargo.toml              (sdk line)
  README.md                             README.md
  src/{{module_name}}/    (rename)      src/main.rs
    __init__.py __main__.py plugin.py   tests/plugin.rs
  tests/test_plugin.py
```

The only path to rename is `python/src/{{module_name}}/`. Copy everything else as is;
`test_templates.sh` and this file are not part of a template.

## SDK dependency

The line ending in `# ch:sdk-source` (in `pyproject.toml` and `Cargo.toml`) points at the
SDK in the Contact Hub git repo. When `ch` runs from a source checkout, or with
`--sdk-path`, replace the whole line with a path dependency:

- python: `contact-hub-sdk = { path = "<checkout>/sdk/python" }`
- rust: `ch-plugin-sdk = { path = "<checkout>/crates/ch-plugin-sdk" }`

Otherwise leave it (drop the marker comment either way).

## After copying

- python: `uv sync` (creates `.venv`, which `plugin.json`'s `entry` points at), then
  `uv run pytest`.
- rust: `cargo build`, then `cargo test`. `entry` is `./target/release/<crate_name>`, so
  tell the user to run `cargo build --release` before installing.
- Print the next steps: the destination, `ch plugin test <path>`, and where user plugins
  are installed (`$XDG_DATA_HOME/contact-hub-ng/plugins/<plugin_id>/`).
- With `--no-install`, skip `uv sync` / `cargo build`.
