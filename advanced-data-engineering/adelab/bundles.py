"""Course 4 helpers: run the Databricks CLI from notebooks and inspect bundles offline.

`databricks bundle validate / deploy / run` need a workspace. Without one you can still:
  * preview()   - merge a target into the bundle config and substitute ${var.*} / ${bundle.*}, offline
  * validate_schema() - check the merged config against the CLI's own JSON schema (`databricks bundle schema`)
  * run the project's unit tests locally
`databricks bundle validate` against a real workspace stays the source of truth.
"""
from __future__ import annotations

import copy
import json
import os
import re
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path

import yaml

from .spark import DATA_ROOT


# --------------------------------------------------------------------------- CLI


@lru_cache(maxsize=1)
def cli_path() -> str | None:
    found = shutil.which("databricks")
    if found:
        return found
    winget = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet"
    hits = list(winget.rglob("databricks.exe")) if winget.exists() else []
    return str(hits[0]) if hits else None


def cli(*args: str, cwd: str | os.PathLike | None = None, check: bool = False, echo: bool = True,
        timeout: int = 900) -> subprocess.CompletedProcess:
    """Run `databricks <args>` and print its output, like a %sh cell in a Databricks notebook."""
    exe = cli_path()
    if exe is None:
        raise FileNotFoundError("Databricks CLI not found. Install it: winget install Databricks.DatabricksCLI "
                                "(or see 04-automated-deployment-dabs/README.md).")
    if echo:
        print(f"$ databricks {' '.join(args)}" + (f"   (in {cwd})" if cwd else ""))
    p = subprocess.run([exe, *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout)
    noise = "Databricks skills are not installed"
    out = "\n".join(l for l in (p.stdout + p.stderr).splitlines() if noise not in l and "aitools install" not in l)
    if echo and out.strip():
        print(out)
    if check and p.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args)} failed with exit code {p.returncode}")
    return p


def workspace_status(profile: str | None = None) -> dict:
    """Is a workspace configured for this machine? (profile in ~/.databrickscfg or DATABRICKS_HOST env)"""
    if cli_path() is None:
        return {"cli": False, "configured": False, "reason": "Databricks CLI not installed"}
    args = ["current-user", "me", "-o", "json"] + (["-p", profile] if profile else [])
    p = cli(*args, echo=False, timeout=60)
    if p.returncode == 0:
        try:
            user = json.loads(p.stdout).get("userName")
        except json.JSONDecodeError:
            user = None
        return {"cli": True, "configured": True, "user": user, "profile": profile or "DEFAULT"}
    return {"cli": True, "configured": False,
            "reason": (p.stderr or p.stdout).strip().splitlines()[-1][:200] if (p.stderr or p.stdout) else ""}


def require_workspace(status: dict) -> bool:
    if status.get("configured"):
        return True
    print("SKIPPED - no Databricks workspace configured "
          f"({status.get('reason', '')}).\nRun the authentication steps in 4.1 to execute this cell for real.")
    return False


# --------------------------------------------------------------------------- offline preview


def _deep_merge(base, override):
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for k, v in override.items():
            out[k] = _deep_merge(base.get(k), v) if k in base else copy.deepcopy(v)
        return out
    return copy.deepcopy(override)


def load_bundle(bundle_dir: str | os.PathLike) -> dict:
    """databricks.yml + every file matched by `include`, merged (no target applied)."""
    root = Path(bundle_dir)
    cfg = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8")) or {}
    for pattern in cfg.get("include", []) or []:
        for f in sorted(root.glob(pattern)):
            cfg = _deep_merge(cfg, yaml.safe_load(f.read_text(encoding="utf-8")) or {})
    return cfg


_REF = re.compile(r"\$\{([^}]+)\}")


def preview(bundle_dir: str | os.PathLike, target: str | None = None, variables: dict | None = None,
            user: str = "you@example.com", show: bool = True) -> dict:
    """Approximate `databricks bundle validate --output json` offline: apply the target, resolve ${...}."""
    cfg = load_bundle(bundle_dir)
    targets = cfg.get("targets", {}) or {}
    if target is None:
        target = next((n for n, t in targets.items() if (t or {}).get("default")), next(iter(targets), None))
    if target not in targets:
        raise KeyError(f"unknown target '{target}', available: {list(targets)}")
    t = targets[target] or {}
    merged = {k: v for k, v in cfg.items() if k not in ("targets", "include")}
    for section in ("bundle", "workspace", "resources", "variables", "presets", "permissions", "run_as",
                    "artifacts", "sync"):
        if section in t:
            if section == "variables":  # target-level variables override only the value
                merged.setdefault("variables", {})
                for k, v in t["variables"].items():
                    merged["variables"].setdefault(k, {})
                    merged["variables"][k] = {**merged["variables"][k],
                                              **({"default": v} if not isinstance(v, dict) else v)}
            else:
                merged[section] = _deep_merge(merged.get(section, {}), t[section])
    mode = t.get("mode")
    merged.setdefault("bundle", {})["target"] = target
    if mode:
        merged["bundle"]["mode"] = mode

    values = {k: (v or {}).get("default") for k, v in (merged.get("variables") or {}).items()}
    values.update(variables or {})
    missing = [k for k, v in values.items() if v is None]
    if missing:
        raise ValueError(f"variables without a value for target '{target}': {missing} (pass --var or set them)")

    short = user.split("@")[0]
    ctx = {"bundle.name": merged["bundle"].get("name"), "bundle.target": target,
           "workspace.current_user.userName": user, "workspace.current_user.short_name": short}
    ws_root = (merged.get("workspace") or {}).get("root_path") or \
        f"/Workspace/Users/{user}/.bundle/{merged['bundle'].get('name')}/{target}"

    def resolve(node):
        if isinstance(node, dict):
            return {k: resolve(v) for k, v in node.items()}
        if isinstance(node, list):
            return [resolve(v) for v in node]
        if isinstance(node, str):
            def repl(m):
                ref = m.group(1)
                if ref.startswith("var."):
                    return str(values[ref[4:]])
                if ref in ctx:
                    return str(ctx[ref])
                if ref == "workspace.root_path":
                    return ws_root
                if ref.startswith("workspace.") and ref.endswith("_path"):
                    return f"{ws_root}/{ref.split('.')[1].replace('_path', '')}"
                return m.group(0)  # resource references (${resources.jobs.x.id}) resolve at deploy time
            for _ in range(5):
                new = _REF.sub(repl, node)
                if new == node:
                    break
                node = new
            return node
        return node

    merged = resolve(merged)
    merged["variables"] = {k: {"value": v} for k, v in values.items()}

    # what `mode: development` does to resource names and schedules
    if mode == "development":
        prefix = f"[dev {short}] "
        for kind, items in (merged.get("resources") or {}).items():
            for res in (items or {}).values():
                if isinstance(res, dict) and "name" in res and kind in ("jobs", "pipelines"):
                    res["name"] = prefix + res["name"]
                if kind == "jobs" and isinstance(res, dict):
                    for trig in ("schedule", "trigger", "continuous"):
                        if trig in res:
                            res[trig] = {**res[trig], "pause_status": "PAUSED"}
                if kind == "pipelines" and isinstance(res, dict):
                    res["development"] = True
    if show:
        print(f"# offline preview of target '{target}' (mode: {mode or 'none'})")
        print(yaml.safe_dump(merged, sort_keys=False, width=110))
    return merged


@lru_cache(maxsize=1)
def _schema() -> dict:
    cache = DATA_ROOT / "bundle-schema.json"
    if not cache.exists():
        p = cli("bundle", "schema", echo=False, check=True)
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(p.stdout, encoding="utf-8")
    return _python_regex(json.loads(cache.read_text(encoding="utf-8")))


def _translate_go_regex(p: str) -> str:
    """\\p{L} / \\p{N} -> Python equivalents, inside and outside character classes."""
    out, i, in_class = [], 0, False
    while i < len(p):
        if p.startswith(r"\p{L}", i):
            out.append(r"a-zA-ZÀ-￿" if in_class else r"[^\W\d_]")
            i += 5
        elif p.startswith(r"\p{N}", i):
            out.append("0-9" if in_class else r"\d")
            i += 5
        elif p[i] == "\\" and i + 1 < len(p):
            out.append(p[i:i + 2])
            i += 2
        else:
            in_class = True if p[i] == "[" else False if p[i] == "]" else in_class
            out.append(p[i])
            i += 1
    return "".join(out)


def _python_regex(node):
    """The schema uses Go regexes (\\p{L}, \\p{N}); translate them, drop any Python can't compile."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k == "pattern" and isinstance(v, str):
                v = _translate_go_regex(v)
                try:
                    re.compile(v)
                except re.error:
                    continue
            out[k] = _python_regex(v)
        return out
    if isinstance(node, list):
        return [_python_regex(v) for v in node]
    return node


def validate_schema(bundle_dir: str | os.PathLike) -> list[str]:
    """Validate databricks.yml + includes (and every target) against the CLI's JSON schema, offline."""
    import jsonschema

    cfg = load_bundle(bundle_dir)
    cfg.pop("include", None)

    def leaves(err):
        # oneOf/anyOf report one vague error at the top; descend to the branch that matched best.
        if not err.context:
            return [err]
        branches = {}
        for sub in err.context:
            branches.setdefault(sub.relative_schema_path[0], []).append(sub)
        # Ignore branches of the wrong JSON type (e.g. the "${var...}" string form of an object field),
        # then report the branch with the fewest problems.
        same_type = [b for b in branches.values()
                     if not any(s.validator == "type" and not s.relative_path for s in b)]
        best = min(same_type or list(branches.values()), key=len)
        return [leaf for sub in best for leaf in leaves(sub)]

    validator = jsonschema.Draft7Validator(_schema())
    errors = sorted({f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message[:160]}"
                     for top in validator.iter_errors(cfg) for e in leaves(top)})
    print(f"{len(errors)} schema error(s) in {Path(bundle_dir).name}" + (":" if errors else " - OK"))
    for e in errors[:20]:
        print("  -", e)
    return errors


def tree(path: str | os.PathLike, max_depth: int = 4) -> None:
    root = Path(path)
    skip = {"__pycache__", ".databricks", ".pytest_cache", ".venv", "dist", "build"}

    def walk(p: Path, prefix: str, depth: int):
        entries = sorted([c for c in p.iterdir() if c.name not in skip], key=lambda c: (c.is_file(), c.name))
        for i, c in enumerate(entries):
            last = i == len(entries) - 1
            print(prefix + ("└── " if last else "├── ") + c.name + ("/" if c.is_dir() else ""))
            if c.is_dir() and depth < max_depth:
                walk(c, prefix + ("    " if last else "│   "), depth + 1)
    print(root.name + "/")
    walk(root, "", 1)
