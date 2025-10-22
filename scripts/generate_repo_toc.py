"""Generate a repository table of contents with links to hosted files.

This script walks the repository tree and produces a Markdown document that
links to every file using `raw.githubusercontent.com` URLs so the content can be
fetched directly. Directory entries link to the GitHub tree view. The intent is
to make it easy for downstream tooling (for example, ChatGPT) to open any file
in the project without having to manually browse the repository.

Usage
-----
```
python scripts/generate_repo_toc.py --output docs/repository-map.md
```

The script automatically detects the repository slug (``owner/name``) and the
current commit SHA when executed inside a Git checkout. The values can be
overridden explicitly or supplied by the GitHub Actions environment variables
(``GITHUB_REPOSITORY`` and ``GITHUB_SHA``).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional


EXCLUDED_DIR_NAMES = {
    ".git",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
}

EXCLUDED_PATHS = {
    Path("docs/_build"),
}

EXCLUDED_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".DS_Store",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root of the repository to index (defaults to the current working directory).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to the Markdown file that will be written.",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=None,
        help="GitHub repository in 'owner/name' form (auto-detected when omitted).",
    )
    parser.add_argument(
        "--ref",
        type=str,
        default=None,
        help="Commit SHA or branch name that the links should target (auto-detected when omitted).",
    )
    parser.add_argument(
        "--tree-ref",
        type=str,
        default=None,
        help="Git reference to use for directory tree links (defaults to --ref).",
    )
    return parser.parse_args()


def detect_repo_slug(root: Path) -> Optional[str]:
    repo = os.environ.get("GITHUB_REPOSITORY")
    if repo:
        return repo.strip()

    try:
        result = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        url = None
    else:
        url = result.stdout.strip()

    if url:
        if url.endswith(".git"):
            url = url[:-4]

        if url.startswith("git@github.com:"):
            return url.split(":", 1)[1]
        if url.startswith("https://github.com/"):
            return url.split("github.com/", 1)[1]

    # Fall back to pyproject metadata when available.
    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        with pyproject.open("rb") as fh:
            try:
                data = tomllib.load(fh)
            except tomllib.TOMLDecodeError:
                data = {}
        urls = data.get("project", {}).get("urls", {})
        repo_url = urls.get("Repository") or urls.get("repository")
        if isinstance(repo_url, str) and "github.com" in repo_url:
            repo_url = repo_url.rstrip("/")
            if repo_url.endswith(".git"):
                repo_url = repo_url[:-4]
            parts = repo_url.split("github.com/", 1)
            if len(parts) == 2:
                return parts[1]

    return None


def detect_ref(root: Path) -> Optional[str]:
    ref = os.environ.get("GITHUB_SHA") or os.environ.get("GITHUB_REF_NAME")
    if ref:
        return ref.strip()

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

    return result.stdout.strip() or None


def should_exclude(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    if any(part in EXCLUDED_DIR_NAMES for part in relative.parts):
        return True
    for excluded in EXCLUDED_PATHS:
        if relative == excluded:
            return True
        try:
            relative.relative_to(excluded)
        except ValueError:
            pass
        else:
            return True
    if path.is_dir():
        return False
    if path.suffix in EXCLUDED_SUFFIXES:
        return True
    return False


@dataclass
class Node:
    children: Dict[str, "Node"]
    is_dir: bool

    @classmethod
    def directory(cls) -> "Node":
        return cls(children={}, is_dir=True)

    @classmethod
    def file(cls) -> "Node":
        return cls(children={}, is_dir=False)


def build_tree(root: Path) -> Node:
    root_node = Node.directory()
    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath)
        rel_dir = current_dir.relative_to(root)

        # Filter directories in-place so os.walk will not traverse them.
        dirnames[:] = [d for d in dirnames if not should_exclude(current_dir / d, root)]

        if rel_dir != Path('.'):
            parts = rel_dir.parts
            current = root_node
            for part in parts:
                current = current.children.setdefault(part, Node.directory())

        current_node = root_node
        if rel_dir != Path('.'):
            for part in rel_dir.parts:
                current_node = current_node.children[part]

        for filename in sorted(filenames):
            file_path = current_dir / filename
            if should_exclude(file_path, root):
                continue
            current_node.children.setdefault(filename, Node.file())
    return root_node


def iter_rendered_lines(
    node: Node,
    root: Path,
    repo_slug: str,
    raw_base: str,
    tree_base: str,
    prefix: Path = Path(""),
    depth: int = 0,
) -> Iterator[str]:
    indent = "  " * depth
    for name in sorted(node.children):
        child = node.children[name]
        child_path = prefix / name
        posix_path = child_path.as_posix()
        if child.is_dir:
            line = f"{indent}- **[{name}/]({tree_base}/{posix_path})**"
            yield line
            yield from iter_rendered_lines(child, root, repo_slug, raw_base, tree_base, child_path, depth + 1)
        else:
            yield f"{indent}- [{name}]({raw_base}/{posix_path})"


def generate_markdown(root: Path, repo_slug: str, ref: str, tree_ref: str) -> List[str]:
    raw_base = f"https://raw.githubusercontent.com/{repo_slug}/{ref}"
    tree_base = f"https://github.com/{repo_slug}/tree/{tree_ref}"
    header = [
        "---",
        "title: Repository Table of Contents",
        "---",
        "",
        "# Repository Table of Contents",
        "",
        "This index is automatically generated. Each entry links directly to the",
        "raw file contents so tooling can retrieve any resource without manual",
        "navigation.",
        "",
        f"- **Repository:** [{repo_slug}](https://github.com/{repo_slug})",
        f"- **Source reference:** `{ref}`",
        "",
    ]
    tree = build_tree(root)
    lines = list(iter_rendered_lines(tree, root, repo_slug, raw_base, tree_base))
    return header + lines + [""]


def main() -> None:
    args = parse_args()
    root = args.root.resolve()

    if not root.exists():
        raise SystemExit(f"Repository root '{root}' does not exist")

    repo_slug = args.repo or detect_repo_slug(root)
    if not repo_slug:
        raise SystemExit("Unable to determine repository slug. Use --repo to specify it explicitly.")

    ref = args.ref or detect_ref(root) or "main"
    tree_ref = args.tree_ref or ref

    markdown_lines = generate_markdown(root, repo_slug, ref, tree_ref)

    output_path = args.output if args.output.is_absolute() else args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(markdown_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
