"""Sphinx configuration for Duck+ documentation."""
from __future__ import annotations

import os
import sys
from datetime import datetime
from importlib import metadata

# -- Path setup --------------------------------------------------------------

ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
SRC_DIR = os.path.join(ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# -- Project information -----------------------------------------------------

project = "Duck+"
author = "Isaac Moore"
copyright = f"{datetime.now():%Y}, {author}"

try:
    release = metadata.version("duckplus")
except metadata.PackageNotFoundError:
    from pathlib import Path

    pyproject_toml = Path(ROOT) / "pyproject.toml"
    release = "0.0.0"
    if pyproject_toml.exists():
        for line in pyproject_toml.read_text().splitlines():
            if line.startswith("version"):
                release = line.split("=", maxsplit=1)[1].strip().strip('"')
                break
version = release

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
]

autosummary_generate = False
autodoc_default_options = {
    "member-order": "bysource",
    "members": True,
    "undoc-members": False,
    "special-members": "__call__",
    "show-inheritance": True,
}
autodoc_typehints = "description"

napoleon_google_docstring = True
napoleon_numpy_docstring = False

suppress_warnings = ["intersphinx"]

intersphinx_mapping: dict[str, tuple[str, str | None]] = {}

templates_path = ["_templates"]
exclude_patterns: list[str] = ["_build"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# -- Options for HTML output -------------------------------------------------

html_theme = "furo"
html_static_path = ["_static"]
html_title = "Duck+ Documentation"
