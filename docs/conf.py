"""Sphinx configuration for the DuckPlus documentation site."""
from __future__ import annotations

import datetime as _dt
import os
import sys
from typing import Any, Dict
from urllib.parse import urljoin

# Ensure the package can be imported for autodoc examples.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

project = "DuckPlus"
author = "DuckPlus maintainers"
current_year = _dt.date.today().year
copyright = f"{current_year}, {author}"
release = "1.0.0"
version = "1.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinxext.opengraph",
    "sphinx_multiversion",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution",
]

intersphinx_mapping: Dict[str, tuple[str, str | None]] = {}

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "community_extension_targets.md",
    "extensions_audit.md",
    "io.md",
    "language_server_demo.md",
    "pi_demo.md",
    "relation.md",
    "schema.md",
    "typed_api.md",
]
autosectionlabel_prefix_document = True

def setup(app: Any) -> None:  # pragma: no cover - Sphinx hook
    """Register substitutions that keep version references centralised."""
    app.add_config_value("duckplus_version", version, "env")

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["theme_overrides.css"]

DEFAULT_REPOSITORY = "isaacnfairplay/duckplus"
repository = os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPOSITORY)
try:
    owner, repo_name = repository.split("/", 1)
except ValueError:
    owner, repo_name = DEFAULT_REPOSITORY.split("/", 1)

DEFAULT_HTML_BASEURL = f"https://{owner}.github.io/{repo_name}/"
html_baseurl = os.environ.get("DUCKPLUS_DOCS_BASEURL", DEFAULT_HTML_BASEURL)

switcher_path = "_static/version_switcher.json"
DEFAULT_SWITCHER_URL = os.environ.get(
    "DUCKPLUS_DOCS_SWITCHER_URL",
    urljoin(html_baseurl, switcher_path),
)

html_theme_options: Dict[str, Any] = {
    "logo": {
        "text": "DuckPlus",
    },
    "show_nav_level": 2,
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["version-switcher", "theme-switcher"],
    "switcher": {
        "json_url": DEFAULT_SWITCHER_URL,
        "version_match": version,
    },
    "use_edit_page_button": False,
    "primary_sidebar_end": ["indices.html", "searchbox.html"],
}

html_context = {
    "default_mode": "light",
}

smv_tag_whitelist = r"^v\d+\.\d+\.\d+$"
smv_branch_whitelist = r".*"
smv_remote_whitelist = r"^origin$"
smv_latest_version = "1.0"
smv_rename_latest_version = ("1.0", "latest")
smv_released_pattern = r"^tags/v\d+\.\d+\.\d+$"
smv_outputdir = "_build/html"

# Keep todo entries visible for unreleased features while still rendering cleanly.
todo_include_todos = True
