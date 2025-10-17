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
release = "1.1.0"
version = "1.1"

extensions = [
    "myst_parser",
    "autoapi.extension",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinxext.opengraph",
    "sphinx_multiversion",
]

autoapi_dirs = [os.path.join(ROOT, "duckplus")]
autoapi_root = "reference"
autoapi_python_class_content = "both"

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

# ``sphinx-multiversion`` populates ``smv_rename_latest_version`` during CLI
# execution. Local imports (e.g. ``python -m sphinx``) may evaluate the config
# before the variable is set, so normalise it here to avoid ``NameError``
# scenarios during CI builds.
DEFAULT_SMV_RENAME_LATEST = ("main", "latest")
rename_latest = globals().get("smv_rename_latest_version", DEFAULT_SMV_RENAME_LATEST)
if not (isinstance(rename_latest, tuple) and len(rename_latest) == 2):
    rename_latest = DEFAULT_SMV_RENAME_LATEST
smv_rename_latest_version = rename_latest

# The version switcher JSON is served from the ``latest`` build so that all
# historic versions can reference a single canonical copy. Without including the
# ``latest`` prefix the generated pages attempt to fetch ``/_static`` from the
# site root, which does not exist once the docs are published under versioned
# subdirectories (e.g. ``.../duckplus/1.0``).  That empty response meant the
# dropdown rendered with no options even though the JSON was present in each
# build directory.
latest_alias = smv_rename_latest_version[1]

switcher_path = f"{latest_alias}/_static/version_switcher.json"
DEFAULT_SWITCHER_URL = os.environ.get(
    "DUCKPLUS_DOCS_SWITCHER_URL",
    urljoin(html_baseurl, switcher_path),
)

# Theme configuration depends on the switcher configuration above.
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

# Keep todo entries visible for unreleased features while still rendering cleanly.
todo_include_todos = True
