"""Sphinx configuration for the standalone Hedloom documentation."""

import os

project = "Hedloom"
author = "smldis"
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]
html_theme = "furo"
source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
exclude_patterns = ["**/examples/**", "**/requirements.txt"]
autodoc_member_order = "bysource"
autodoc_typehints = "description"
myst_heading_anchors = 3
html_baseurl = os.environ.get("READTHEDOCS_CANONICAL_URL", "")
html_context = {"READTHEDOCS": os.environ.get("READTHEDOCS") == "True"}
