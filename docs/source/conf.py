"""Configuration file for the Sphinx documentation builder."""

project = "R2X"
copyright = "2024, Alliance for Sustainable Energy, LLC"
author = "R2X developers"

# Add more extensions here
extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx_tabs.tabs",
    "sphinx.ext.autosummary",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.coverage",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinxcontrib.mermaid",
    "sphinxcontrib.autodoc_pydantic",
]
# Make sure the target is unique
autosectionlabel_prefix_document = True

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# autosummary_generate = True  # Turn on sphinx.ext.autosummary


# Adding other sphinx documentation as hyperlinks
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "infrasys": ("https://nrel.github.io/infrasys/", None),
}

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
source_suffix = [".md"]

html_theme_options = {
    "repository_url": "https://github.com/NREL/R2X",
    "path_to_docs": "docs/source/",
    "show_toc_level": 3,
    "use_source_button": True,
    "use_edit_page_button": True,
}

myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "attrs_block",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "colon_fence",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# Pydantic stuff
autodoc_pydantic_model_show_config_summary = False

# -- Options for autodoc ----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# Automatically extract typehints when specified and place them in
# descriptions of the relevant function/method.
autodoc_typehints = "description"

# Don't show class signature with the class' name.
# autodoc_class_signature = "separated"
suppress_warnings = ["myst.header"]

# Copy button
copybutton_exclude = ".linenos, .gp, .go"
