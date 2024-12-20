# Copyright (C) 2020 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# tooz documentation build configuration file
#
# This file is execfile()d with the current directory set to
# its containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

import datetime

# Add any Sphinx extension module names here, as strings.
# They can be extensions coming with Sphinx (named 'sphinx.ext.*') or your
# custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.graphviz',
    'sphinx.ext.extlinks',
    'openstackdocstheme',
    'sphinx.ext.inheritance_diagram',
    'sphinx.ext.viewcode',
    'stevedore.sphinxext',
]

# openstackdocstheme options
openstackdocs_repo_name = 'openstack/tooz'
openstackdocs_auto_name = False
openstackdocs_bug_project = 'tooz'
openstackdocs_bug_tag = ''

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'tooz'
copyright = '%s, OpenStack Foundation' % datetime.date.today().year

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'native'

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'openstackdocs'

# Output file base name for HTML help builder.
htmlhelp_basename = 'toozdoc'

latex_elements = {}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author,
# documentclass [howto/manual]).
latex_documents = [
  ('index', 'tooz.tex', 'tooz Documentation',
   'eNovance', 'manual'),
]

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
  ('index', 'tooz', 'tooz Documentation',
   'OpenStack Foundation', 'tooz', 'One line description of project.',
   'Miscellaneous'),
]

autodoc_default_options = {
    'members': None,
    'special-members': None,
    'show_inheritance': None,
    }
