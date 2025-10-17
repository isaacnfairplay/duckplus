# Minimal makefile for Sphinx documentation
SPHINXBUILD ?= sphinx-build
SOURCEDIR ?= .
BUILDDIR ?= _build

.PHONY: html clean linkcheck

html:
	@sphinx-multiversion "$(SOURCEDIR)" "$(BUILDDIR)/html"

clean:
	@rm -rf "$(BUILDDIR)"

linkcheck:
	@"$(SPHINXBUILD)" -b linkcheck "$(SOURCEDIR)" "$(BUILDDIR)/linkcheck"
