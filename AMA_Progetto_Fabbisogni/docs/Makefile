# Minimal makefile for Sphinx documentation
#
SPHINXAPI     ?= sphinx-apidoc
SPHINXAOPTS   ?= --force --separate --private --module-first
SPHINXBUILD   ?= sphinx-build
SPHINXBOPTS   ?=
SOURCEDIR     = source
BUILDDIR      = build
DOCSDIR       = docs

MODULES       = src

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXBOPTS)

.PHONY: help Makefile

all: build html
	@echo

cleanall: clean
	@for MOD in $(MODULES); do \
		echo "Removing everything under 'source/$$MOD'..." ; \
		rm -rf "$(SOURCEDIR)/$$MOD" ; \
		if [ "$$MOD" = "mininet" ] || [ "$$MOD" = "containernet" ]; then \
			cd .. ; \
			rm -f $$MOD ; \
			cd - > /dev/null ; \
		fi ; \
	done

build: cleanall
	@for MOD in $(MODULES); do \
		echo ; \
		echo "Build $$MOD..." ; \
		$(SPHINXAPI) $(SPHINXAOPTS) -o "$(SOURCEDIR)/$$MOD/" "../$$MOD/" ; \
	done

github: build html
	@echo
	@echo "Copy $@..."
	@cp -a "$(BUILDDIR)/html/." "../$(DOCSDIR)"

wiki: build markdown
	@echo
	@echo "Copy $@..."
	@cp -a "$(BUILDDIR)/markdown/." "../$(DOCSDIR)"

# Catch-all target: route all unknown targets to Sphinx using the new "make mode" option.
%: Makefile
	@echo
	@echo "Make $@..."
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXBOPTS)
