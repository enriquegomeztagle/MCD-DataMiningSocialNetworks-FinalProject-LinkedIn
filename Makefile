.PHONY: clean-temp format-docs all
all:
	@echo "Available targets:"
	@echo "  clean-temp            - Remove temporary files"
	@echo "  format-docs           - Format the documentation"

# ────────────────────────────────────────────────────────────────────────────────
clean-temp:
	find . -type f -name "._*" -delete
	rm -f .git/objects/pack/._pack-*.idx

format-docs:
	latexindent -o doc/doc.tex doc/doc.tex
