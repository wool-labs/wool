# Markdown Style Guide

Key words MUST, MUST NOT, SHOULD, and SHOULD NOT are interpreted as described in RFC 2119. This guide covers Markdown authoring conventions. It is format-specific but project-agnostic — project-level details belong in `CLAUDE.md`.

## 1. Line Wrapping

MUST NOT arbitrarily hard-wrap prose in Markdown files. Each paragraph, list item, or block-level element MUST be a single long line. Only break lines where Markdown syntax requires it (e.g., inside fenced code blocks or tables).

**Rationale:** Hard-wrapping at a fixed column (e.g., 80 characters) creates noisy diffs when text is edited — a single word change can reflow an entire paragraph. Soft-wrapping (one logical line per block element) keeps diffs minimal and makes version-control history easier to review.

### Do

```markdown
This is a paragraph that explains something in detail. It keeps going on a single line regardless of length, because the editor and renderer handle wrapping.

- This list item contains a full sentence of explanation and stays on one line even though it is quite long.
```

### Don't

```markdown
This is a paragraph that explains something
in detail. It keeps going but has been
hard-wrapped at 50 columns for no reason.

- This list item contains a full sentence
  of explanation and has been hard-wrapped
  even though Markdown does not require it.
```
