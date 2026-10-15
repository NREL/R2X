# R2X documentation

The Astro and Starlight site organizes the current documentation into tutorials,
how-to guides, reference, and explanation. Search indexes each version separately.

## Build and preview

Use Node.js 22.12 or newer and npm. From the repository root, install the locked
site dependencies and build the pages:

```bash
cd docs && npm ci && npm run build
```

The build writes `docs/dist/`. From `docs/`, start the local preview:

```bash
npm run preview
```

Open the printed URL under `/R2X/`. Use `npm run dev` for live editing.

## Content and versions

- Current pages: `src/content/docs/`, grouped by reader task.
- Release archives: `src/content/docs/2-2-0/` and `src/content/docs/2-0-0/`,
  migrated from the actual `r2x-v2.2.0` and `r2x-v2.0.0` tags.
- Version sidebars: `src/content/versions/`.
- Current navigation and plugins: `astro.config.mjs`.

Use `/R2X/`-prefixed page links so they work on GitHub Pages. The version plugin
adds the archive prefix for historical content. Link to source at the verified current code commit for
current docs and the matching release tag for archives. Keep release content
frozen, apart from migration fixes such as links and rendering.

See [development](src/content/docs/how-to/development.mdx) for repository checks.

## Dependency overrides

The lockfile selects patched `postcss-nested` 7.0.2 for Expressive Code and
KaTeX 0.18.2 for Mermaid. These overrides remove transitive security advisories;
the Node.js 22 build and rendered CSS and diagrams have been checked. Revisit
them when upstream dependencies include the patched versions.
