# R2X documentation

This directory contains the Astro and Starlight documentation site for R2X.

## Develop the site

```bash
npm ci
npm run dev
```

Build the static site with the same command used by CI:

```bash
npm run build
```

The site source lives under `src/content/docs/`. Content is organized into
Starlight's tutorial, how-to, explanation, and reference sections. The
`src/content/docs/2-0-0/` tree is the archived v2.0.0 documentation and
`src/content/versions/2-0-0.json` defines its sidebar.

Keep the current documentation and archived version synchronized when a change
applies to both. See the [development guide](src/content/docs/how-to/development.mdx)
for translation maintenance guidance and documentation authoring rules.
