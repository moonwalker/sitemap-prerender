# Sitemap Prerender

A high-performance tool to prerender entire websites from sitemaps for static hosting and offline use.

## Features

- 🚀 **Fast parallel processing** with automatic concurrency optimization
- 🔄 **Auto-resume** - interrupted runs continue where they left off
- 🌐 **Multi-site support** - process multiple sites without conflicts
- 📦 **Offline-ready** - captures and rewrites assets for standalone use
- 🎯 **Smart filtering** - blocks analytics, ads, and tracking
- 📊 **Real-time progress** - ETA, success rate, and detailed statistics

## Quick Start

```bash
npm i
npm run playwright:install
./sitemap-prerender.ts --sitemap https://example.com/sitemap.xml
```

## Usage

```bash
# Basic usage
./sitemap-prerender.ts --sitemap https://example.com/sitemap.xml --out ./output

# High performance
./sitemap-prerender.ts --sitemap https://example.com/sitemap.xml --concurrency 12

# Block unwanted content
./sitemap-prerender.ts --sitemap https://example.com/sitemap.xml --blockTypes analytics,ads

# Create zip archive
./sitemap-prerender.ts --sitemap https://example.com/sitemap.xml --zip
```

## Key Options

- `--sitemap` - Sitemap URL (required)
- `--out` - Output directory (default: `./static_out`)
- `--concurrency` - Parallel workers (auto-detected)
- `--blockTypes` - Block analytics, ads, beacon
- `--zip` - Create zip archive
- `--offline` - Full offline mirror (default: true)

## Resume & Multi-Site

The tool automatically creates progress files per site. Interrupted runs resume automatically:

```bash
# Process first site
./sitemap-prerender.ts --sitemap https://foo.com/sitemap.xml

# Process second site (independent progress)
./sitemap-prerender.ts --sitemap https://bar.com/sitemap.xml

# Resume first site if interrupted
./sitemap-prerender.ts --sitemap https://foo.com/sitemap.xml
```

Perfect for large sites (100k+ pages) and production workflows.