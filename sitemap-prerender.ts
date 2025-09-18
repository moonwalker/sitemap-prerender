#!/usr/bin/env ts-node

import fs from 'fs';
import path from 'path';
import { promisify } from 'util';
import { chromium, Browser, Route, Request, Response, Page } from 'playwright';
import fetch from 'node-fetch';
import { XMLParser } from 'fast-xml-parser';
import * as url from 'url';
import * as os from 'os';
import archiver from 'archiver';

// Async file operations
const writeFileAsync = promisify(fs.writeFile);
const mkdirAsync = promisify(fs.mkdir);

// -----------------------------
// Progress Management
// -----------------------------
interface ProgressState {
  completed: string[];
  failed: { url: string; error: string; timestamp: number }[];
  assetMappings: Record<string, string>;
  totalProcessed: number;
  startTime: number;
  lastSave: number;
}

class ProgressManager {
  private progressFile: string;
  private assetMappingFile: string;
  private state: ProgressState;
  private saveInterval: any; // Use any instead of NodeJS.Timeout for compatibility

  constructor(outDir: string, siteIdentifier: string) {
    // Create site-specific progress files using a safe identifier
    const safeId = this.createSafeIdentifier(siteIdentifier);
    this.progressFile = path.join(outDir, `.prerender-progress-${safeId}.json`);
    this.assetMappingFile = path.join(outDir, `.asset-mappings-${safeId}.json`);
    this.initializeState(); // Initialize first
    this.loadProgress(); // Then try to load existing
    
    // Auto-save every 30 seconds
    this.saveInterval = setInterval(() => this.saveProgress(), 30000);
  }

  private createSafeIdentifier(siteUrl: string): string {
    // Extract host from URL and make it filesystem-safe
    try {
      const url = new URL(siteUrl);
      return url.host.replace(/[^a-z0-9.-]/gi, '_').toLowerCase();
    } catch {
      // If URL parsing fails, create a safe hash of the input
      return siteUrl.replace(/[^a-z0-9.-]/gi, '_').toLowerCase().substring(0, 50);
    }
  }

  loadProgress(): void {
    console.log(`📂 Using progress file: ${path.basename(this.progressFile)}`);
    if (fs.existsSync(this.progressFile)) {
      try {
        this.state = JSON.parse(fs.readFileSync(this.progressFile, 'utf8'));
        console.log(`🔄 Resuming: ${this.state.totalProcessed} pages already processed for this site`);
        
        // Load asset mappings if they exist
        if (fs.existsSync(this.assetMappingFile)) {
          this.state.assetMappings = JSON.parse(fs.readFileSync(this.assetMappingFile, 'utf8'));
          console.log(`📦 Loaded ${Object.keys(this.state.assetMappings).length} existing asset mappings`);
        }
      } catch (e) {
        console.warn('⚠️  Corrupted progress file, starting fresh');
        this.initializeState();
      }
    } else {
      console.log('🆕 Starting fresh for this site');
      this.initializeState();
    }
  }

  private initializeState(): void {
    this.state = {
      completed: [],
      failed: [],
      assetMappings: {},
      totalProcessed: 0,
      startTime: Date.now(),
      lastSave: Date.now()
    };
  }

  saveProgress(): void {
    this.state.lastSave = Date.now();
    fs.writeFileSync(this.progressFile, JSON.stringify(this.state, null, 2));
    
    // Save asset mappings separately to reduce main progress file size
    if (Object.keys(this.state.assetMappings).length > 0) {
      fs.writeFileSync(this.assetMappingFile, JSON.stringify(this.state.assetMappings, null, 2));
    }
  }

  isCompleted(url: string): boolean {
    return this.state.completed.includes(url);
  }

  markCompleted(url: string): void {
    if (!this.state.completed.includes(url)) {
      this.state.completed.push(url);
      this.state.totalProcessed++;
    }
  }

  markFailed(url: string, error: string): void {
    this.state.failed.push({ url, error, timestamp: Date.now() });
    this.state.totalProcessed++;
  }

  getRemaining(allUrls: { raw: string; mapped: string }[]): { raw: string; mapped: string }[] {
    const completed = new Set(this.state.completed);
    return allUrls.filter(item => !completed.has(item.raw));
  }

  addAssetMapping(url: string, localPath: string): void {
    this.state.assetMappings[url] = localPath;
  }

  getAssetMappings(): Record<string, string> {
    return this.state.assetMappings;
  }

  getStats() {
    const runtime = Date.now() - this.state.startTime;
    const rate = this.state.totalProcessed / (runtime / 1000 / 60); // pages per minute
    return {
      completed: this.state.completed.length,
      failed: this.state.failed.length,
      total: this.state.totalProcessed,
      runtimeMinutes: Math.round(runtime / 1000 / 60),
      pagesPerMinute: Math.round(rate * 100) / 100
    };
  }

  getFailedUrls(limit?: number): { url: string; error: string; timestamp: number }[] {
    return limit ? this.state.failed.slice(-limit) : this.state.failed;
  }

  cleanup(): void {
    clearInterval(this.saveInterval);
    this.saveProgress();
  }

  // Static method to list progress files for different sites in a directory
  static listProgressFiles(outDir: string): void {
    if (!fs.existsSync(outDir)) return;
    
    const files = fs.readdirSync(outDir);
    const progressFiles = files.filter((f: string) => f.startsWith('.prerender-progress-') && f.endsWith('.json'));
    
    if (progressFiles.length > 0) {
      console.log('\n📋 Found progress files for other sites:');
      progressFiles.forEach((file: string) => {
        const site = file.replace('.prerender-progress-', '').replace('.json', '');
        console.log(`   ${site}`);
      });
    }
  }
}

// -----------------------------
// File I/O Queue
// -----------------------------
class FileWriteQueue {
  private queue = new Map<string, Promise<void>>();

  async writeFile(filePath: string, content: string | any): Promise<void> {
    // Wait for any existing write to this file to complete
    if (this.queue.has(filePath)) {
      await this.queue.get(filePath);
    }

    const writePromise = this.performWrite(filePath, content);
    this.queue.set(filePath, writePromise);
    
    try {
      await writePromise;
    } finally {
      this.queue.delete(filePath);
    }
  }

  private async performWrite(filePath: string, content: string | any): Promise<void> {
    await mkdirAsync(path.dirname(filePath), { recursive: true });
    
    // Atomic write using temp file
    const tempPath = filePath + '.tmp';
    await writeFileAsync(tempPath, content);
    await fs.promises.rename(tempPath, filePath);
  }
}

// -----------------------------
// Retry Logic
// -----------------------------
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelayMs: number = 1000
): Promise<T> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      if (attempt === maxRetries) break;
      
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 1000; // Add jitter
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw lastError || new Error('Maximum retries exceeded');
}

// -----------------------------
// Performance Utilities  
// -----------------------------
function getOptimalConcurrency(userConcurrency?: number): number {
  if (userConcurrency) return userConcurrency;
  
  const cpuCount = os.cpus().length;
  const memoryGB = os.totalmem() / (1024 * 1024 * 1024);
  
  // Conservative: 1.5 workers per CPU core, limited by memory (2GB per worker)
  const cpuBasedLimit = Math.max(1, Math.floor(cpuCount * 1.5));
  const memoryBasedLimit = Math.max(1, Math.floor(memoryGB / 2));
  
  return Math.min(cpuBasedLimit, memoryBasedLimit, 12); // Cap at 12
}

// -----------------------------
// CLI parsing
// -----------------------------
function parseArgs() {
  const args = process.argv.slice(2);
  const out: Record<string, string | boolean | number> = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (!a.startsWith('--')) continue;
    const key = a.replace(/^--/, '');
    const next = args[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
    } else {
      out[key] = next;
      i++;
    }
  }
  return out as {
    sitemap?: string;
    out?: string;
    concurrency?: string | number;
    wait?: string | number;
    headers?: string;
    zip?: string | boolean;
    userAgent?: string;
    timeout?: string | number;
    includeExternal?: string | boolean;
    offline?: string | boolean; // FULL OFFLINE MIRROR (DEFAULT: true)
    noOffline?: string | boolean; // override to disable offline
    maxAssetMB?: string | number; // skip assets larger than this size
    blockTypes?: string; // comma list: analytics,ads,beacon
    recordApi?: string | boolean; // capture JSON/API and inject fetch replay (DEFAULT: true when offline)
  };
}

function boolArg(v: any, dflt: boolean) {
  if (v === undefined) return dflt;
  if (typeof v === 'boolean') return v;
  const s = String(v).toLowerCase();
  return !(s === '0' || s === 'false' || s === 'no' || s === 'off');
}

function numArg(v: any, dflt: number) {
  if (v === undefined) return dflt;
  const n = Number(v);
  return Number.isFinite(n) ? n : dflt;
}

function parseHeaderArg(arg?: string): Record<string,string> {
  const res: Record<string, string> = {};
  if (!arg) return res;
  arg.split(',').forEach(p => {
    const [k, ...rest] = p.split('=');
    const key = k?.trim();
    const val = rest.join('=').trim();
    if (key && val) res[key] = val;
  });
  return res;
}

// -----------------------------
// Utilities
// -----------------------------
async function fetchText(u: string): Promise<string> {
  const r = await fetch(u, { headers: { 'User-Agent': 'sitemap-prerender/1.2' }});
  if (!r.ok) throw new Error(`HTTP ${r.status} on ${u}`);
  return await r.text();
}

const EXT_FROM_CT: Record<string, string> = {
  'text/css': '.css',
  'text/javascript': '.js',
  'application/javascript': '.js',
  'application/x-javascript': '.js',
  'application/json': '.json',
  'application/graphql-response+json': '.json',
  'text/plain': '.txt',
  'image/png': '.png',
  'image/jpeg': '.jpg',
  'image/jpg': '.jpg',
  'image/gif': '.gif',
  'image/webp': '.webp',
  'image/svg+xml': '.svg',
  'font/woff': '.woff',
  'font/woff2': '.woff2',
  'application/font-woff2': '.woff2',
  'application/font-woff': '.woff',
  'text/html': '.html'
};

function ensureDirFor(p: string) { fs.mkdirSync(path.dirname(p), { recursive: true }); }
function sanitizeFileName(name: string) { return name.replace(/[#?*:<>"|]/g, '_'); }

function mapUrlToPath(baseHost: string, raw: string, outDir: string, includeExternal = false): string | null {
  const u = new url.URL(raw);
  if (!includeExternal && u.host !== baseHost) return null;
  let p = u.pathname;
  if (!p || p.endsWith('/')) p = path.posix.join(p, 'index.html');
  const ext = path.posix.extname(p);
  if (!ext) p = p + '.html';
  const filePath = path.join(outDir, sanitizeFileName(u.host), sanitizeFileName(p));
  return path.normalize(filePath);
}

function mapAssetUrlToPath(raw: string, outDir: string): string {
  const u = new url.URL(raw);
  let p = sanitizeFileName(u.pathname);
  if (p.endsWith('/')) p = path.posix.join(p, 'index');
  const filePath = path.join(outDir, sanitizeFileName(u.host), p);
  return path.normalize(filePath);
}

async function zipDir(srcDir: string, zipPath: string) {
  await new Promise<void>((resolve, reject) => {
    const out = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    out.on('close', resolve);
    archive.on('error', reject);
    archive.pipe(out);
    archive.directory(srcDir, false);
    archive.finalize();
  });
}

// -----------------------------
// Sitemap loading
// -----------------------------
function isSitemapIndex(obj: any): boolean { return obj && obj.sitemapindex; }
function isUrlset(obj: any): boolean { return obj && obj.urlset; }

async function collectUrls(sitemapURL: string, visited = new Set<string>()): Promise<string[]> {
  if (visited.has(sitemapURL)) return [];
  visited.add(sitemapURL);
  const xml = await fetchText(sitemapURL);
  const parser = new XMLParser({ ignoreAttributes: false, preserveOrder: false });
  const obj = parser.parse(xml);
  const out: string[] = [];
  if (isSitemapIndex(obj)) {
    const list = obj.sitemapindex.sitemap ?? [];
    const items = Array.isArray(list) ? list : [list];
    for (const it of items) {
      const loc = (it.loc ?? '').trim();
      if (!loc) continue;
      out.push(...await collectUrls(loc, visited));
    }
  } else if (isUrlset(obj)) {
    const list = obj.urlset.url ?? [];
    const items = Array.isArray(list) ? list : [list];
    for (const it of items) {
      const loc = (it.loc ?? '').trim();
      if (!loc) continue;
      out.push(loc);
    }
  } else {
    throw new Error('Unknown sitemap XML (expected <sitemapindex> or <urlset>)');
  }
  return out;
}

// -----------------------------
// Rewriting helpers
// -----------------------------
function rewriteHtml(html: string, urlMapAbsToRel: Map<string,string>, htmlOutPath: string, injectReplay: boolean, pageMap: Record<string,string>): string {
  const dir = path.dirname(htmlOutPath);
  const replaceAttr = (m: string, p1: string, p2: string) => {
    const orig = p2.trim();
    const mappedAbs = pageMap[orig] || urlMapAbsToRel.get(orig);
    if (!mappedAbs) return m;
    const rel = path.relative(dir, mappedAbs).split(path.sep).join('/');
    return `${p1}\"${rel}\"`;
  };
  html = html.replace(/(\s(?:src|href|poster|data-src|data-href)=["'])(https?:\/\/[^"'>\s]+)["']/gi, replaceAttr);
  html = html.replace(/(\ssrcset=["'])([^"']+)["']/gi, (_m, p1, p2) => {
    const parts = p2.split(',').map((s: string) => s.trim()).filter(Boolean).map((token: string) => {
      const [u, desc] = token.split(/\s+/, 2);
      const mappedAbs = pageMap[u] || urlMapAbsToRel.get(u);
      if (!mappedAbs) return token;
      const rel = path.relative(dir, mappedAbs).split(path.sep).join('/');
      return desc ? `${rel} ${desc}` : rel;
    });
    return `${p1}${parts.join(', ')}"`;
  });

  if (injectReplay) {
    const mappingJson = JSON.stringify(pageMap);
    const encoded = encodeURIComponent(mappingJson);
    const shim = `\n<script>(function(){try{const M=JSON.parse(decodeURIComponent('${encoded}'));const OF=window.fetch;window.fetch=async function(i,init){try{const u=typeof i==='string'?i:(i&&i.url)||'';if(M[u]){return OF(M[u],init);} }catch(_){}return OF(i,init)};}catch(_){}})();</script>\n`;
    // inject before </head> or at start of <body>
    if (/<\/head>/i.test(html)) html = html.replace(/<\/head>/i, shim + '</head>');
    else if (/<body[^>]*>/i.test(html)) html = html.replace(/<body[^>]*>/i, (m)=> m+shim);
    else html = shim + html;
  }
  return html;
}

function rewriteCss(css: string, urlMapAbsToRel: Map<string,string>, cssOutPath: string): string {
  const dir = path.dirname(cssOutPath);
  return css.replace(/url\(([^)]+)\)/gi, (m, p1) => {
    const raw = p1.trim().replace(/^['"]|['"]$/g, '');
    if (!/^https?:\/\//i.test(raw)) return m;
    const mappedAbs = urlMapAbsToRel.get(raw);
    if (!mappedAbs) return m;
    const rel = path.relative(dir, mappedAbs).split(path.sep).join('/');
    return `url(${rel})`;
  });
}

// -----------------------------
// Main
// -----------------------------
async function main() {
  const args = parseArgs();
  const sitemapURL = args.sitemap as string;
  if (!sitemapURL) { console.error('--sitemap is required'); process.exit(1); }

  const outDir = (args.out as string) || './static_out';
  const userConcurrency = args.concurrency ? numArg(args.concurrency, 0) : undefined;
  const concurrency = getOptimalConcurrency(userConcurrency);
  const settleMs = numArg(args.wait, 1000);
  const perPageTimeout = numArg(args.timeout, 60000);
  const headers = parseHeaderArg(args.headers);
  const includeExternal = boolArg(args.includeExternal, true);
  const offline = args.noOffline ? false : boolArg(args.offline, true);
  const recordApi = boolArg(args.recordApi, offline);
  const maxAssetMB = numArg(args.maxAssetMB, 25);
  const blockTypes = String(args.blockTypes || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  const wantZip = boolArg(args.zip, false);
  const ua = (args.userAgent as string) || `sitemap-prerender/1.2 (+${os.platform()} ${os.release()})`;

  const base = new URL(sitemapURL);

  // Initialize progress manager and file queue with site-specific identifiers
  ensureDirFor(path.join(outDir, 'dummy')); // Ensure output dir exists
  const progressManager = new ProgressManager(outDir, sitemapURL);
  const fileQueue = new FileWriteQueue();

  console.log('📊 Collecting URLs from sitemap…');
  const urls = await collectUrls(sitemapURL);
  const filtered = urls
    .map(u => ({ raw: u, mapped: mapUrlToPath(base.host, u, outDir, includeExternal) }))
    .filter(x => x.mapped !== null) as { raw: string; mapped: string }[];

  if (!filtered.length) { console.error('No URLs to render after filtering.'); process.exit(2); }

  // Get remaining URLs (skip completed ones)
  const remaining = progressManager.getRemaining(filtered);
  const stats = progressManager.getStats();
  
  // Show existing progress files for other sites
  ProgressManager.listProgressFiles(outDir);
  
  if (remaining.length === 0) {
    console.log('✅ All URLs already processed!');
    if (stats.failed > 0) {
      console.log(`❌ ${stats.failed} URLs had failed previously. Use fresh output directory to retry.`);
    }
    progressManager.cleanup();
    return;
  }

  console.log(`📈 Processing ${remaining.length} URLs (${stats.completed} already done)`);
  console.log(`⚙️  Using ${concurrency} concurrent workers`);

  const browser: Browser = await chromium.launch({ 
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage'] // Better memory usage
  });

  let processedCount = stats.total;
  const totalToProcess = remaining.length + stats.total;
  const startTime = Date.now();
  
  // Asset mappings - start with existing ones and add memory limit
  const existingAssetMappings = progressManager.getAssetMappings();
  const urlToLocalAbsPath = new Map<string, string>(Object.entries(existingAssetMappings));
  const savedAssets = new Set<string>(Object.values(existingAssetMappings));
  
  // Memory management for asset mappings
  const ASSET_FLUSH_INTERVAL = 1000;
  let assetFlushCounter = 0;

  function flushAssetMappings() {
    console.log('💾 Flushing asset mappings to reduce memory usage...');
    for (const [url, localPath] of urlToLocalAbsPath.entries()) {
      progressManager.addAssetMapping(url, localPath);
    }
    progressManager.saveProgress();
    urlToLocalAbsPath.clear();
    assetFlushCounter = 0;
  }

  function shouldBlock(req: Request): boolean {
    const rt = req.resourceType();
    if (blockTypes.includes('analytics')) {
      if (/googletagmanager|google-analytics|gtag\/js|clarity|segment|fullstory|hotjar/i.test(req.url())) return true;
    }
    if (blockTypes.includes('ads')) {
      if (/doubleclick|adservice|adnxs|googlesyndication/i.test(req.url())) return true;
    }
    if (blockTypes.includes('beacon') && (rt === 'beacon' || rt === 'xhr')) return true;
    return false;
  }

  async function captureResponse(resp: Response) {
    try {
      const req = resp.request();
      const method = req.method();
      if (method !== 'GET') return;
      const status = resp.status();
      if (status < 200 || status >= 300) return;

      const u = req.url();
      const headers = resp.headers();
      const ct = (headers['content-type'] || '').split(';')[0].trim().toLowerCase();
      if (/text\/html/i.test(ct)) return; // HTML pages saved separately

      // Size guard
      const lenStr = headers['content-length'];
      const maxBytes = maxAssetMB * 1024 * 1024;
      if (lenStr) {
        const len = parseInt(lenStr, 10);
        if (Number.isFinite(len) && len > maxBytes) return;
      }

      const buf = await resp.body();
      if (buf.length > maxBytes) return;

      const basePathNoExt = mapAssetUrlToPath(u, outDir);
      let outPathFile = basePathNoExt;
      let ext = path.extname(basePathNoExt);
      if (!ext) {
        ext = EXT_FROM_CT[ct] || '';
        outPathFile = basePathNoExt + ext;
      }

      if (!savedAssets.has(outPathFile)) {
        savedAssets.add(outPathFile);
        
        if (ct.startsWith('text/css')) {
          let css = buf.toString('utf8');
          // Use current + existing mappings for CSS rewriting
          const allMappings = new Map([...Object.entries(progressManager.getAssetMappings()), ...urlToLocalAbsPath.entries()]);
          css = rewriteCss(css, allMappings, outPathFile);
          await fileQueue.writeFile(outPathFile, css);
        } else {
          await fileQueue.writeFile(outPathFile, buf);
        }
      }
      
      urlToLocalAbsPath.set(u, outPathFile);
      
      // Periodic asset mapping flush to manage memory
      assetFlushCounter++;
      if (assetFlushCounter >= ASSET_FLUSH_INTERVAL) {
        flushAssetMappings();
      }
    } catch {
      // ignore asset capture errors
    }
  }

  async function worker(items: { raw: string; mapped: string }[], workerId: number) {
    const context = await browser.newContext({ 
      userAgent: ua, 
      extraHTTPHeaders: headers,
      viewport: { width: 1280, height: 720 }, // Smaller viewport for memory
      deviceScaleFactor: 1
    });

    if (offline) {
      await context.route('**/*', async (route: Route, req: Request) => {
        if (shouldBlock(req)) { await route.abort(); return; }
        await route.continue();
      });
    }

    // Create a pool of pages for this worker
    const PAGE_POOL_SIZE = Math.min(2, Math.max(1, Math.floor(items.length / 50)));
    const pages: Page[] = [];
    
    for (let i = 0; i < PAGE_POOL_SIZE; i++) {
      const page = await context.newPage();
      if (offline) page.on('response', (r) => captureResponse(r));
      pages.push(page);
    }

    let currentPageIndex = 0;

    try {
      for (const item of items) {
        const page = pages[currentPageIndex];
        currentPageIndex = (currentPageIndex + 1) % pages.length;
        
        processedCount++;
        const progress = Math.round((processedCount / totalToProcess) * 100);
        const elapsed = Math.round((Date.now() - startTime) / 1000 / 60);
        const rate = processedCount / elapsed || 0;
        const eta = remaining.length > 0 ? Math.round((totalToProcess - processedCount) / rate) : 0;
        
        console.log(`[${processedCount}/${totalToProcess}] ${progress}% (${elapsed}m elapsed, ~${eta}m ETA) → ${item.raw}`);
        
        try {
          await retryWithBackoff(async () => {
            await page.goto(item.raw, { waitUntil: 'networkidle', timeout: perPageTimeout });
            if (settleMs > 0) await page.waitForTimeout(settleMs);

            // Build per-page API mapping for fetch replay
            const pageMap: Record<string, string> = {};
            if (recordApi) {
              // Use both existing and current mappings
              const allMappings = { ...progressManager.getAssetMappings(), ...Object.fromEntries(urlToLocalAbsPath.entries()) };
              for (const [absUrl, absPath] of Object.entries(allMappings)) {
                if (/\.(json|txt)(\?|$)/i.test(absUrl) || /application\/(json|graphql-response\+json)/i.test(EXT_FROM_CT[path.extname(absPath).replace('.', '')] || '')) {
                  pageMap[absUrl] = absPath;
                }
              }
            }

            let html = await page.content();
            if (offline) {
              const allMappings = new Map([...Object.entries(progressManager.getAssetMappings()), ...urlToLocalAbsPath.entries()]);
              html = rewriteHtml(html, allMappings, item.mapped, recordApi, pageMap);
            }
            
            await fileQueue.writeFile(item.mapped, html);
          }, 3, 2000);

          progressManager.markCompleted(item.raw);
        } catch (e: any) {
          const errorMsg = e?.message || String(e);
          console.warn(`❌ Failed ${item.raw}: ${errorMsg}`);
          progressManager.markFailed(item.raw, errorMsg);
        }
      }
    } finally {
      // Close all pages
      await Promise.all(pages.map(page => page.close().catch(() => {})));
      await context.close();
    }
  }

  // Distribute work across workers
  const buckets: { raw: string; mapped: string }[][] = Array.from({ length: Math.min(concurrency, remaining.length) }, () => []);
  remaining.forEach((item, idx) => buckets[idx % buckets.length].push(item));

  console.log('🚀 Starting parallel processing...');
  await Promise.all(buckets.map((bucket, workerId) => worker(bucket, workerId)));
  
  // Final flush of any remaining asset mappings
  if (urlToLocalAbsPath.size > 0) {
    flushAssetMappings();
  }
  
  await browser.close();

  // Final statistics
  const finalStats = progressManager.getStats();
  console.log('\n📊 Final Statistics:');
  console.log(`✅ Completed: ${finalStats.completed} pages`);
  console.log(`❌ Failed: ${finalStats.failed} pages`);
  console.log(`⏱️  Total runtime: ${finalStats.runtimeMinutes} minutes`);
  console.log(`📈 Average rate: ${finalStats.pagesPerMinute} pages/minute`);

  if (finalStats.failed > 0) {
    console.log('\n❌ Failed URLs (see .prerender-progress.json for details):');
    const recentFails = progressManager.getFailedUrls(5);
    recentFails.forEach(fail => console.log(`   ${fail.url} - ${fail.error}`));
    const totalFailed = progressManager.getFailedUrls().length;
    if (totalFailed > 5) {
      console.log(`   ... and ${totalFailed - 5} more`);
    }
  }

  if (wantZip) {
    const zipPath = path.resolve(outDir + '.zip');
    console.log('📦 Creating zip archive...', zipPath);
    await zipDir(outDir, zipPath);
    console.log('✅ Zip created successfully');
  }

  progressManager.cleanup();
  console.log(`\n🎉 Done! Successfully processed ${finalStats.completed} pages.`);
}

main().catch(e => { console.error(e); process.exit(1); });
