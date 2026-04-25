const express = require('express');
const puppeteerExtra = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const axios = require('axios');

puppeteerExtra.use(StealthPlugin());

const app = express();
app.use(express.static(path.join(__dirname)));

const CHROMIUM_PATH = (() => {
  const { execSync } = require('child_process');
  try { return execSync('which chromium').toString().trim(); } catch (_) {}
  return '/nix/store/qa9cnw4v5xkxyip6mb9kxqfq1z4x2dx1-chromium-138.0.7204.100/bin/chromium';
})();

const PSA_CARDS = {
  pp4: {
    name: 'PP4 ブラマジガール',
    setID: 40702,
    setUrl: 'https://www.psacard.com/pop/tcg-cards/2001/yu-gi-oh-japanese-premium-pack-4/40702',
    rowFilter: (row) => row.SpecID !== 0 && row.CardNumber === 'P401',
  },
  kaiba: {
    name: '海馬セット ブルーアイズ',
    setID: 212330,
    setUrl: 'https://www.psacard.com/pop/tcg-cards/2022/yu-gi-oh-japanese-25th-anniversary-ultimate-kaiba-set/212330',
    rowFilter: (row) =>
      row.SpecID !== 0 &&
      row.SubjectName && row.SubjectName.toLowerCase().includes('blue-eyes white dragon') &&
      (row.CardNumber === '' || row.CardNumber === null || row.CardNumber === undefined),
  },
  ukiyoe: {
    name: '浮世絵ブルーアイズ',
    setID: 301724,
    setUrl: 'https://www.psacard.com/pop/tcg-cards/2025/yu-gi-oh-japanese-nyc1-blue-eyes-white-dragon-ukiyo-e-style-limited-ocg-card-framed-stamp-set/301724',
    rowFilter: (row) =>
      row.SpecID !== 0 &&
      row.SubjectName && row.SubjectName.toLowerCase().includes('blue-eyes white dragon'),
  },
  pikachu: {
    name: 'マックピカチュウ 2025 M-P プロモ',
    setID: 312898,
    setUrl: 'https://www.psacard.com/pop/tcg-cards/2025/pokemon-japanese-m-p-promo/312898',
    rowFilter: (row) =>
      row.SpecID !== 0 &&
      row.SubjectName && row.SubjectName.toLowerCase().includes('pikachu') &&
      row.CardNumber && row.CardNumber.toString().includes('020'),
  },
};

const CACHE = {};
let cacheTime = null;
let fetchInProgress = false;

function parseResult(data, rowFilter, label) {
  if (!data || !Array.isArray(data.data)) return null;
  let row = null;
  if (rowFilter) {
    const matches = data.data.filter(rowFilter);
    if (matches.length > 0) {
      row = matches.reduce((best, r) =>
        (parseInt(r.Grade10) || 0) > (parseInt(best.Grade10) || 0) ? r : best
      , matches[0]);
      console.log(`[PSA][${label}] Matched: Num=${row.CardNumber} Name=${row.SubjectName} G10=${row.Grade10}`);
    }
  }
  if (!row) row = data.data.find(d => d.SpecID === 0);
  if (!row) return null;
  const psa10 = parseInt(row.Grade10) || 0;
  const psa9 = parseInt(row.Grade9) || 0;
  const total = parseInt(row.GradeTotal) || (psa10 + psa9);
  const rate10 = total > 0 ? (psa10 / total) * 100 : null;
  const rate9  = total > 0 ? (psa9  / total) * 100 : null;
  return { psa10, psa9, total, rate10, rate9 };
}

async function fetchOneCard(cardKey) {
  const card = PSA_CARDS[cardKey];
  if (!card || !card.setUrl) return null;
  let browser = null;
  try {
    browser = await puppeteerExtra.launch({
      executablePath: CHROMIUM_PATH,
      headless: true,
      args: [
        '--no-sandbox', '--disable-setuid-sandbox',
        '--disable-dev-shm-usage', '--disable-gpu',
        '--no-first-run', '--no-zygote',
        '--disable-blink-features=AutomationControlled',
        '--window-size=1280,800',
      ],
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9,ja;q=0.8' });

    return await new Promise(async (resolve, reject) => {
      let settled = false;
      const done = (val, err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (err) reject(err); else resolve(val);
      };

      const timer = setTimeout(() => done(null, new Error('Timeout')), 70000);

      page.on('response', async (response) => {
        if (settled) return;
        try {
          const url = response.url();
          if (!url.includes('/Pop/') && !url.includes('/pop/')) return;
          const ct = response.headers()['content-type'] || '';
          if (!ct.includes('json')) return;
          const body = await response.text().catch(() => '');
          if (!body.includes('Grade10')) return;
          console.log(`[PSA][${cardKey}] Captured API response`);
          const json = JSON.parse(body);
          const result = parseResult(json, card.rowFilter, cardKey);
          if (result) done(result);
        } catch (_) {}
      });

      try {
        await page.goto(card.setUrl, { waitUntil: 'load', timeout: 55000 });
        // Wait for React to render and make API calls
        await new Promise(r => setTimeout(r, 8000));
      } catch (e) {
        console.log(`[PSA][${cardKey}] Nav error: ${e.message}`);
        // Even on nav error, wait for any pending response captures
        await new Promise(r => setTimeout(r, 3000));
      }
      if (!settled) done(null, new Error('No data captured'));
    });
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

async function fetchOneCardWithRetry(cardKey, retries = 10) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const result = await fetchOneCard(cardKey);
      if (result) return result;
    } catch (err) {
      console.log(`[PSA][${cardKey}] Attempt ${attempt} failed: ${err.message}`);
      if (attempt < retries) await new Promise(r => setTimeout(r, 5000));
    }
  }
  return null;
}

async function refreshAllCards() {
  if (fetchInProgress) return;
  fetchInProgress = true;
  console.log('[PSA] Starting background fetch of all cards...');
  const keys = Object.keys(PSA_CARDS);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if (i > 0) await new Promise(r => setTimeout(r, 3000)); // 3s gap between cards
    try {
      const result = await fetchOneCardWithRetry(key);
      if (result) {
        CACHE[key] = result;
        console.log(`[PSA][${key}] Cached: psa10=${result.psa10}, total=${result.total}`);
      } else {
        console.error(`[PSA][${key}] Fetch failed after retries`);
      }
    } catch (err) {
      console.error(`[PSA][${key}] Fetch error: ${err.message}`);
    }
  }
  cacheTime = new Date();
  fetchInProgress = false;
  console.log(`[PSA] Cache updated at ${cacheTime.toLocaleTimeString()}`);
}

app.get('/api/psa', async (req, res) => {
  const force = req.query.force === '1';
  const cacheAge = cacheTime ? (Date.now() - cacheTime.getTime()) : Infinity;
  const stale = cacheAge > 6 * 60 * 60 * 1000;

  if (!force && Object.keys(CACHE).length > 0 && !stale) {
    console.log('[PSA] Serving from cache');
    return res.json({ results: CACHE, errors: {}, cached: true, cacheTime });
  }

  try {
    await refreshAllCards();
    res.json({ results: CACHE, errors: {}, cached: false, cacheTime });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const YAHUOKU_CARDS = {
  pp4:    { keyword: '2001 ブラック・マジシャン・ガール PSA10',
            requiredRE: /P4-01/ },
  kaiba:  { keyword: '海馬セット 青眼の白龍 PSA10',
            requiredRE: /海馬|kaiba/i },
  ukiyoe: { keyword: '浮世絵 青眼の白龍 PSA10',
            requiredRE: /浮世絵/ },
  pikachu: { keyword: 'マックピカチュウ PSA10',
             requiredRE: /ピカチュウ|pikachu/i,
             titleRequireAll: ['2025', 'マクドナルド'],
             minPrice: 5000 },
};

// ── 共通ユーティリティ ──────────────────────────────
function calcOutlierFilter(prices) {
  const sorted = [...prices].sort((a, b) => a - b);
  if (sorted.length === 0) return { valid: [], median: 0, removed: 0 };
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
  const valid = sorted.filter(p =>
    p >= 5000 &&
    p >= median * 0.5 &&
    p <= median * 2.5
  );
  return { valid, median, removed: sorted.length - valid.length };
}

// ── ヤフオク生価格取得ヘルパー ───────────────────────
async function fetchYahuokuRaw(cardKey, days = 90) {
  const cardCfg = YAHUOKU_CARDS[cardKey];
  if (!cardCfg) throw new Error('invalid card key');
  const { keyword, requiredRE, titleRequireAll = [], minPrice = 0 } = cardCfg;

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);

  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja-JP,ja;q=0.9,en-US;q=0.8',
    'Referer': 'https://auctions.yahoo.co.jp/',
  };

  let allItems = [];
  let b = 1;
  const perPage = 50;

  while (true) {
    const url = `https://auctions.yahoo.co.jp/closedsearch/closedsearch?p=${encodeURIComponent(keyword)}&auccat=0&tab_ex=commerce&ei=utf-8&aq=-1&oq=&sc_i=&exflg=1&b=${b}&n=${perPage}&s1=end&o1=d`;
    const resp = await axios.get(url, { headers, timeout: 20000 });
    const html = resp.data;
    const match = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (!match) break;
    const nextData = JSON.parse(match[1]);
    const items = nextData?.props?.pageProps?.initialState?.search?.items?.listing?.items;
    if (!Array.isArray(items) || items.length === 0) break;
    allItems = allItems.concat(items);
    const oldest = new Date(items[items.length - 1].endTime);
    if (oldest < cutoff || items.length < perPage) break;
    b += perPage;
    if (b > 500) break;
  }

  // デバッグログ
  console.log(`[Yahuoku][${cardKey}] --- 生データ先頭5件 ---`);
  allItems.slice(0, 5).forEach((item, i) => {
    const type = item.isFleamarketItem ? 'フリマ' : 'オークション';
    console.log(`[Yahuoku][${cardKey}] #${i+1} [${type}] ${item.endTime?.substring(0,10)} ${(item.price||0).toLocaleString()}円 | ${(item.title||'').substring(0,60)}`);
  });
  const fleaCount = allItems.filter(i => i.isFleamarketItem).length;
  const auctionCount = allItems.length - fleaCount;
  const futureCount = allItems.filter(i => new Date(i.endTime) > new Date()).length;
  console.log(`[Yahuoku][${cardKey}] 取得合計=${allItems.length}件: オークション=${auctionCount}件 フリマ=${fleaCount}件 未来日付=${futureCount}件`);

  const BUNDLE_RE = /まとめ|複数|セット売|一括|連番|OCGカード|\d+枚セット|\s[＋+]\s|\s＋\S/;
  const FAKE_RE = /PSA\s*10相当|psa10相当|ARS鑑定|ARS10|レプリカ|Replica|コピー|模造/i;
  const PSA10_RE = /PSA[\s-]?10/i;

  const filtered = allItems.filter(item => {
    const title = item.title || '';
    if (!PSA10_RE.test(title)) return false;
    if (FAKE_RE.test(title)) return false;
    if (!requiredRE.test(title)) return false;
    if (BUNDLE_RE.test(title)) return false;
    if (titleRequireAll.some(kw => !title.includes(kw))) return false;
    if (minPrice > 0 && (item.price || 0) < minPrice) return false;
    const endDate = new Date(item.endTime);
    if (endDate > new Date()) return false;
    if (endDate < cutoff) return false;
    return true;
  });

  const reasons = { noPsa10: 0, fake: 0, noCardKw: 0, bundle: 0, future: 0, old: 0 };
  allItems.forEach(item => {
    const title = item.title || '';
    const endDate = new Date(item.endTime);
    if (!PSA10_RE.test(title)) reasons.noPsa10++;
    else if (FAKE_RE.test(title)) reasons.fake++;
    else if (!requiredRE.test(title)) reasons.noCardKw++;
    else if (BUNDLE_RE.test(title)) reasons.bundle++;
    else if (endDate > new Date()) reasons.future++;
    else if (endDate < cutoff) reasons.old++;
  });
  console.log(`[Yahuoku][${cardKey}] 除外内訳: PSA10なし=${reasons.noPsa10} ARS/偽=${reasons.fake} カードKWなし=${reasons.noCardKw} まとめ=${reasons.bundle} 出品中=${reasons.future} 90日超=${reasons.old}`);

  console.log(`[Yahuoku][${cardKey}] --- フィルタ後の有効アイテム (${filtered.length}件) ---`);
  filtered.forEach((item, i) => {
    const type = item.isFleamarketItem ? 'フリマ' : 'オークション';
    console.log(`[Yahuoku][${cardKey}] valid#${i+1} [${type}] ${item.endTime?.substring(0,10)} ${(item.price||0).toLocaleString()}円 | ${(item.title||'').substring(0,70)}`);
  });

  const sortedByDate = [...filtered].sort((a, b) => new Date(a.endTime) - new Date(b.endTime));
  const oldestDate = filtered.length ? sortedByDate[0].endTime.substring(0, 10) : null;
  const newestDate = filtered.length ? sortedByDate[sortedByDate.length - 1].endTime.substring(0, 10) : null;

  const rawPrices = filtered.map(i => i.price).filter(p => p > 0);
  return { rawPrices, rawCount: rawPrices.length, oldestDate, newestDate, fleaCount, auctionCount };
}

// ── カルドバ生価格取得ヘルパー ───────────────────────
async function fetchCardovaRaw(cardKey, days = 90) {
  const keyword = CARDOVA_KEYWORDS[cardKey];
  if (!keyword) throw new Error('invalid card key');

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);

  let allItems = [];
  let page = 1;
  const limit = 100;

  // status=close（落札済み）・APIデフォルト=最新順で全ページ取得
  while (true) {
    const url = `https://bg.cardova.co.jp/api/v1/auction/list?page=${page}&limit=${limit}&word=${encodeURIComponent(keyword)}&status=close`;
    const resp = await axios.get(url, {
      headers: {
        'Referer': 'https://www.cardova.co.jp/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
      timeout: 15000,
    });
    const items = resp.data.list || [];
    if (items.length === 0) break;
    allItems = allItems.concat(items);
    // 最古アイテムが90日cutoffより古ければ終了（最新順ソートを前提）
    const oldest = new Date(items[items.length - 1].end_date);
    if (oldest < cutoff) break;
    // 次ページへ
    page++;
    if (page > 200) break; // 安全上限
  }

  console.log(`[Cardova][${cardKey}] 取得完了: ${page}ページ / 合計${allItems.length}件（90日分）`);

  const itemFilter = CARDOVA_ITEM_FILTERS[cardKey] || null;
  const psa10 = allItems.filter(item => {
    // canceled_at=null（キャンセル除外・落札済みのみ）
    if (item.canceled_at !== null) return false;
    // 90日以内
    if (new Date(item.end_date) < cutoff) return false;
    // PSA10（authentication_company_code="P" かつ grade=10）
    if (item.authentication_company_code !== 'P') return false;
    if (parseFloat(item.grade) !== 10) return false;
    // カード固有フィルター（pikachu: year=2025, card_number #020）
    if (itemFilter && !itemFilter(item)) return false;
    return true;
  });

  const sorted = psa10.sort((a, b) => new Date(b.end_date) - new Date(a.end_date));
  const lastSaleDate = psa10.length ? sorted[0].end_date.substring(0, 10) : null;
  const oldestSaleDate = psa10.length ? sorted[sorted.length - 1].end_date.substring(0, 10) : null;

  const rawPrices = psa10.map(i => i.bid_price).filter(p => p > 0);
  const rawMax = rawPrices.length ? Math.max(...rawPrices) : 0;
  const rawMin = rawPrices.length ? Math.min(...rawPrices) : 0;
  console.log(`[Cardova][${cardKey}] PSA10絞込: ${psa10.length}件 価格範囲: ${rawMin.toLocaleString()}〜${rawMax.toLocaleString()}円 期間: ${oldestSaleDate}〜${lastSaleDate}`);
  if (cardKey === 'pikachu') {
    console.log(`[Cardova][pikachu] --- 先頭10件 ---`);
    sorted.slice(0, 10).forEach((item, i) => {
      console.log(`[Cardova][pikachu] #${i+1} ${item.end_date?.substring(0,10)} ${(item.bid_price||0).toLocaleString()}円 | card=${item.card_number} year=${item.year} variety_jp="${item.variety_jp}" auction_title="${item.auction_title}"`);
    });
  }
  return { rawPrices, rawCount: rawPrices.length, lastSaleDate };
}

// ── /api/yahuoku ────────────────────────────────────
app.get('/api/yahuoku', async (req, res) => {
  const cardKey = req.query.card;
  const days = Math.min(360, Math.max(7, parseInt(req.query.days) || 90));
  if (!YAHUOKU_CARDS[cardKey]) return res.status(400).json({ error: 'invalid card key' });
  try {
    const { rawPrices, rawCount, oldestDate, newestDate, fleaCount, auctionCount } = await fetchYahuokuRaw(cardKey, days);
    if (rawCount === 0) return res.json({ empty: true, message: 'ヤフオクに該当データなし', count: 0 });

    const { valid, median, removed } = calcOutlierFilter(rawPrices);
    if (valid.length === 0) return res.json({ empty: true, message: 'ヤフオクに該当データなし', count: 0 });

    const avg = Math.round(valid.reduce((a, b) => a + b, 0) / valid.length);
    console.log(`[Yahuoku][${cardKey}] 外れ値除去前=${rawCount}件 除去=${removed}件 有効=${valid.length}件 avg=${avg.toLocaleString()}円 median=${median.toLocaleString()}円`);
    res.json({ empty: false, avg, max: valid[valid.length-1], min: valid[0], median, count: valid.length, removed, lastSaleDate: newestDate, oldestDate, fleaCount, auctionCount });
  } catch (err) {
    console.error(`[Yahuoku][${cardKey}] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ── /api/combined (カルドバ＋ヤフオク合算) ────────────
app.get('/api/combined', async (req, res) => {
  const cardKey = req.query.card;
  const days = Math.min(360, Math.max(7, parseInt(req.query.days) || 90));
  if (!YAHUOKU_CARDS[cardKey] || !CARDOVA_KEYWORDS[cardKey]) return res.status(400).json({ error: 'invalid card key' });
  try {
    const [cardova, yahuoku] = await Promise.all([
      fetchCardovaRaw(cardKey, days).catch(e => { console.error(`[Combined][${cardKey}] Cardova error: ${e.message}`); return { rawPrices: [], rawCount: 0 }; }),
      fetchYahuokuRaw(cardKey, days).catch(e => { console.error(`[Combined][${cardKey}] Yahuoku error: ${e.message}`); return { rawPrices: [], rawCount: 0 }; }),
    ]);

    const combinedRaw = [...cardova.rawPrices, ...yahuoku.rawPrices];
    console.log(`[Combined][${cardKey}] カルドバ${cardova.rawCount}件 ＋ ヤフオク${yahuoku.rawCount}件 = 合計${combinedRaw.length}件`);

    if (combinedRaw.length === 0) {
      return res.json({ empty: true, message: '相場データなし', cardovaCount: 0, yahuokuCount: 0 });
    }

    const { valid, median, removed } = calcOutlierFilter(combinedRaw);
    if (valid.length === 0) {
      return res.json({ empty: true, message: '相場データなし', cardovaCount: cardova.rawCount, yahuokuCount: yahuoku.rawCount });
    }

    const avg = Math.round(valid.reduce((a, b) => a + b, 0) / valid.length);
    const max = valid[valid.length - 1];
    const min = valid[0];
    console.log(`[Combined][${cardKey}] 外れ値除去=${removed}件 有効=${valid.length}件 avg=${avg.toLocaleString()}円 median=${median.toLocaleString()}円`);
    res.json({
      empty: false, avg, max, min, median,
      totalCount: valid.length, removed,
      cardovaCount: cardova.rawCount,
      yahuokuCount: yahuoku.rawCount,
    });
  } catch (err) {
    console.error(`[Combined][${cardKey}] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ── カード検索用ヘルパー ──────────────────────────────
async function fetchCardovaByKeyword(keyword) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 90);
  let allItems = [];
  let page = 1;
  const limit = 100;
  while (true) {
    const url = `https://bg.cardova.co.jp/api/v1/auction/list?page=${page}&limit=${limit}&word=${encodeURIComponent(keyword + ' PSA10')}&status=close`;
    const resp = await axios.get(url, {
      headers: {
        'Referer': 'https://www.cardova.co.jp/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
      timeout: 15000,
    });
    const items = resp.data.list || [];
    const allCount = resp.data.all_count || 0;
    if (items.length === 0) break;
    allItems = allItems.concat(items);
    if (allItems.length >= allCount) break;
    const oldest = new Date(items[items.length - 1].end_date);
    if (oldest < cutoff) break;
    page++;
  }
  const keywordWords = keyword.toLowerCase().split(/\s+/).filter(w => w.length > 0 && !/^psa[\s-]?10$/i.test(w));
  const psa10 = allItems.filter(item => {
    if (item.authentication_company_code !== 'P') return false;
    if (parseFloat(item.grade) !== 10) return false;
    if (item.canceled_at !== null) return false;
    if (new Date(item.end_date) < cutoff) return false;
    const name = (item.name || '').toLowerCase();
    if (/カードダス/i.test(item.name || '')) return false;
    if (!keywordWords.every(w => name.includes(w))) return false;
    return true;
  });
  const rawPrices = psa10.map(i => i.bid_price).filter(p => p > 0);
  console.log(`[Search][Cardova] "${keyword}" PSA10: ${rawPrices.length}件`);
  psa10.sort((a, b) => new Date(b.end_date) - new Date(a.end_date)).forEach((item, i) => {
    console.log(`[Search][Cardova] #${i+1} ${item.end_date?.substring(0,10)} ${(item.bid_price||0).toLocaleString()}円 | ${(item.name||'').substring(0,60)}`);
  });
  return { rawPrices, rawCount: rawPrices.length };
}

async function fetchYahuokuByKeyword(keyword) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 90);
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja-JP,ja;q=0.9,en-US;q=0.8',
    'Referer': 'https://auctions.yahoo.co.jp/',
  };
  let allItems = [];
  let b = 1;
  const perPage = 50;
  const searchQuery = keyword + ' PSA10';
  while (true) {
    const url = `https://auctions.yahoo.co.jp/closedsearch/closedsearch?p=${encodeURIComponent(searchQuery)}&auccat=0&tab_ex=commerce&ei=utf-8&aq=-1&oq=&sc_i=&exflg=1&b=${b}&n=${perPage}&s1=end&o1=d`;
    const resp = await axios.get(url, { headers, timeout: 20000 });
    const html = resp.data;
    const match = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (!match) break;
    const nextData = JSON.parse(match[1]);
    const items = nextData?.props?.pageProps?.initialState?.search?.items?.listing?.items;
    if (!Array.isArray(items) || items.length === 0) break;
    allItems = allItems.concat(items);
    const oldest = new Date(items[items.length - 1].endTime);
    if (oldest < cutoff || items.length < perPage) break;
    b += perPage;
    if (b > 300) break;
  }
  const BUNDLE_RE = /まとめ|複数|セット売|一括|連番|OCGカード|\d+枚セット|\s[＋+]\s|\s＋\S/;
  const FAKE_RE = /PSA\s*10相当|psa10相当|ARS鑑定|ARS10|レプリカ|Replica|コピー|模造/i;
  const PSA10_RE = /PSA[\s-]?10/i;
  const keywordWords = keyword.toLowerCase().split(/\s+/).filter(w => w.length > 0 && !/^psa[\s-]?10$/i.test(w));
  const filtered = allItems.filter(item => {
    const title = item.title || '';
    const titleLower = title.toLowerCase();
    if (!PSA10_RE.test(title)) return false;
    if (FAKE_RE.test(title)) return false;
    if (BUNDLE_RE.test(title)) return false;
    if (/カードダス/i.test(title)) return false;
    if (!keywordWords.every(w => titleLower.includes(w))) return false;
    const endDate = new Date(item.endTime);
    if (endDate > new Date()) return false;
    if (endDate < cutoff) return false;
    return true;
  });
  const rawPrices = filtered.map(i => i.price).filter(p => p > 0);
  console.log(`[Search][Yahuoku] "${keyword}" PSA10: ${rawPrices.length}件`);
  [...filtered].sort((a, b) => new Date(b.endTime) - new Date(a.endTime)).forEach((item, i) => {
    const type = item.isFleamarketItem ? 'フリマ' : 'オークション';
    console.log(`[Search][Yahuoku] #${i+1} [${type}] ${item.endTime?.substring(0,10)} ${(item.price||0).toLocaleString()}円 | ${(item.title||'').substring(0,60)}`);
  });
  return { rawPrices, rawCount: rawPrices.length };
}

// ── /api/search (カード名フリー検索) ─────────────────
app.get('/api/search', async (req, res) => {
  const keyword = (req.query.keyword || '').trim();
  if (!keyword) return res.status(400).json({ error: 'キーワードを入力してください' });
  try {
    const [cardova, yahuoku] = await Promise.all([
      fetchCardovaByKeyword(keyword).catch(e => { console.error(`[Search] Cardova error: ${e.message}`); return { rawPrices: [], rawCount: 0 }; }),
      fetchYahuokuByKeyword(keyword).catch(e => { console.error(`[Search] Yahuoku error: ${e.message}`); return { rawPrices: [], rawCount: 0 }; }),
    ]);
    const combinedRaw = [...cardova.rawPrices, ...yahuoku.rawPrices];
    console.log(`[Search] "${keyword}" カルドバ${cardova.rawCount}件 ＋ ヤフオク${yahuoku.rawCount}件 ＝ ${combinedRaw.length}件`);
    if (combinedRaw.length === 0) {
      return res.json({ empty: true, message: '該当データなし', cardovaCount: 0, yahuokuCount: 0 });
    }
    const { valid, removed } = calcOutlierFilter(combinedRaw);
    if (valid.length === 0) {
      return res.json({ empty: true, message: '該当データなし', cardovaCount: cardova.rawCount, yahuokuCount: yahuoku.rawCount });
    }
    const avg = Math.round(valid.reduce((a, b) => a + b, 0) / valid.length);
    console.log(`[Search] ── 合算結果 ──────────────────────────────────`);
    console.log(`[Search] カルドバ${cardova.rawCount}件 ＋ ヤフオク${yahuoku.rawCount}件 → 合計生データ${combinedRaw.length}件`);
    console.log(`[Search] 外れ値除去=${removed}件 → 有効${valid.length}件`);
    console.log(`[Search] avg=${avg.toLocaleString()}円  max=${valid[valid.length-1].toLocaleString()}円  min=${valid[0].toLocaleString()}円`);
    console.log(`[Search] 有効価格一覧: [${valid.map(p=>p.toLocaleString()).join(', ')}]`);
    res.json({
      empty: false, avg,
      max: valid[valid.length - 1],
      min: valid[0],
      totalCount: valid.length,
      removed,
      cardovaCount: cardova.rawCount,
      yahuokuCount: yahuoku.rawCount,
    });
  } catch (err) {
    console.error(`[Search] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

const CARDOVA_KEYWORDS = {
  pp4: '2001 ブラック・マジシャン・ガール ウルトラレア',
  kaiba: '2022 青眼の白龍 Ultimate Kaiba Set',
  ukiyoe: '2025 浮世絵 青眼の白龍',
  pikachu: 'マクドナルド ピカチュウ',
};

const CARDOVA_ITEM_FILTERS = {
  pikachu: (item) =>
    item.authentication_company_code === 'P' &&
    item.year === 2025 &&
    item.card_number && /020/i.test(item.card_number),
};

// ── /api/cardova ─────────────────────────────────────
app.get('/api/cardova', async (req, res) => {
  const cardKey = req.query.card;
  const days = Math.min(360, Math.max(7, parseInt(req.query.days) || 90));
  if (!CARDOVA_KEYWORDS[cardKey]) return res.status(400).json({ error: 'invalid card key' });
  try {
    const { rawPrices, rawCount, lastSaleDate } = await fetchCardovaRaw(cardKey, days);
    if (rawCount === 0) return res.json({ empty: true, message: 'カルドバに該当データなし', count: 0 });

    const { valid, median, removed } = calcOutlierFilter(rawPrices);
    if (valid.length === 0) return res.json({ empty: true, message: 'カルドバに該当データなし', count: 0 });

    const avg = Math.round(valid.reduce((a, b) => a + b, 0) / valid.length);
    res.json({ empty: false, avg, max: valid[valid.length-1], min: valid[0], median, count: valid.length, removed, lastSaleDate });
  } catch (err) {
    console.error(`[Cardova][${cardKey}] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/psa-search', async (req, res) => {
  const q = req.query.q || '';
  let browser = null;
  try {
    browser = await puppeteerExtra.launch({
      executablePath: CHROMIUM_PATH, headless: true,
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage',
             '--disable-gpu','--no-first-run','--no-zygote','--single-process'],
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    const allData = [];
    page.on('response', async (response) => {
      try {
        const ct = response.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const body = await response.text().catch(() => '');
        if (body.includes('Grade10')) allData.push({ url: response.url(), body });
      } catch (_) {}
    });
    await page.goto(`https://www.psacard.com/pop/#0%7C${encodeURIComponent(q)}`, { waitUntil: 'networkidle2', timeout: 40000 });
    await new Promise(r => setTimeout(r, 5000));
    const links = await page.$$eval('a[href*="/pop/tcg-cards/"]', els =>
      els.map(a => ({ href: a.href, text: a.textContent.trim().substring(0, 100) })).slice(0, 50)
    );
    await browser.close().catch(() => {});
    res.json({ q, links, rawData: allData.slice(0, 3) });
  } catch (err) {
    if (browser) await browser.close().catch(() => {});
    res.json({ error: err.message });
  }
});

app.get('/api/psa-url', async (req, res) => {
  const setUrl = req.query.url;
  if (!setUrl) return res.status(400).json({ error: 'url required' });
  let browser = null;
  try {
    browser = await puppeteerExtra.launch({
      executablePath: CHROMIUM_PATH, headless: true,
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage',
             '--disable-gpu','--no-first-run','--no-zygote','--single-process'],
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9,ja;q=0.8' });
    let rawData = null;
    page.on('response', async (response) => {
      try {
        const url = response.url();
        if (!url.includes('/Pop/') && !url.includes('/pop/')) return;
        const ct = response.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const body = await response.text().catch(() => '');
        if (body.includes('Grade10')) rawData = JSON.parse(body);
      } catch (_) {}
    });
    await page.goto(setUrl, { waitUntil: 'networkidle2', timeout: 45000 });
    await new Promise(r => setTimeout(r, 5000));
    await browser.close().catch(() => {});
    if (!rawData) return res.json({ error: 'PSAデータを取得できませんでした' });
    const result = parseResult(rawData, null, 'url-search');
    if (!result) return res.json({ error: 'PSAデータの解析に失敗しました' });
    console.log(`[PSA-URL] psa10=${result.psa10} psa9=${result.psa9} total=${result.total} rate10=${result.rate10?.toFixed(1)}%`);
    res.json({ psa10: result.psa10, psa9: result.psa9, total: result.total, rate10: result.rate10 });
  } catch (err) {
    if (browser) await browser.close().catch(() => {});
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/psa-raw', async (req, res) => {
  const setUrl = req.query.url;
  if (!setUrl) return res.json({ error: 'url required' });
  let browser = null;
  try {
    browser = await puppeteerExtra.launch({
      executablePath: CHROMIUM_PATH, headless: true,
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage',
             '--disable-gpu','--no-first-run','--no-zygote','--single-process'],
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    let rawData = null;
    page.on('response', async (response) => {
      try {
        if (!response.url().includes('/Pop/')) return;
        const ct = response.headers()['content-type'] || '';
        if (!ct.includes('json')) return;
        const body = await response.text().catch(() => '');
        if (body.includes('Grade10')) rawData = JSON.parse(body);
      } catch (_) {}
    });
    await page.goto(setUrl, { waitUntil: 'networkidle2', timeout: 45000 });
    await new Promise(r => setTimeout(r, 5000));
    await browser.close().catch(() => {});
    res.json(rawData || { error: 'No data' });
  } catch (err) {
    if (browser) await browser.close().catch(() => {});
    res.json({ error: err.message });
  }
});

const PORT = 5000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[PSA] Server on http://0.0.0.0:${PORT}`);
  refreshAllCards().catch(e => console.error('[PSA] Initial fetch error:', e.message));
});
