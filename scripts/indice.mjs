// Índice aberto de partidas presenciais por FIDE ID, a partir das transmissões oficiais do
// Lichess (https://database.lichess.org/#broadcasts, licença CC BY-SA 4.0). O índice só diz ONDE
// estão as partidas de cada jogador (rodada e capítulo da transmissão no Lichess), com data, cor,
// resultado e ratings; os lances ficam no Lichess e o app os baixa na hora pela API pública
// (GET https://lichess.org/api/study/<rodada>/<capítulo>.pgn).
//
// Saída (publicada no GitHub Pages pelo workflow indice.yml):
//   site/meta.json         período, totais, fonte e licença
//   site/idx/<xx>.json     jogadores cujo FIDE ID % 256 = 0x<xx>:
//                          { "<fideId>": "<entrada>;<entrada>;…" }, entradas da mais recente à mais antiga
//   entrada = rodada/capítulo,AAMMDD,cor(w|b),resultado do jogador(1|0|=|?),rating dele,rating do adversário,FIDE ID do adversário[,ritmo]
//             ritmo: ausente = clássica, r = rápida, b = blitz
//   site/nivel/<faixa>/<xx>.json  o que jogadores daquela faixa de rating jogam nos 12 primeiros lances
//                          (partidas clássicas e rápidas presenciais); faixa = rating de quem joga o
//                          lance: 0 (<1800), 1800, 2000, 2200, 2400 (2400+). xx = fnv1a(2 primeiros
//                          lances do caminho, separados por espaço) % 32 em hexadecimal:
//                          { "<lances até aqui, separados por espaço>": "san:partidas:meios-pontos|…" }
//
// As transmissões até 2023 quase não trazem a tag FideId: com --fide (players_list_foa.txt da lista
// oficial, https://ratings.fide.com/download/players_list.zip) o nome do jogador é casado com o FIDE ID.
//
// Só módulos do Node (zstd do próprio Node).
// Uso: node scripts/indice.mjs [--out site] [--month 2025-01,…] [--sample-mb 5] [--fide players_list_foa.txt]
import { createReadStream, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createInterface } from 'node:readline';
import { parseArgs } from 'node:util';
import zlib from 'node:zlib';

const { values: opt } = parseArgs({
  options: {
    out: { type: 'string', default: 'site' },
    month: { type: 'string', default: '' },
    'sample-mb': { type: 'string', default: '0' },
    fide: { type: 'string', default: '' },
  },
});
const OUT = resolve(opt.out);
const SAMPLE = Math.round(Number(opt['sample-mb']) * 1048576);
const BASE = 'https://database.lichess.org/broadcast/';
const SHARDS = 256;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ---------------------------------------------------------------------------------------------
// Ritmo da partida presencial: o relógio do primeiro lance ([%clk]) é o sinal mais confiável; sem
// ele, a tag TimeControl; sem nenhum dos dois, o nome do evento. 55 minutos ou mais = clássica,
// 10 a 54 = rápida, 3 a 9 = blitz; bullet, armageddon, online e variantes ficam de fora.
const MIN_MINUTES = 55;
const NOT_OTB = /bullet|armageddon|online|hybrid|titled|960|freestyle|fischer|random|simul|tcec|\bccc\b|computer|engine|bughouse/i;
const BLITZ_EVENT = /blitz|lightning|speed/i;
const RAPID_EVENT = /rapid|r[aá]pid|schnell|semi-?rapid|quick\s?play/i;
const TIEBREAK = /tie-?\s?breaks?|desempate|play-?offs?/i;
const ONLINE_SITE = /chess24|chess\.com|chesscom|playchess|tornelo|internet chess|chesskid/i;
const LICHESS_GAME = /lichess\.org\/(?!broadcast|study)[A-Za-z0-9]{8}\b/;
const REF = /\/([A-Za-z0-9]{8})\/([A-Za-z0-9]{8})\/?$/;
const FIDE = /^[1-9]\d{3,}$/;

function tcMinutes(tc) {
  const s = tc.replace(/^\D*/, '').replace(/^\d+\s*\/\s*/, '');
  const n = parseFloat(s);
  if (!Number.isFinite(n)) return -1;
  return n >= 300 ? n / 60 : n;
}
function clockMinutes(line) {
  let best = -1;
  let k = 0;
  for (const m of line.matchAll(/\[%clk (\d+):(\d+)(?::(\d+))?/g)) {
    best = Math.max(best, Number(m[1]) * 60 + Number(m[2]) + Number(m[3] ?? 0) / 60);
    if (++k >= 2) break;
  }
  return best;
}
// 'c' clássica, 'r' rápida, 'b' blitz, '' fora (online, bullet, armageddon, desempate de ritmo desconhecido)
function speedOf(h, line) {
  const ev = `${h.Event ?? ''} ${h.BroadcastName ?? ''} ${h.Round ?? ''}`;
  if (NOT_OTB.test(ev) || LICHESS_GAME.test(h.Site ?? '') || ONLINE_SITE.test(h.Site ?? '')) return '';
  let min = clockMinutes(line);
  if (min < 0 && h.TimeControl) min = tcMinutes(h.TimeControl);
  if (min >= 0) return min >= MIN_MINUTES ? 'c' : min >= 10 ? 'r' : min >= 3 ? 'b' : '';
  if (TIEBREAK.test(ev)) return '';
  if (BLITZ_EVENT.test(ev)) return 'b';
  if (RAPID_EVENT.test(ev)) return 'r';
  return 'c';
}

// ---------------------------------------------------------------------------------------------
// Nome → FIDE ID pela lista oficial. Chave = palavras do nome sem acento, em ordem alfabética
// ("Carlsen, Magnus" = "Magnus Carlsen"); a chave curta usa só o primeiro prenome (a lista traz
// "Silva, Joao Ricardo", a transmissão às vezes só "Joao Silva"). Homônimos: decide o rating.
const LETTER = { ø: 'o', æ: 'ae', œ: 'oe', ß: 'ss', ł: 'l', đ: 'd', ð: 'd', þ: 'th', ı: 'i' };
const NAME_STOP = new Set(['gm', 'im', 'fm', 'cm', 'wgm', 'wim', 'wfm', 'wcm', 'dr', 'prof', 'jr', 'sr']);
const words = (name) =>
  name
    .toLowerCase()
    .replace(/[øæœßłđðþı]/g, (ch) => LETTER[ch])
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z,]+/g, ' ');
const keyOf = (tokens) =>
  tokens
    .filter((t) => t.length > 1 && !NAME_STOP.has(t))
    .sort()
    .join(' ');
const nameKey = (name) => keyOf(words(name).replace(/,/g, ' ').trim().split(/\s+/));

let fide = null; // { full: Map<chave, [id, rating][]>, short: Map }

async function loadFide(path) {
  const full = new Map();
  const short = new Map();
  const add = (map, k, p) => {
    if (!k) return;
    const list = map.get(k);
    if (list) list.push(p);
    else map.set(k, [p]);
  };
  let cols = null;
  let n = 0;
  for await (const line of createInterface({ input: createReadStream(path, 'latin1'), crlfDelay: Infinity })) {
    if (!cols) {
      const at = (h) => line.indexOf(h);
      cols = { id: at('ID Number'), name: at('Name'), fed: at('Fed'), s: at('SRtng'), sg: at('SGm'), r: at('RRtng'), rg: at('RGm'), b: at('BRtng'), bg: at('BGm') };
      continue;
    }
    const id = line.slice(cols.id, cols.name).trim();
    if (!FIDE.test(id)) continue;
    const rating = Number(line.slice(cols.s, cols.sg)) || Number(line.slice(cols.r, cols.rg)) || Number(line.slice(cols.b, cols.bg)) || 0;
    if (!rating) continue; // nunca teve rating: não aparece em transmissão de torneio válido
    const raw = words(line.slice(cols.name, cols.fed).trim());
    const [sur, given = ''] = raw.split(',');
    const s = sur.trim().split(/\s+/);
    const g = given.trim().split(/\s+/).filter(Boolean);
    const p = [id, rating];
    const kFull = keyOf([...s, ...g]);
    add(full, kFull, p);
    const kShort = keyOf([...s, ...g.slice(0, 1)]);
    if (kShort !== kFull) add(short, kShort, p);
    n++;
  }
  log(`lista da FIDE: ${n} jogadores com rating`);
  return { full, short };
}

const resolved = new Map(); // "nome|rating/100" -> id ('' = não achou)
function idByName(name, rating) {
  if (!fide || !name) return '';
  const memo = `${name}|${Math.round(rating / 100)}`;
  const hit = resolved.get(memo);
  if (hit !== undefined) return hit;
  const k = nameKey(name);
  let id = '';
  const full = fide.full.get(k);
  if (full) {
    // nome completo igual: um só no mundo vale mesmo com o rating mudado (juniores sobem rápido)
    const near = rating ? full.filter(([, r]) => Math.abs(r - rating) <= 250) : [];
    if (near.length === 1) id = near[0][0];
    else if (full.length === 1 && (!rating || Math.abs(full[0][1] - rating) <= 600)) id = full[0][0];
  }
  if (!id) {
    const short = fide.short.get(k);
    const near = short && rating ? short.filter(([, r]) => Math.abs(r - rating) <= 200) : [];
    if (near.length === 1) id = near[0][0];
  }
  resolved.set(memo, id);
  return id;
}

// ---------------------------------------------------------------------------------------------
// Lances (SAN) do começo da partida, sem comentários, variantes nem números.
const SAN = /^(?:[KQRBN][a-h]?[1-8]?x?[a-h][1-8]|[a-h](?:x[a-h])?[1-8](?:=[QRBN])?|O-O(?:-O)?)[+#]?$/;
function sansOf(line, max) {
  const out = [];
  const text = line.replace(/\{[^}]*\}/g, ' ').replace(/\([^()]*\)/g, ' ').replace(/\$\d+/g, ' ');
  for (const tok of text.split(/\s+/)) {
    const t = tok.replace(/^\d+\.+/, '').replace(/[!?]+$/, '');
    if (!t) continue;
    if (!SAN.test(t)) break;
    out.push(t);
    if (out.length >= max) break;
  }
  return out;
}

// Árvore por faixa de rating de quem joga o lance: "faixa|caminho|lance" -> [partidas, meios-pontos]
const LEVEL_PLIES = 12;
const BANDS = [2400, 2200, 2000, 1800, 0];
const bandOf = (r) => BANDS.find((b) => r >= b);
const tree = new Map();
function addTree(sans, we, be, result) {
  if (!result) return;
  for (let i = 0; i < sans.length; i++) {
    const white = i % 2 === 0;
    const r = white ? we : be;
    if (!r) continue;
    const pts = result === '=' ? 1 : (result === '1') === white ? 2 : 0;
    const key = `${bandOf(r)}|${sans.slice(0, i).join(' ')}|${sans[i]}`;
    const v = tree.get(key);
    if (v) {
      v[0]++;
      v[1] += pts;
    } else tree.set(key, [1, pts]);
  }
}
function fnv1a(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h;
}
const LEVEL_SHARDS = 32;
const LEVEL_MIN_GAMES = 8; // posição com menos partidas que isso não entra
const LEVEL_MIN_MOVE = 2;

// O Lichess comprime com pzstd: cada frame zstd vem depois de um "skippable frame", e o zstd do
// Node para no primeiro deles. Este filtro anda pelos frames (RFC 8878) e tira os skippable.
function stripSkippableFrames() {
  let pending = Buffer.alloc(0);
  let state = 'frame';
  let copy = 0;
  let skip = 0;
  let checksum = false;
  return new Transform({
    transform(chunk, _enc, done) {
      pending = pending.length ? Buffer.concat([pending, chunk]) : Buffer.from(chunk);
      let i = 0;
      const out = [];
      for (;;) {
        if (skip) {
          const n = Math.min(skip, pending.length - i);
          skip -= n;
          i += n;
          if (skip) break;
          continue;
        }
        if (copy) {
          const n = Math.min(copy, pending.length - i);
          if (n) out.push(pending.subarray(i, i + n));
          copy -= n;
          i += n;
          if (copy) break;
          continue;
        }
        const left = pending.length - i;
        if (state === 'checksum') {
          copy = checksum ? 4 : 0;
          state = 'frame';
          continue;
        }
        if (state === 'block') {
          if (left < 3) break;
          const v = pending[i] | (pending[i + 1] << 8) | (pending[i + 2] << 16);
          const type = (v >> 1) & 3;
          copy = 3 + (type === 1 ? 1 : v >>> 3);
          if (v & 1) state = 'checksum';
          continue;
        }
        if (left < 4) break;
        const magic = pending.readUInt32LE(i);
        if ((magic & 0xfffffff0) === 0x184d2a50) {
          if (left < 8) break;
          skip = 8 + pending.readUInt32LE(i + 4);
          continue;
        }
        if (magic !== 0xfd2fb528) return done(new Error(`não é zstd (magic ${magic.toString(16)})`));
        if (left < 5) break;
        const fhd = pending[i + 4];
        const single = (fhd >> 5) & 1;
        const fcs = [single ? 1 : 0, 2, 4, 8][fhd >> 6];
        checksum = !!((fhd >> 2) & 1);
        copy = 5 + (single ? 0 : 1) + [0, 1, 2, 4][fhd & 3] + fcs;
        state = 'block';
      }
      pending = pending.subarray(i);
      for (const b of out) this.push(b);
      done();
    },
  });
}

// ---------------------------------------------------------------------------------------------
const byId = new Map(); // FIDE ID -> [{ d, s }] (d = AAMMDD para ordenar, s = entrada)
const seen = new Set();
const totals = { read: 0, classical: 0, rapid: 0, blitz: 0, indexed: 0, byName: 0, duplicates: 0, noRef: 0, noId: 0, months: 0 };
const elo = (v) => (/^\d{3,4}$/.test(v ?? '') ? Number(v) : 0);
const RES = { '1-0': ['1', '0'], '0-1': ['0', '1'], '1/2-1/2': ['=', '='] };

function addGame(h, moveLine) {
  totals.read++;
  if ((h.Variant && h.Variant !== 'Standard') || h.FEN) return;
  const speed = speedOf(h, moveLine);
  if (!speed) return;
  totals[{ c: 'classical', r: 'rapid', b: 'blitz' }[speed]]++;
  const we = elo(h.WhiteElo);
  const be = elo(h.BlackElo);
  let w = FIDE.test(h.WhiteFideId ?? '') ? h.WhiteFideId : '';
  let b = FIDE.test(h.BlackFideId ?? '') ? h.BlackFideId : '';
  const tagged = Boolean(w && b);
  if (!w) w = idByName(h.White ?? '', we);
  if (!b) b = idByName(h.Black ?? '', be);
  const date = (h.Date || h.UTCDate || '').replace(/\D/g, '').slice(2, 8);
  const key = `${date}|${w || h.White}|${b || h.Black}|${h.Round ?? ''}`;
  if (seen.has(key)) return void totals.duplicates++;
  seen.add(key);
  const [rw, rb] = RES[h.Result] ?? ['?', '?'];
  if (speed !== 'b') addTree(sansOf(moveLine, LEVEL_PLIES), we, be, RES[h.Result] ? rw : '');
  if (!w && !b) return void totals.noId++;
  const ref = REF.exec(h.GameURL ?? h.Site ?? '');
  if (!ref) return void totals.noRef++;
  totals.indexed++;
  if (!tagged) totals.byName++;
  const at = `${ref[1]}/${ref[2]}`;
  const sp = speed === 'c' ? '' : `,${speed}`;
  const put = (id, s) => {
    let list = byId.get(id);
    if (!list) byId.set(id, (list = []));
    list.push({ d: date, s });
  };
  if (w) put(w, `${at},${date},w,${rw},${we},${be},${b}${sp}`);
  if (b) put(b, `${at},${date},b,${rb},${be},${we},${w}${sp}`);
}

async function readMonth(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'ChessCodex-dados-preparador', ...(SAMPLE ? { Range: `bytes=0-${SAMPLE}` } : {}) } });
  if (!res.ok && res.status !== 206) throw new Error(`HTTP ${res.status} em ${url}`);
  let h = {};
  let moves = '';
  let inMoves = false;
  let rest = '';
  const flush = () => {
    if (Object.keys(h).length) addGame(h, moves);
    h = {};
    moves = '';
    inMoves = false;
  };
  const sink = async (src) => {
    for await (const chunk of src) {
      const text = rest + chunk.toString('utf8');
      const lines = text.split('\n');
      rest = lines.pop();
      for (const l of lines) {
        if (l.startsWith('[')) {
          if (inMoves) flush();
          const m = /^\[(\w+)\s+"(.*)"\]/.exec(l);
          if (m) h[m[1]] = m[2];
        } else if (l.trim()) {
          if (moves.length < 1200) moves += ` ${l}`;
          inMoves = true;
        }
      }
    }
  };
  try {
    await pipeline(Readable.fromWeb(res.body), stripSkippableFrames(), zlib.createZstdDecompress(), sink);
  } catch (e) {
    if (!SAMPLE) throw e; // amostra = arquivo cortado: o fim reclama, o que veio antes vale
  }
  flush();
}

async function main() {
  let months = opt.month.split(',').map((m) => m.trim()).filter(Boolean);
  if (!months.length) {
    const list = await (await fetch(`${BASE}list.txt`)).text();
    months = list
      .split('\n')
      .map((l) => /(\d{4}-\d{2})\.pgn\.zst\s*$/.exec(l)?.[1])
      .filter(Boolean)
      .sort();
  }
  if (opt.fide && existsSync(opt.fide)) fide = await loadFide(opt.fide);
  else log('sem a lista da FIDE: só as partidas com a tag FideId entram');
  for (const m of months) {
    log(`lendo ${m}…`);
    await readMonth(`${BASE}lichess_db_broadcast_${m}.pgn.zst`);
    totals.months++;
  }

  const shards = Array.from({ length: SHARDS }, () => ({}));
  let entries = 0;
  for (const [id, list] of byId) {
    list.sort((a, b) => b.d.localeCompare(a.d));
    shards[Number(id) % SHARDS][id] = list.map((e) => e.s).join(';');
    entries += list.length;
  }
  mkdirSync(join(OUT, 'idx'), { recursive: true });
  let bytes = 0;
  shards.forEach((s, i) => {
    const json = JSON.stringify(s);
    bytes += json.length;
    writeFileSync(join(OUT, 'idx', `${i.toString(16).padStart(2, '0')}.json`), json);
  });
  // árvore por nível: agrupa "faixa|caminho" e corta o que tem pouca partida
  const level = new Map(); // "faixa|caminho" -> { n, moves: [san, n, pts][] }
  for (const [key, [n, pts]] of tree) {
    const cut = key.lastIndexOf('|');
    const at = key.slice(0, cut);
    let node = level.get(at);
    if (!node) level.set(at, (node = { n: 0, moves: [] }));
    node.n += n;
    if (n >= LEVEL_MIN_MOVE) node.moves.push([key.slice(cut + 1), n, pts]);
  }
  tree.clear();
  const levelFiles = new Map(); // "faixa/xx" -> { caminho: "san:n:pts|…" }
  let positions = 0;
  for (const [at, node] of level) {
    if (node.n < LEVEL_MIN_GAMES || !node.moves.length) continue;
    const bar = at.indexOf('|');
    const band = at.slice(0, bar);
    const path = at.slice(bar + 1);
    const shard = (fnv1a(path.split(' ').slice(0, 2).join(' ')) % LEVEL_SHARDS).toString(16).padStart(2, '0');
    const file = `${band}/${shard}`;
    let obj = levelFiles.get(file);
    if (!obj) levelFiles.set(file, (obj = {}));
    obj[path] = node.moves
      .sort((a, b) => b[1] - a[1])
      .map(([san, n, pts]) => `${san}:${n}:${pts}`)
      .join('|');
    positions++;
  }
  let levelBytes = 0;
  for (const band of BANDS) mkdirSync(join(OUT, 'nivel', String(band)), { recursive: true });
  for (const [file, obj] of levelFiles) {
    const json = JSON.stringify(obj);
    levelBytes += json.length;
    writeFileSync(join(OUT, 'nivel', `${file}.json`), json);
  }
  log(`nível: ${positions} posições, ${(levelBytes / 1e6).toFixed(1)} MB`);

  const meta = {
    v: 2,
    generated: new Date().toISOString().slice(0, 10),
    months: [months[0], months[months.length - 1]],
    games: totals.indexed,
    players: byId.size,
    entries,
    shards: SHARDS,
    format: 'idx/<FIDE ID % 256 em hexadecimal, 2 dígitos>.json -> { "<FIDE ID>": "entrada;entrada;…" }; entrada = rodada/capítulo,AAMMDD,cor,resultado do jogador,rating dele,rating do adversário,FIDE ID do adversário[,ritmo r|b]; lances em https://lichess.org/api/study/<rodada>/<capítulo>.pgn',
    level: {
      format: 'nivel/<faixa>/<fnv1a(2 primeiros lances do caminho) % 32, hexadecimal>.json -> { "<lances separados por espaço>": "san:partidas:meios-pontos|…" }',
      bands: BANDS,
      plies: LEVEL_PLIES,
      minGames: LEVEL_MIN_GAMES,
      positions,
    },
    source: 'Transmissões oficiais do Lichess (lichess.org/broadcast), base em https://database.lichess.org/#broadcasts',
    license: 'CC BY-SA 4.0 (https://creativecommons.org/licenses/by-sa/4.0/). Alterado pelo Chess Codex: só partidas presenciais (clássicas, rápidas e blitz) com FIDE ID (da tag ou casado pelo nome na lista oficial da FIDE), sem os lances; árvore de aberturas por faixa de rating.',
    totals,
  };
  writeFileSync(join(OUT, 'meta.json'), `${JSON.stringify(meta, null, 2)}\n`);
  writeFileSync(
    join(OUT, 'index.html'),
    `<!doctype html><meta charset="utf-8"><title>Dados do Preparador · Chess Codex</title><p>Índice aberto de partidas presenciais por FIDE ID e árvore de aberturas por nível (transmissões do Lichess, CC BY-SA 4.0). Detalhes em <a href="meta.json">meta.json</a> e no <a href="https://github.com/chess-codex/dados-preparador">repositório</a>.</p>\n`,
  );
  log(`ok: ${totals.indexed} partidas, ${byId.size} jogadores, ${entries} entradas, ${(bytes / 1e6).toFixed(1)} MB em ${SHARDS} arquivos`);
}

await main();
