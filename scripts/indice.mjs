// Índice aberto de partidas clássicas presenciais por FIDE ID, a partir das transmissões oficiais do
// Lichess (https://database.lichess.org/#broadcasts, licença CC BY-SA 4.0). O índice só diz ONDE
// estão as partidas de cada jogador (rodada e capítulo da transmissão no Lichess), com data, cor,
// resultado e ratings; os lances ficam no Lichess e o app os baixa na hora pela API pública
// (GET https://lichess.org/api/study/<rodada>/<capítulo>.pgn).
//
// Saída (publicada no GitHub Pages pelo workflow indice.yml):
//   site/meta.json         período, totais, fonte e licença
//   site/idx/<xx>.json     jogadores cujo FIDE ID % 256 = 0x<xx>:
//                          { "<fideId>": "<entrada>;<entrada>;…" }, entradas da mais recente à mais antiga
//   entrada = rodada/capítulo,AAMMDD,cor(w|b),resultado do jogador(1|0|=|?),rating dele,rating do adversário,FIDE ID do adversário
//
// Só módulos do Node (zstd do próprio Node). Uso: node scripts/indice.mjs [--out site] [--month 2025-01,…] [--sample-mb 5]
import { mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import zlib from 'node:zlib';

const { values: opt } = parseArgs({
  options: { out: { type: 'string', default: 'site' }, month: { type: 'string', default: '' }, 'sample-mb': { type: 'string', default: '0' } },
});
const OUT = resolve(opt.out);
const SAMPLE = Math.round(Number(opt['sample-mb']) * 1048576);
const BASE = 'https://database.lichess.org/broadcast/';
const SHARDS = 256;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ---------------------------------------------------------------------------------------------
// Partida clássica presencial: o relógio do primeiro lance ([%clk]) é o sinal mais confiável; sem
// ele, a tag TimeControl; sem nenhum dos dois, aceita se o nome do evento não for de ritmo rápido
// nem online. Ritmo de 55 minutos ou mais = clássica.
const MIN_MINUTES = 55;
const FAST_EVENT = /blitz|rapid|r[aá]pid|bullet|armageddon|tie-?\s?breaks?|desempate|play-?offs?|speed|online|hybrid|titled|960|freestyle|fischer|random|schnell|semi-?rapid|quick\s?play|lightning|simul|tcec|\bccc\b|computer|engine|bughouse/i;
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
function classical(h, line) {
  if (FAST_EVENT.test(`${h.Event ?? ''} ${h.BroadcastName ?? ''} ${h.Round ?? ''}`)) return false;
  if (LICHESS_GAME.test(h.Site ?? '')) return false;
  const clk = clockMinutes(line);
  if (clk >= 0) return clk >= MIN_MINUTES;
  const tc = h.TimeControl ? tcMinutes(h.TimeControl) : -1;
  if (tc >= 0) return tc >= MIN_MINUTES;
  return !ONLINE_SITE.test(h.Site ?? '');
}

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
const totals = { read: 0, classical: 0, indexed: 0, duplicates: 0, noRef: 0, months: 0 };
const elo = (v) => (/^\d{3,4}$/.test(v ?? '') ? Number(v) : 0);
const RES = { '1-0': ['1', '0'], '0-1': ['0', '1'], '1/2-1/2': ['=', '='] };

function addGame(h, moveLine) {
  totals.read++;
  if ((h.Variant && h.Variant !== 'Standard') || h.FEN) return;
  if (!classical(h, moveLine)) return;
  totals.classical++;
  const w = FIDE.test(h.WhiteFideId ?? '') ? h.WhiteFideId : '';
  const b = FIDE.test(h.BlackFideId ?? '') ? h.BlackFideId : '';
  if (!w && !b) return;
  const ref = REF.exec(h.GameURL ?? h.Site ?? '');
  if (!ref) return void totals.noRef++;
  const date = (h.Date || h.UTCDate || '').replace(/\D/g, '').slice(2, 8);
  const key = `${date}|${w || h.White}|${b || h.Black}|${h.Round ?? ''}`;
  if (seen.has(key)) return void totals.duplicates++;
  seen.add(key);
  totals.indexed++;
  const [rw, rb] = RES[h.Result] ?? ['?', '?'];
  const at = `${ref[1]}/${ref[2]}`;
  const we = elo(h.WhiteElo);
  const be = elo(h.BlackElo);
  const put = (id, s) => {
    let list = byId.get(id);
    if (!list) byId.set(id, (list = []));
    list.push({ d: date, s });
  };
  if (w) put(w, `${at},${date},w,${rw},${we},${be},${b}`);
  if (b) put(b, `${at},${date},b,${rb},${be},${we},${w}`);
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
          if (moves.length < 400) moves += ` ${l}`;
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
  const meta = {
    v: 1,
    generated: new Date().toISOString().slice(0, 10),
    months: [months[0], months[months.length - 1]],
    games: totals.indexed,
    players: byId.size,
    entries,
    shards: SHARDS,
    format: 'idx/<FIDE ID % 256 em hexadecimal, 2 dígitos>.json -> { "<FIDE ID>": "entrada;entrada;…" }; entrada = rodada/capítulo,AAMMDD,cor,resultado do jogador,rating dele,rating do adversário,FIDE ID do adversário; lances em https://lichess.org/api/study/<rodada>/<capítulo>.pgn',
    source: 'Transmissões oficiais do Lichess (lichess.org/broadcast), base em https://database.lichess.org/#broadcasts',
    license: 'CC BY-SA 4.0 (https://creativecommons.org/licenses/by-sa/4.0/). Alterado pelo Chess Codex: só partidas clássicas presenciais com FIDE ID, sem os lances.',
    totals,
  };
  writeFileSync(join(OUT, 'meta.json'), `${JSON.stringify(meta, null, 2)}\n`);
  writeFileSync(
    join(OUT, 'index.html'),
    `<!doctype html><meta charset="utf-8"><title>Dados do Preparador · Chess Codex</title><p>Índice aberto de partidas clássicas presenciais por FIDE ID (transmissões do Lichess, CC BY-SA 4.0). Detalhes em <a href="meta.json">meta.json</a> e no <a href="https://github.com/chess-codex/dados-preparador">repositório</a>.</p>\n`,
  );
  log(`ok: ${totals.indexed} partidas, ${byId.size} jogadores, ${entries} entradas, ${(bytes / 1e6).toFixed(1)} MB em ${SHARDS} arquivos`);
}

await main();
