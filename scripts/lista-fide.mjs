// Lista compacta da FIDE para o robô do índice casar nome → FIDE ID nas transmissões antigas.
// O site da FIDE recusa conexões dos servidores do GitHub, então a lista é gerada fora (de
// players_list_foa.txt, https://ratings.fide.com/download/players_list.zip) e publicada como anexo
// do release "lista-fide" deste repositório; o workflow baixa de lá.
// Saída: fide.tsv.gz, uma linha por jogador com rating: "FIDE ID<TAB>rating<TAB>nome".
// Uso: node scripts/lista-fide.mjs players_list_foa.txt [saida.tsv.gz]
import { createReadStream, createWriteStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import zlib from 'node:zlib';

const [src, out = 'fide.tsv.gz'] = process.argv.slice(2);
if (!src) throw new Error('uso: node scripts/lista-fide.mjs players_list_foa.txt [saida.tsv.gz]');

async function* rows() {
  let cols = null;
  for await (const line of createInterface({ input: createReadStream(src, 'latin1'), crlfDelay: Infinity })) {
    if (!cols) {
      const at = (h) => line.indexOf(h);
      cols = { id: at('ID Number'), name: at('Name'), fed: at('Fed'), s: at('SRtng'), sg: at('SGm'), r: at('RRtng'), rg: at('RGm'), b: at('BRtng'), bg: at('BGm') };
      continue;
    }
    const id = line.slice(cols.id, cols.name).trim();
    if (!/^[1-9]\d{3,}$/.test(id)) continue;
    const rating = Number(line.slice(cols.s, cols.sg)) || Number(line.slice(cols.r, cols.rg)) || Number(line.slice(cols.b, cols.bg)) || 0;
    if (!rating) continue; // nunca teve rating: não aparece em transmissão de torneio válido
    yield `${id}\t${rating}\t${line.slice(cols.name, cols.fed).trim()}\n`;
  }
}

await pipeline(Readable.from(rows()), zlib.createGzip({ level: 9 }), createWriteStream(out));
console.log(`ok: ${out}`);
