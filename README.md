# Dados do Preparador · Chess Codex

Índice aberto de partidas **clássicas presenciais** por **FIDE ID**, gerado a partir das transmissões
oficiais do Lichess ([database.lichess.org/#broadcasts](https://database.lichess.org/#broadcasts)).

O índice não guarda os lances: só diz **onde** estão as partidas de cada jogador no Lichess (rodada e
capítulo da transmissão), com data, cor, resultado e ratings. O app baixa cada partida na hora pela API
pública do Lichess (`https://lichess.org/api/study/<rodada>/<capítulo>.pgn`).

## Arquivos (GitHub Pages)

- `meta.json`: período, totais, fonte e licença.
- `idx/<xx>.json`: jogadores cujo FIDE ID % 256 = `0x<xx>`, no formato
  `{ "<FIDE ID>": "entrada;entrada;…" }`, da partida mais recente para a mais antiga.
  Cada entrada: `rodada/capítulo,AAMMDD,cor(w|b),resultado do jogador(1|0|=|?),rating dele,rating do adversário,FIDE ID do adversário`.

Atualizado todo mês pelo workflow `indice` (`scripts/indice.mjs`).

## Licença

Os dados são obra derivada das transmissões do Lichess e seguem a mesma licença,
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Alterações feitas pelo Chess Codex:
só partidas de ritmo clássico jogadas presencialmente, com FIDE ID, sem duplicatas e sem os lances.
