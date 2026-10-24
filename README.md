# Dados do Preparador · Chess Codex

Índice aberto de partidas **presenciais** (clássicas, rápidas e blitz) por **FIDE ID**, gerado a partir das transmissões
oficiais do Lichess ([database.lichess.org/#broadcasts](https://database.lichess.org/#broadcasts)).

O índice não guarda os lances: só diz **onde** estão as partidas de cada jogador no Lichess (rodada e
capítulo da transmissão), com data, cor, resultado e ratings. O app baixa cada partida na hora pela API
pública do Lichess (`https://lichess.org/api/study/<rodada>/<capítulo>.pgn`).

## Arquivos (GitHub Pages)

- `meta.json`: período, totais, fonte e licença.
- `idx/<xx>.json`: jogadores cujo FIDE ID % 256 = `0x<xx>`, no formato
  `{ "<FIDE ID>": "entrada;entrada;…" }`, da partida mais recente para a mais antiga.
  Cada entrada: `rodada/capítulo,AAMMDD,cor(w|b),resultado do jogador(1|0|=|?),rating dele,rating do adversário,FIDE ID do adversário[,ritmo]`
  (ritmo ausente = clássica, `r` = rápida, `b` = blitz).
- `nivel/<faixa>/<xx>.json`: o que jogadores de cada faixa de rating (0 = abaixo de 1800, 1800, 2000, 2200, 2400+)
  jogam nos 12 primeiros lances, em partidas clássicas e rápidas presenciais. A faixa é a de quem joga o lance.
  `xx` = fnv1a dos 2 primeiros lances do caminho (separados por espaço) % 32, em hexadecimal.
  Formato: `{ "<lances até a posição, separados por espaço>": "san:partidas:meios-pontos|…" }`.

As transmissões até 2023 quase nunca trazem a tag `FideId`. Nelas, o nome do jogador é casado com o FIDE ID
pela [lista oficial da FIDE](https://ratings.fide.com/download/players_list.zip) (nome igual; homônimos
decididos pelo rating). Medido nas partidas que têm a tag: 99,7% de acerto; o que não casa fica de fora.

Atualizado todo mês pelo workflow `indice` (`scripts/indice.mjs`).

## Licença

Os dados são obra derivada das transmissões do Lichess e seguem a mesma licença,
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). Alterações feitas pelo Chess Codex:
só partidas jogadas presencialmente, com FIDE ID (da tag ou casado pelo nome), sem duplicatas e sem os lances,
e a árvore de aberturas por faixa de rating.
