# Expression using slice step
select id1, seq_max(p.stats2.last5int20[1:2]) from playerinfo p