# Expression using slice step
select id1, seq_min(p.stats2.last5int20[1:2]) from playerinfo p