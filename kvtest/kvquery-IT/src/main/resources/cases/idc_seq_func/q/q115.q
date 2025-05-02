# Expression using slice step
select id1, seq_sum(p.stats2.last5intest[0:3]) from playerinfo p