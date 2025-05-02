# Expression using slice step
select id1, seq_avg(p.stats2.last5inodi[1:2]) from playerinfo p