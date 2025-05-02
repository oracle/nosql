# Expression using slice step
select id1, seq_avg(p.stats3.runs.odi[0:4]) from playerinfo p