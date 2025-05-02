# Expression using slice step
select id1, seq_sum(p.stats3.runs.test[0:4]) from playerinfo p