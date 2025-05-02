# Expression using slice step
select id1, seq_min(p.stats3.runs.t20[0:4]) from playerinfo p