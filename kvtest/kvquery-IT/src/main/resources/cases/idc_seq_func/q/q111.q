# Expression using slice step
select id1, seq_count(p.stats3.runs[0:4]) from playerinfo p