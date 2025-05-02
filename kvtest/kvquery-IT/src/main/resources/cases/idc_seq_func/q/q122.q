# Expression using $key
select id1, seq_avg(p.stats3.runs.values($key="odi")) from playerinfo p