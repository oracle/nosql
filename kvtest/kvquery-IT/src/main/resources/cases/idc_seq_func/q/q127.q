# Expression using $key
select id1, seq_min(p.stats3.runs.values($key="t20")) from playerinfo p