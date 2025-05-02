# Expression using $key
select id1, seq_max(p.stats3.runs.values($key!="test")) from playerinfo p