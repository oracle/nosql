# Expression using $key
select id1, seq_count(p.stats1.values($key!="odi")) from playerinfo p