# Test for regex_like function with sequence comparison operator

select id1,name from playerinfo p where p.info.id.score =any [30,40] or regex_like(p.info.id.name,"s.*","i")