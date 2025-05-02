# Test for regex_like function with NOT operator

select id1,p.name, p.info.id.name from playerinfo p where NOT regex_like(p.info.id.name,"s.*","i")