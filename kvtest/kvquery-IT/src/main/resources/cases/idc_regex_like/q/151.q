# Test for regex_like function with AND operator

select id1,p.name, p.info.id.name from playerinfo p where regex_like(p.info.id.name,"s.*","i") AND regex_like(p.name,"r.*","i")