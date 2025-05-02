# Test for regex_like function with is of type operator

select id1,p.info.id.name from playerinfo p where p.info.id.address is of type (string) or regex_like(p.info.id.name,"s.*","i")