# Test for regex_like function with OR operator

select id1,p.name, p.info.id.name from playerinfo p where regex_like(p.info.id.name,"s.*","i") OR regex_like(p.name,"r.*","i")