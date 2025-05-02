# Test for regex_like function with size function

select id1,size(p.info.id) from playerinfo p where regex_like(p.info.id.name,"s.*","i")