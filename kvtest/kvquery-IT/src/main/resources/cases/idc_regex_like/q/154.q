# Test for regex_like function with count() function 

select count(id1) from playerinfo p where regex_like(p.info.id.name,"s.*","i")