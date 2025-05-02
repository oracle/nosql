# Test for regex_like function with sum() function 

select sum(p.info.id.age) from playerinfo p where regex_like(p.name,"s.*","i")