# Test for regex_like function with min() function 

select min(p.info.id.age) from playerinfo p where regex_like(p.name,"s.*","i")