# Test for regex_like function with max() function 

select max(p.info.id.age) from playerinfo p where regex_like(p.name,"s.*","i")