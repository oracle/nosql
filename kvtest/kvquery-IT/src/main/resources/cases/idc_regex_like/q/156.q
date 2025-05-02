# Test for regex_like function with avg() function 

select avg(p.info.id.age) from playerinfo p where regex_like(p.name,"a.*","i")