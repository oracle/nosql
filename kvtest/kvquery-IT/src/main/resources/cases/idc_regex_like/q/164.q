# Test for regex_like function with Value comparison operator

select id1,name from playerinfo p where p.info.id.score = [10,20,30,40] or not regex_like(p.info.id.name,"s.*","i")