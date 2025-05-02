# Test for regex_like function with exists operator

select id1,p.info.id.name from playerinfo p where exists p.info.id.address.zip or regex_like(p.name,"a.*","i")