# Test for regex_like function with expression having array filter step

select count(p.info.id.phones[$element.areacode = 339]) from playerinfo p where regex_like(p.info.id.name,".*a.*","i")