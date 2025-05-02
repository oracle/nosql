# Test for oct and hex characters

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\u004F")