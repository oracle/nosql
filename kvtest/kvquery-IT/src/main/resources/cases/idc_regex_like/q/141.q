# Test for hex characters

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\\x41\\x42\\x43")