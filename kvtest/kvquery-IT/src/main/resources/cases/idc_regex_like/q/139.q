# Test for Flag = "c" CANON_EQ

select id1, profile from playerinfo p where regex_like(p.info.id.profile,"\u00C3","c")