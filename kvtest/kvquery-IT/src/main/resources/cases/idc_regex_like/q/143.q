# Test for the tab character.

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\t")