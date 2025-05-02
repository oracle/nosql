# Test for alert (bell) character (unicode: '\u0007').

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\\a")