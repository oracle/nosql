# Test for the escape character (unicode: '\u001B').

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\\e")