# Test for the newline (line feed) character (unicode: '\u000A').

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\u000A")