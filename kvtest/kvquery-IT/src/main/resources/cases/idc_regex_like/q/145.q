# Test for the newline carriage-return character (unicode: '\u000D').

select id1, p.info.id.profile from playerinfo p where regex_like(p.info.id.profile,"\u000D")