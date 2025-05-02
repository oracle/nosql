#TestDescription: ".*A\\*B.*"  (\) as the escape character. In the pattern, the escape character precedes the "." This interprets the "." literally, rather than as a special pattern matching character.

select id1,p.profile from playerinfo p where regex_like(p.profile,".*A\\*B.*")