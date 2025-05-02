#TestDescription: Use of predefined character classes should be flagged as an error.

select id, name from playerinfo p where regex_like(p.name,"\\w.*")