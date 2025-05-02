#TestDescription: Use of Boundary matchers should be flagged as an error.

select id, name from playerinfo p where regex_like(p.name,"\\$")