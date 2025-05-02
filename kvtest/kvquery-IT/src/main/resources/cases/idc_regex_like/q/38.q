#TestDescription: source is string column having value as sql null
#Expected result: return NULL

select regex_like(p.profile,"test") from playerinfo p where id1=2