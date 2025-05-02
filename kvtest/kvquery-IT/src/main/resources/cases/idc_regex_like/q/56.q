#TestDescription: pattern is of type timestamp
#Expected result: return error

select regex_like(p.name,p.time) from playerinfo p where id1=1