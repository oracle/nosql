#TestDescription: pattern is of type float
#Expected result: return error

select regex_like(p.name,p.avg) from playerinfo p where id1=1