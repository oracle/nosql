#TestDescription: pattern is of type fixed binary
#Expected result: return error

select regex_like(p.name,p.fbin) from playerinfo p where id1=1