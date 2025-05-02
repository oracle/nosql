#TestDescription: pattern is of type binary
#Expected result: return error

select regex_like(p.name,p.bin) from playerinfo p where id1=1