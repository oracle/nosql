#TestDescription: pattern is of type long
#Expected result: return error

select regex_like(p.name,p.ballsbowled) from playerinfo p where id1=1