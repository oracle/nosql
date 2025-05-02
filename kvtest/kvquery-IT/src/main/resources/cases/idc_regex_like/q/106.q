#TestDescription: flag is of type double
#Expected result: return error

select regex_like(p.name,p.name,p.strikerate) from playerinfo p where id1=1