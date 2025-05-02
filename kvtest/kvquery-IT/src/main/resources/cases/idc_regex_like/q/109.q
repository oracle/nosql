#TestDescription: flag is of type boolean
#Expected result: return error

select regex_like(p.name,p.name,p.tier1rated) from playerinfo p where id1=1