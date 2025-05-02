#TestDescription: Pattern match string is a literal of Atomic String type.
#Expected result: return true

select regex_like(p.name,"Virat Kohli") from playerinfo p where id1=1