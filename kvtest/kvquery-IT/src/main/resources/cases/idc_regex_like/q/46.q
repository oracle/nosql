#TestDescription: Pattern match string is a Table Field.
#Expected result: return true

select regex_like(p.info.id.name,p.name) from playerinfo p where id1=1