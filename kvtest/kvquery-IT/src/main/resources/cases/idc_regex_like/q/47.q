#TestDescription: Pattern match string is a JSON string field.
#Expected result: return true

select regex_like(p.name,p.info.id.name) from playerinfo p where id1=1