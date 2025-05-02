#TestDescription: source is map type inside json
#Expected result: return false

select regex_like(p.info.id.stats2,"odi") from playerinfo p where id1=1