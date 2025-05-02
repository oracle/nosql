#TestDescription: source is array type inside json
#Expected result: return false

select regex_like(p.info.id.stats1,"11,222,33") from playerinfo p where id1=1