#TestDescription: source is number type inside json
#Expected result: return false

select id1,regex_like(p.info.id.ballsbowled,"5266232323232323") from playerinfo p