#TestDescription: pattern is map type inside json
#Expected result: return error

select regex_like("odi",p.info.id.stats2) from playerinfo p where id1=1