#TestDescription: flag is map type inside json
#Expected result: return error

select regex_like("odi","odi",p.info.id.stats2) from playerinfo p where id1=1