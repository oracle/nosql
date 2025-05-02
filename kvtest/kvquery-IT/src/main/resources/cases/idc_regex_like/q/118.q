#TestDescription: flag is boolean type inside json
#Expected result: return error

select regex_like("true","true",p.info.id.tier1rated) from playerinfo p