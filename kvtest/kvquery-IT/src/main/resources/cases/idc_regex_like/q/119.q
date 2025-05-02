#TestDescription: flag is array type inside json
#Expected result: return error

select regex_like("11,222,33","1122233",p.info.id.stats1) from playerinfo p where id1=1