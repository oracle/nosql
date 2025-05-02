#TestDescription: pattern is number type inside json
#Expected result: return error

select regex_like("5266232323232323",p.info.id.ballsbowled) from playerinfo p