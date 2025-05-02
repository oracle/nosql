#TestDescription: source is integer type
#Expected result: return error

select regex_like(p.age,"30") from playerinfo p