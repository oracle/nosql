#TestDescription: source is long type
#Expected result: return error

select regex_like(p.ballsbowled,"5266") from playerinfo p