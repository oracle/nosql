#TestDescription: source is boolean type
#Expected result: return error

select regex_like(p.tier1rated,"true") from playerinfo p