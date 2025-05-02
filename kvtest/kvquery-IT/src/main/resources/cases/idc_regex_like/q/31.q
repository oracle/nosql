#TestDescription: source is float type
#Expected result: return error
# ????

select regex_like(p.avg,"48.0") from playerinfo p
