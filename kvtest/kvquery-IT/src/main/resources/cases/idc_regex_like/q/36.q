#TestDescription: source is of timestamp type
#Expected result: return error

select regex_like(p.time,"1987-04-30T07:45:00Z") from playerinfo p
