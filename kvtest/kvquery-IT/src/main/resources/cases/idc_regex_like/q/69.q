#TestDescription: Evil regex should result in error.
#Expected result: return error

select regex_like(name,"(a|a?)+") from playerinfo where id1=3