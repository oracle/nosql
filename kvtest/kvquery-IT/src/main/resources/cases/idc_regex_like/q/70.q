#TestDescription: Evil regex should result in error.
#Expected result: return error

select regex_like(name,"(.*a){100}") from playerinfo where id1=3