#TestDescription: Evil regex should result in error.
#Expected result: return error

select regex_like(name,"^(([a-z])+.)+[A-Z]([a-z])+$") from playerinfo where id1=3