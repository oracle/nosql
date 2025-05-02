#TestDescription: Evil regex should result in error.
#Expected result: return error

select regex_like(name,"^([a-zA-Z0-9])(([\\-.]|[_]+)?([a-zA-Z0-9]+))*(@){1}[a-z0-9]+[.]{1}(([a-z]{2,3})|([a-z]{2,3}[.]{1}[a-z]{2,3}))$") from playerinfo where id1=3