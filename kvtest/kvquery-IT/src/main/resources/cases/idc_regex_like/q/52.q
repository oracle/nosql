#TestDescription: pattern is of type number
#Expected result: return error

select regex_like(p.name,p.ballsplayed) from playerinfo p where id1=1