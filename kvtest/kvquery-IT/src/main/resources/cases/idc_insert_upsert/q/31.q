#TestDescription: expression does not return any result
#Expected result: null insertion

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 52,
 "null result",
 $runs.test,
 true
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=52