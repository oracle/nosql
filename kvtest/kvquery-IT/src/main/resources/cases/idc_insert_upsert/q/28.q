#TestDescription: expression includes function call
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 49,
 "function call",
 size($last5mat.runs),
true
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=49