#TestDescription: expression includes is null / is not null operator for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 45,
 "is null / is not null operator",
 34,
 $dblcentury is null and $century is not null
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=45