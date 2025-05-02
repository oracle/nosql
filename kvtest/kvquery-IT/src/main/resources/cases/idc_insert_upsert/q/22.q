#TestDescription: expression includes value comparison operators for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 54,
 "value comparision operator",
 34,
 $runs >= 9000
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=54