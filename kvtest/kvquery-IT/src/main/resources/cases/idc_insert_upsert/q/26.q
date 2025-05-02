#TestDescription: expression includes is of type operator for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 47,
 "is of type operator",
 34,
 $ballsbowled is of type (string)
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=47