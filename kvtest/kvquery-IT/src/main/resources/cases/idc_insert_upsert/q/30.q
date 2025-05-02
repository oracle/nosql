#TestDescription: expression includes cast expression
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 51,
 "cast expression",
 cast($century as integer),
 $ballsbowled is of type (string)
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=51