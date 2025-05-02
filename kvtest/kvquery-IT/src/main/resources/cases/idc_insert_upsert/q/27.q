#TestDescription: expression includes string expression for string column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 48,
 cast ( true as string ),
 34,
 $ballsbowled is of type (string)
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=48