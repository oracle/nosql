#TestDescription: expression includes sequence comparison operators for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 55,
 "sequence comparision operator",
 34,
 $last5mat.runs[] >=any 50
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=55