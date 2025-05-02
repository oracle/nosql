#TestDescription: expression includes exists operator for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 46,
 "exists operator",
 34,
 exists $last5mat.runs[10 < $element] and
 exists $last5mat.runs[$element < 100]
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=46