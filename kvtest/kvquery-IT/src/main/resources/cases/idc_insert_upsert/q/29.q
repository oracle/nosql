#TestDescription: expression includes case expression
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,bin) values 
(
 100,
 50,
 "case expr",
 34,
 case when $catches = 18 then $name end
)

select id,id1,name,age,bin
from playerinformation 
where id1=50