#TestDescription: expression includes array path expression returning a single item
#Expected result: insert successful

insert into playerinformation (id,id1,name,tier1rated) values 
(
 100,
 64,
 "array path expression returning a single item",
 $test1.person.phones.number >any 31
)

select id,id1,name,tier1rated
from playerinformation 
where id1=64