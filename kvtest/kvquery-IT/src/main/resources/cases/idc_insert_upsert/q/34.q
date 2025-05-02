#TestDescription: expression includes array path expression returning more than one item
#Expected result: insert failure

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 65,
 "array path expression returning more than one item",
 $test1.person.phones.areacode,
 $test1.person.phones.areacode >=any $test2.person.phones.areacode
)

select id,id1,name,tier1rated
from playerinformation 
where id1=65