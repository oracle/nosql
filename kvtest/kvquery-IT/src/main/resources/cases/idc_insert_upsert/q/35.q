#TestDescription: expression includes map path expression returning a single item
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 65,
 "map path expression returning a single item",
 $test3.person.expenses.food,
 $test3.person.expenses.books>any10
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=65