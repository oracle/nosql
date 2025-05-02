#TestDescription: expression includes map path expression returning more than one item
#Expected result: insert failure

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 66,
 "map path expression returning more than one item",
 $test3.person.expenses.values(),
 $test3.person.expenses.books
)