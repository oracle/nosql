#TestDescription: expression includes boolean operators for boolean column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,tier1rated) values 
(
 100,
 43,
 "logical operator",
 34,
 $ballsbowled>9000 AND $ballsplayed>$ballsbowled OR $strikerate>95.00
)

select id,id1,name,age,tier1rated
from playerinformation 
where id1=43