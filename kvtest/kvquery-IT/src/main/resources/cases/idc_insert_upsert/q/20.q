#TestDescription: expression includes arithmetic expression for numeric column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,avg) values (
1*100,
2*22,
"Arithmetic Expression",
30+25+4+1-30,
100*12,
98*123,
12+3.5-000.0008,
$runs/$matches
)
select id,id1,name,age,ballsbowled,ballsplayed,strikerate,avg
from playerinformation
where id1=44