#TestDescription: expression includes external variables
#Expected result: insert successful

declare $name string;
        $runs long;
insert into playerinformation (id,id1,name,age) values 
(
 100,
 53,
 $name,
 $runs
)

select *
from playerinformation 
where id1=53