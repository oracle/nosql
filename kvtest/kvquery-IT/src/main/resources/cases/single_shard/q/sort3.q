declare $age integer;
        $id1 integer;
select id1, id2, id3, id4, age
from Foo
where id1 = $id1 and id2 = 4 and age > $age
order by age
