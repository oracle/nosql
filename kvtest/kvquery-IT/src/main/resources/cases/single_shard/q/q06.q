declare $id2 integer;
        $age integer;
select id1, id2, id3, id4, age
from Foo
where id1 = 3 and id2 = $id2 and age > $age
