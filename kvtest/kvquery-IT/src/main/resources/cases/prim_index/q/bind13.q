declare
$long LONG; // 923424293492870982

select id1, id2, id4
from Foo
where $long = id1 and id2 = 3 and id3 = "tok1" and id4 = "id4-1"
