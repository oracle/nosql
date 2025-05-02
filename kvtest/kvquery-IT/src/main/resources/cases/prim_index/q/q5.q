#
# primary key gap
#
select id1, id2, id4, age
from Foo
where id2 = 30 and id1 = 3 and id4 = "id4-3"
