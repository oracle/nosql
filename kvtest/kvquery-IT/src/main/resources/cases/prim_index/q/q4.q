#
# complete primary key
# covering index
#
select id1, id2, id4
from Foo
where id2 = 30.0 and id1 = 3 and id1 > 0 and id2 < -+-5E1 and
      id4 = "id4-3" and id3 = 'tok0'
