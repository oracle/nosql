#
# complete primary key, with no remaining preds
#
select id1, id2, id4
from foo
where id2 = 30 and id1 = 3 and id4 = "id4-3" and id3 = "tok0"
