
select /* FORCE_INDEX(Foo idx_children_both) */id
from foo f
where f.info.children.values($key = "Anna" and $value.age <= 10).school <=any "sch_1"
