select /*+ FORCE_INDEX(Foo idx_children_values) */id
from foo f
where f.info.children.values($key in ("Anna", "Mark") and
                             $value.age in (3, 5, 10)).school <=any "sch_1"
