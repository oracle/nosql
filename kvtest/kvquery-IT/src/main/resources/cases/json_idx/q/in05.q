#
# TODO: find a way to push all preds as filtering preds.
#
select /*+ FORCE_INDEX(Foo idx_children_both) */
       id
from foo f
where f.info.children.values(ltrim($key) in ("Anna", "Mark") and
                             $value.age + 1 in (3, 5, 10)).school <=any "sch_1"
