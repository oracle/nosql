select /*+ FORCE_INDEX(foo idx_children_both) */id
from foo f
where exists f.info[$element.children.Anna.age = 9 and $element.children.Mark.school = "sch_1"]
