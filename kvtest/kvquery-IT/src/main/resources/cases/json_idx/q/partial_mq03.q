#
# TODO: recognize that the 2 queries below are equivalent:
#
# select id
# from foo f
# where f.info.children.Anna.school = "sch_1" and f.info.children.Anna.age <=10
#
select /*+ FORCE_INDEX(Foo idx_children_values) */id
from foo f
where f.info.children.values($key = "Anna" and $value.school = "sch_1").age <=any 10
