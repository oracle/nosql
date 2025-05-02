select id
from foo f
where f.info.children.Anna.values($value.school = "sch_1").age <=10
