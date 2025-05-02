select id
from foo f
where f.info.children.Anna.age > 10 and
      f.info.children.John.school = "sch_1"
