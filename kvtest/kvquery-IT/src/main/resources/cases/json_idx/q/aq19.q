select id
from foo f
where f.info.children.Anna.friends[] =any "Bobby"
