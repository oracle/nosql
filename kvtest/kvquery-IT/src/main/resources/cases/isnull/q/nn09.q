select id
from Foo f
where f.children.keys() =any "Mark" and f.children.Anna.age is not null
