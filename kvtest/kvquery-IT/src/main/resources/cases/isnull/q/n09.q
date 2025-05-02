select id
from Foo f
where f.children.keys() =any "Anna" and f.children.Mark.age is null
