select id
from Foo f
where f.children.Mark.age is null and f.children.keys() =any "Anna"
