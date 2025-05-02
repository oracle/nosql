select id1
from Foo
where Foo.map.key1[] >any Foo.map.key2[]
