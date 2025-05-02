select id1
from Foo f
where f.arr[] !=any [1.0, 2.0, 3.0][]
