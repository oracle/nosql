select id1
from Foo f
where [1.0, 2.0, 3.0, 5.5, 4.0, 15.0, 16.3, 23.4, 1.3, 4.0, 12.4, 30.0][] =any f.arr[]
