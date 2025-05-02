#Using Math func on complex data types in Json Collection table

select trunc(abs(t.complex.arr),7),
       sign(t.complex.map.b)
from jsonCollection_test t where id =1