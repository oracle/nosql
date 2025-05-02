select f.mixed.a,
       min(case
             when f.mixed.x is of type (number) then f.mixed.x
             else seq_concat()
           end) as min
from Foo f
group by f.mixed.a
