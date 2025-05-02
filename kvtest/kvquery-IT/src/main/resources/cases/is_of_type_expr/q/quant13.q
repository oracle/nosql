select  id, f.arr[] IS OF TYPE ( integer* ) as typeof
from Foo f
order by id
