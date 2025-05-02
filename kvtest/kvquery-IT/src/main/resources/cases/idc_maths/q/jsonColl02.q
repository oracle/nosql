#Using Math func on JSON collection on fields that are null

select sign(t.numbers.num5),
       trunc(sin(t.address.otherPhone),7),
       trunc(asin(t.city),7)
from jsonCollection_test t where id =1