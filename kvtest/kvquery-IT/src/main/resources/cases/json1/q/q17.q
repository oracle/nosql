#
# NOT operator
#
select id, 1
from Foo f
where not f.info.address.city = "Salem" and not f.info.children.keys() =any "John"

