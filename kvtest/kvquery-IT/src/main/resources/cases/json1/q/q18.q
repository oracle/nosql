#
# NOT operator
#
select id
from Foo f
where not f.info.address.city != "Salem" and
      not f.info.children.keys() =any "John" or
      not f.info.address.phones[0].areacode > 200

