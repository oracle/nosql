#
# NOT operator
#
select id
from Foo f
where not f.info.address.city != "Salem" and
      f.info.children.keys() =any "Anna" or
      not f.info.address.phones[0].areacode > 200
