#
# NOT operator
# EXISTS operator
#
select id
from Foo f
where not (f.info.address.city = "Salem" and
           f.info.children.keys() =any "Anna") or
      (exists f.info.address.phones[7] and
       not f.info.address.phones[7].areacode <= 600)
