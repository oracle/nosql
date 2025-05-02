select id
from Foo f
where f.info[].address[].phones[].areacode =any 450 and
      f.info.address.phones.kind =any "work"
