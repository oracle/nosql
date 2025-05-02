select id
from foo f
where exists f.info.addresses.phones.values().values($key = "phone6" and
                                                     $value.areacode = 650 and
                                                     $value.number > 30)
