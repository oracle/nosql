select id
from foo f
where f.record.long is null and exists f.info or 
      f.record.int is not null and f.info.address.city = "Portland"
