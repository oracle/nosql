select id
from foo f
where exists f.info.address.phones[].areacode and
      exists f.info.address.state
