select /*+ FORCE_PRIMARY_INDEX(foo) */ id
from foo t
where t.address.state = "MA" and t.address.city between "C" and "T"
