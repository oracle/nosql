select f.record.int
from foo f
group by f.record.int, f.record.double
