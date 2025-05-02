select id, tm
from foo
where cast (tm as long) > current_time_millis() - 5000
