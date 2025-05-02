select id, tm
from foo
where tm > cast (current_time_millis() - 5000 as timestamp(0))
