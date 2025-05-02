select id, timestamp_round(t0,'year') as t0_to_year
from roundFunc
where timestamp_round(t0,'year') =any '2022-01-01T00:00:00'