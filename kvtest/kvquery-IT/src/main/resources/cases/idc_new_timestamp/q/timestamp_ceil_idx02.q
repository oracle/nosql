select id, timestamp_ceil(t0) as t0_to_quarter
from roundFunc
where timestamp_ceil(t0,'quarter') >= '2021-10-01'