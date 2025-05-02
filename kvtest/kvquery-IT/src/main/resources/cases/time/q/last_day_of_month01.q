select to_last_day_of_month(t0) as last_day_t0,
       to_last_day_of_month(t3) as last_day_t3,
       to_last_day_of_month(l3) as last_day_l3,
       to_last_day_of_month(t.doc.l3) as last_day_jl3,
       to_last_day_of_month(t.doc.s6) as last_day_js6
from roundtest t
