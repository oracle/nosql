select /*+ FORCE_INDEX(bar2 idx_year_month) */ id, b.tmarr[year($element) = 2019] as elements_with_year_2019, b.tmarr[month($element) > 1] as elements_with_month_greater_than_1_from_arrays_where_at_least_one_element_has_year_2019
from bar2 b
where exists b.tmarr[year($element) = 2019] and exists b.tmarr[month($element) > 1]
