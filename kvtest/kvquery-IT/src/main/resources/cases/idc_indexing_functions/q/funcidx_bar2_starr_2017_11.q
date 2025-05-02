select /*+ FORCE_INDEX(bar2 idx_starr) */ id, b.starr[substring($element, 0, 4) = "2017"] as elements_with_year_2017, b.starr[substring($element, 5, 2) = "11"] as elements_with_month_11_from_arrays_where_at_least_one_element_has_year_2017
from bar2 b
where exists b.starr[substring($element, 0, 4) = "2017"] and exists b.starr[substring($element, 5, 2) = "11"]
