#
# Array Filter:
# Input: array, jnull, map
# $element : map, numeric, jnull
#
# Map Filter:
# Input: record
#
# Conditional array constructor
#
select 
  case
  when id < 3 then f.record.values($key = "int" or $key = "string")
  else f.info.address.phones[50 < $element.number and $element.number < 60 or
                             $element.number = 80] 
  end
from foo f
