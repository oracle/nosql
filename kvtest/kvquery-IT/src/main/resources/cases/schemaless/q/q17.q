select $jc, remaining_days($jc) as rem_days
from jsoncol $jc
where $jc.address.name = "rupali"
