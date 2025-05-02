select id,
       year(modification_time($f)) >= 2020
from boo $f
where $f.address.state = "MA"
