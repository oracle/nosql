select /*+ FORCE_INDEX(Foo idx_state_areacode_age) */ id
from foo f
where exists f.info.address
  [
    $element.phones.areacode = 408 and
    ($element.state = "CA" or not exists $element.state)
  ]
