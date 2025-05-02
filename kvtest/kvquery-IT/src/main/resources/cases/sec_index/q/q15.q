select /*+ FORCE_INDEX(keyOnly first) */ *
from keyOnly
where firstName > "first1"
