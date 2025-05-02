select id, version($f) = row_version($f)
from foo $f
