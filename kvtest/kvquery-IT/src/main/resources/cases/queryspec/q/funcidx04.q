select id, u.otherNames.first
from users u
where exists u.otherNames[substring($element.first, 1, 2) in ("re", "ir", "ai") and
                          length($element.last) > 3]
