select id, u.otherNames.first
from users u
where substring(u.otherNames.first, 1, 2) = "re"
