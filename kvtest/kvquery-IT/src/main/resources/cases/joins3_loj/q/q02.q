#
# For user 10, return his name and all of messages
# received by him today.
#
select name, sender
from users u left outer join users.folders.messages m ON
                 u.uid = m.uid and m.receiver = u.email and
                 time >= cast("2017-12-12T00:00:00.0" as timestamp(3))
where u.uid = 10
