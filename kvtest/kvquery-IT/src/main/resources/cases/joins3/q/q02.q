#
# For user 10, return his name and all of messages
# received by him today.
#
select name, sender
from nested tables (
     users u
     descendants(users.folders.messages m ON
                 m.receiver = u.email and
                 time >= cast("2017-12-12T00:00:00.0" as timestamp(3))))
where u.uid = 10
