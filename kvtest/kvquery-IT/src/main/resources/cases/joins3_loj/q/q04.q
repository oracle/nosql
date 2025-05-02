#
# For each message of size > 1MB, return the id of the message,
# the name of the user who sent or received that message, and
# the folder path.
#
select m.uid, m.fid, m.mid, u.name, [ f.name, f.ancestors.name ]
from users.folders.messages m
    left outer join users u on m.uid = u.uid
    left outer join users.folders f on m.uid = f.uid and m.fid = f.fid
where size > 1000
