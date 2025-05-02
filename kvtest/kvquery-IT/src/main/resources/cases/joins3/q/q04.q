#
# For each message of size > 1MB, return the id of the message,
# the name of the user who sent or received that message, and
# the folder path.
#
select m.uid, m.fid, m.mid, u.name, [ f.name, f.ancestors.name ]
from nested tables (
     users.folders.messages m
     ancestors(users u,
               users.folders f))
where size > 1000
