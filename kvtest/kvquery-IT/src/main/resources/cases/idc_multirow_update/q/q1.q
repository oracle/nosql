#
# Update fail:
# Full shard key must be provided for update
#

update users
set name = upper(name),
set age = age + 1
where sid1 = 0