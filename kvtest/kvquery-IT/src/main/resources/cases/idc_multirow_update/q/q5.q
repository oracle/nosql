#
# Update fail:
# RETURNING clause is not supported unless the complete primary key is
# specified in the WHERE clause.
#

update users
set age = age + 1
where sid1 = 0 and sid2 = 1 and pid1 = 0
returning *
