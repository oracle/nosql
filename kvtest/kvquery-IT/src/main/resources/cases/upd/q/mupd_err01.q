#
# Update fail:
# RETURNING clause is not supported unless the complete primary key is
# specified in the WHERE clause.
#
update users
set age = age + 1
where sid = 0
returning *
