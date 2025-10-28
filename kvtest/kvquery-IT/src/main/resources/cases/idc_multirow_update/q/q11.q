#
# Update fail:
# Some row fails, while others succeed
#

update users u
set u.info.height = u.info.height + 1
where sid1 = 2 and sid2 = 3

