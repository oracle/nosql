#
# Update fail:
# Invaild type for JSON index field
#

update users t
put t.info {"code":t.info.address.zipcode}
where sid1 = 3 and sid2 = 4