#
# Update should fail:
#   Invalid type for JSON index field: info.code. Type is STRING, expected type
#   is INTEGER value = unknown
#
update users t 
put t.info {"code":t.info.address.zipcode} 
where sid = 1