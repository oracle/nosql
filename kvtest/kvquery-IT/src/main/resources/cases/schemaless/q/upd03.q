update viewers v
set v.firstName = "Manolo",
remove v.new,
remove v.notExists
where acct_id = 100 and user_id = 2
returning *
