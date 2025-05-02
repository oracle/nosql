insert into viewers (acct_id, user_id, a, b)
values(3, 1,
       [10, 11, 12],
       { "c" : "xyz", "d" : { "e" : [1]}}
      )
returning *
