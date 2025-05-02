insert into viewers
       (acct_id, user_id, "a with space", b)
       values(8,
              1,
              10,
              7)
returning *
