insert into viewers
       (acct_id, user_id, a, b)
       values(2,
              1,
              10,
              cast("2023-05-14T01:01:01" as timestamp(0)))
returning *
