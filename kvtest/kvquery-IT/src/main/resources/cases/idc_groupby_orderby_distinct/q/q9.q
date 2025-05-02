select case when f.info.bar1 is of type (number) then cast(f.info.bar1 as double)
else f.info.bar1
end as b,
5 <sum(f.info.bar2) and sum(f.info.bar2) <= 23.3 as sum, count(*) as cnt from fooNew f group by f.info.bar1

