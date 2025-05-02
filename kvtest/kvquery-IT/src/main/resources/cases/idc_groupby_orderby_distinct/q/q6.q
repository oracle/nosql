select f.info.bar1, sum(f.info.bar3 as string) from fooNew f group by f.info.bar1
