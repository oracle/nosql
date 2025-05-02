select distinct (f.info.bar1 is of type (string)),f.info.bar2 from fooNew f where f.info.bar1<=7 order by (f.info.bar1 is of type (string)),f.info.bar2
