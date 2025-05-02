insert into jsoncol $j values ("ab", "cd", "min6", { "a" : 3, "b" : 5 })
returning $j, remaining_days($j)
