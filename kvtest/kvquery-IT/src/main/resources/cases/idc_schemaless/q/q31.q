select majorKey1 from jsoncol $f where partition($f) < 15 order by majorKey1
