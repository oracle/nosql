select (expiration_time_millis($f)) / (1000 * 3600) from jsoncol $f where majorKey1 = "hello" and majorKey2 = "helloo" and minorKey= "hellooo"
