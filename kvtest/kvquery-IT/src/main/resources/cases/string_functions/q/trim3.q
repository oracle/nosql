select id, str1, trim(str1, "NOT A VALID VALUE"), trim(str1, "both", "a longer
than 1 string") from stringsTable ORDER BY id