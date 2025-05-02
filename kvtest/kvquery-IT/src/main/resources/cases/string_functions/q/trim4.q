select id, str1, trim(str1, "BOTH", " "), str2, trim(str2, "leading", "-"),
  str3, trim(str3,"Trailing", "0") from stringsTable ORDER BY id