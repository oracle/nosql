# Run query and set $ext0 = "ab"
declare $ext6 string;
select pk from bar where pk >= $ext6 and pk <= "ab"
