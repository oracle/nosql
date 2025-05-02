update foo $f
json merge $f with patch { "id" : 10 }
where id = 1
