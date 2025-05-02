delete from foo f
where f.info.lastName > "last5" and f.info.lastName <= "last10"
returning id
