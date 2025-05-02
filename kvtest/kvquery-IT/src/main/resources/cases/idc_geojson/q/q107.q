#geo_near: Returns false if any of the first two operands returns 0 or more than 1 items.

#select id,
#p.info.point
#from points p
#where geo_near(null,{"type": "point", "coordinates": [ 79.85287606716155, 6.934201673601963]},200000)

select * from points
order by id