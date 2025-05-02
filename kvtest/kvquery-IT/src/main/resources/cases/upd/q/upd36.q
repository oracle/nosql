update Bar b
remove b.array[$element.ai=1],
remove b.record.array1[$element.ras='ras1'],
remove b.record.map1.keys($key='rec1')
where id = 20
returning b.array as a, b.record.array1 as ra1, b.record.map1 as rm1
