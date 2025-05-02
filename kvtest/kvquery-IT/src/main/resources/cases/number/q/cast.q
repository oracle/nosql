SELECT
    id,
    num,
    cast ( num as integer ),
    cast ( num as long ),
    cast ( num as float )/10,
    cast ( num as double )/10,
    cast ( num as number ),
    cast ( 0 as number ),
    cast ( 1.2 as number)
FROM NumTable
