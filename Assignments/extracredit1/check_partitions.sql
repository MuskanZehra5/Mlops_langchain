SELECT 
    id,
    value,
    $PARTITION.pf_T1(value) AS partition_number,
    'T1' AS table_name
FROM T1
ORDER BY value;


SELECT 
    id,
    value,
    $PARTITION.pf_T2(value) AS partition_number,
    'T2' AS table_name
FROM T2
ORDER BY value;
