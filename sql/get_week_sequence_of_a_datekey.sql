with cte_calendar_1 as
(
    select datekey, datevalue,
    case when datevalue between start_Date and end_date then fiscal_year end as fiscal_year,
    ceil((DATEDIFF(day,start_Date, datevalue+1)) / 7) AS fiscal_week,
    ceil((DATEDIFF(day,'2018-09-29', datevalue+1)) / 7) AS fiscal_week_sequence
    from (select datekey, datevalue from TIME_DIMENSION.DIMDATE_V where datekey between 44834 and 45561) DD
    cross join 
    (
        SELECT
            '2022-10-01' AS start_date,
            '2023-09-29' AS end_date,
            2023 as fiscal_year
        UNION ALL
        SELECT
            '2023-09-30' AS start_date,
            '2024-09-27' AS end_date,
            2024 as fiscal_year
    ) fy
    where datevalue between start_Date and end_date
)
select fiscal_week_sequence
from cte_calendar_1
where datekey = {datekey}
;