-- test cases with temp tables
with securityMaster as (

    SELECT Column1 testCase,
           Column2 success,
        Column3 CompanyId,
       Column4  ISIN,
       Column5 CUSIP,
       Column6 AsOfDate,
       Column7 created_at_stamp
    FROM (VALUES
                 (1, true, 'id1', 'isin1', 'cusip1', 20230101, 2),
                 (2, true, 'id2', 'isin2', 'cusip2', 20230101, 3),
                 (3, true, 'id3', 'isin3', 'cusip3', 20230101, 4),
                 (4, true, 'id3', 'isin3', 'cusip4', 20230101, 5),
                 (5, true, 'id4', null, 'cusip6', 20230101, 7),
                 (6, false, 'id2', 'isin1', 'cusip2', 20200503, 9), -- date overwrite
                 (7, false, 'id3', 'isin1', 'cusip1', 20230101, 1) -- stamp overwrite
        ) as V

)

select *
from
    (select *
    from securityMaster
    qualify dense_rank() over (order by AsOfDate desc) = '1')
qualify row_number() over (partition by ISIN, CUSIP order by created_at_stamp desc) = '1'
order by testCase
