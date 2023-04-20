-- distinct records
select distinct CompanyId, ISIN
from securityMaster as sm;

-- excluding / renaming columns
select scores.* exclude ("lineage_hash","tenant_id","tracking_id","AS_OF_DATE")
from EsgScores scores;

select sm.* rename (CompanyId as IssuerId, ID_ISIN as ISIN)
from securityMaster as sm;

-- filtering results
select *
from EsgScores
where AS_OF_DATE between '2023-01-01' and '2023-06-30'
	and CompanyId = '68285969';
	
-- limiting results
select CompanyId, TotalCarbonEmissions
from EsgScores
order by TotalCarbonEmissions desc nulls last
limit 100 offset 100;

-- casing
select CompanyId,
	   TotalCarbonEmissions,
	   case
		when TotalCarbonEmissions is null then 'missing data'
		else null
	   end as Errors
from EsgScores;
