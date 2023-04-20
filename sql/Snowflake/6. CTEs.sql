-- query fragmentation
with sm as (

    select CompanyId,
		   CompanyName,
		   ID_ISIN
    from securityMaster
    qualify dense_rank() over (order by SOURCE_DATE desc nulls last) = '1'

), issuers as (

	select IssuerId,
		   IssuerName,
		   ISIN
	from issuerReference
	qualify row_number() over (partition by ISIN order by AsOfDate desc) = '1'

), scores as (

	select *
	from EsgScores
	qualify row_number() over (partition by IssuerId order by AsOfDate desc) = '1'

)

select sm.CompanyId,
	   sm.CompanyName,
	   scores.* exclude IssuerId
from sm
	join issuers
		on sm.ID_ISIN = issuers.ISIN
	join scores
		on issuers.IssuerId = scores.IssuerId;
