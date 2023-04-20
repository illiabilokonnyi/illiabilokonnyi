-- join multiple tables
select distinct sm.CompanyId,
				scores.*
from securityMaster sm
	join IssuerReference issuers
		on sm.ID_ISIN = issuers.ISIN
	join EsgScores scores
		on scores.IssuerId = issuers.IssuerId;

-- join on multiple conditions
select distinct sm.CompanyId,
				issuers.IssuerId
from securityMaster sm
	join IssuerReference issuers
		on sm.ID_ISIN = issuers.ISIN
			or (sm.ID_CUSIP = issuers.CUSIP and sm.ExchCode = issuers.ExchangeCd)
			or (sm.ID_SEDOL1 = issuers.SEDOL and sm.ExchCode = issuers.ExchangeCd);

-- join with analytic
select distinct sm.CompanyId,
				sm.CompanyName
from securityMaster sm
	join securityMasterDetails details
	 on sm.CompanyId = coalesce(details.ParentCoId, details.CompanyId);

-- self join
select child.CompanyId,
	   child.CompanyName,
	   parent.CompanyName as ParentCompanyName
from securityMaster child
	left join securityMaster parent
		on child.ParentCoId = parent.CompanyId;

-- value overrides
select issuers.IssuerName,
	   sq.*
from (
	select issuers.ISIN, coalesce(override.toIssuerId, issuers.IssuerId) IssuerId
	from IssuerReference issuers
		left join SoiOverride override
			on issuers.IssuerId = override.fromIssuerId
) sq
left join IssuerReference issuers
	on issuers.IssuerId = sq.IssuerId;
