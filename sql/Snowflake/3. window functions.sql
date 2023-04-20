-- results from latest update
select *
from securityMaster
qualify dense_rank() over (order by "UPDATE_DATE" desc nulls last) = '1';

-- filtering out duplicated records
select *
from securityMaster
qualify row_number() over (partition by CompanyId, ISIN, CUSIP, SEDOL order by "created_at_stamp" desc) = '1';

-- ambiguous reference filtering on fuzzy logic
select sm.CompanyId,
	   issuers.IssuerId
from securityMaster sm
	join issuerReference issuers
qualify row_number() over (partition by sm.CompanyId order by JAROWINKLER_SIMILARITY(sm.CompanyName, issuers.IssuerName) desc nulls last) = '1';
