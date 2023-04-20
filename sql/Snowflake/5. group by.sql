-- ambiguous reference check
select SEDOL, ExchangeCode, count(CompanyId) cnt
from (
	select distinct CompanyId, SEDOL, ExchangeCode
    from securityMaster
	where SEDOL is not null and ExchangeCode is not null
) sq
group by SEDOL, ExchangeCode
having cnt != 1
order by cnt desc;
