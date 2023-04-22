-- substitue columns and limit
select ${COLUMNS}
from securityMaster
#if (${LIMIT}) limit ${LIMIT}#end;

-- substitue filter values
select *
from securityMaster
where true
#if ( ${ISIN} )
    AND "ID_ISIN" IN (#foreach( ${isins} in ${ISIN} )'${isins}'#if( $foreach.hasNext ),#end#end)
#end;

-- conditional flow
#if (${OVERRIDE_ID})
	with override as (
		select *
		from SoiOverride
		where "tracking_id" = '${OVERRIDE_ID}'
	), issuers as (
		select refdata.ISIN,
			   coalesce(override."toId", refdata.IssuerId) as IssuerId
		from refdata
		left join override
			on refdata.EntityId = override."fromId"
#else
	), issuers as (
		select ISIN, IssuerId from refdata
#end
)

select *
from issuers;
