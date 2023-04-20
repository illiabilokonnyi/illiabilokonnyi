-- dynamic pivot
select src.IssuerId, src.FieldDate, pvt.*
from
(
    select IssuerId,
           FieldValue,
           FieldName,
           FieldDate
    from IssuerReference
) src
pivot
(
    max(FieldValue) for FieldName in (select distinct FieldName from IssuerReference)
) pvt;
