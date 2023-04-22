-- updating records
update xattribute_class SET name = 'CUSIP' where id = 'c8070568-a707-4927-8d33-b62bf495109d';
update xattribute_class SET name = 'ISIN' where physicalName = 'ID_ISIN';

-- deleting records
delete from xattribute_class where id = 'c8070568-a707-4927-8d33-b62bf495109d';
