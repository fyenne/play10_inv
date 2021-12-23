#
SELECT  *
FROM dsc_dwd.dwd_wh_dsc_inventory_dtl_di
WHERE src = 'scale'
AND ou_code IN ( 'HPPXXWHWDS', 'MICHETCTGS' 'COSTASHHTS', 'ZEBRASHALS' )
AND inc_day IN """+ str(fridays) + """
AND usage_flag




show create table dsc_dws.dws_qty_working_hour_labeling_sum_df


drop table if exists tmp_dsc_dws.dws_dsc_wh_fifo_alert_wi

CREATE EXTERNAL TABLE `tmp_dsc_dws.dws_dsc_wh_fifo_alert_wi`(
`week_0` double comment '',
`week_1` double comment '',
`week_2` double comment '',
`week_3` double comment '',
`week_4` double comment '',
`week_5` double comment '',
`week_6` double comment '',
`week_7` double comment '',
`week_8` double comment '',
`received_date` string comment '',
`sku` string comment '',
`mark` string comment '',
`lock_codes` string comment '',
`wms_warehouse_id` string comment '',
`fifo_fefo` string comment '',
`location` string comment '',
`ou_code` string comment '',
`start_of_week` string comment '')
COMMENT '周增,fifo异常管理'
PARTITIONED BY (
`inc_day` string COMMENT '日分区')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'





inbound_header	HK	FRED1M041S	FRED1M041S·Fred Perry	scale	2021-12-17	2021-12-09	1
inbound_header	TSC	FUJIXSYXXS	FUJIXSYXXS·Fuji	scale	2021-12-03	2021-09-23	1
inbound_header	CR	RAZERSHSJS	RAZERSHSJS·Razer	scale	2021-07-12	-	1
inbound_line	CR	DIADOXMNSS	DIADOXMNSS·Diadora	scale	2021-12-20	2021-12-17	1
inbound_line	HK	FRED1M041S	FRED1M041S·Fred Perry	scale	2021-12-17	2021-12-09	1
inbound_line	TSC	FUJIXSYXXS	FUJIXSYXXS·Fuji	scale	2021-12-03	2021-09-23	1
inbound_line	CR	RAZERSHSJS	RAZERSHSJS·Razer	scale	2021-07-12	-	1
inventory	CM	APPLESHWHW	APPLESHWHW·Apple	scale	2021-12-17	-	1
inventory	TSC	BOSEXSHKQS	BOSEXSHKQS·Bose	scale	2021-12-17	-	1
inventory	CR	DIADOXMNSS	DIADOXMNSS·Diadora	scale	2021-12-17	-	1
inventory	CR	FERRECDXXS	FERRECDXXS·Ferrero	scale	2021-12-17	-	1
inventory	HK	FRED1M041S	FRED1M041S·Fred Perry	scale	2021-12-17	-	1
inventory	TSC	FUJIXSYXXS	FUJIXSYXXS·Fuji	scale	2021-12-17	-	1
inventory	TSC	HPPXXSHMGS	HPPXXSHMGS·HP Supply Chain	scale	2021-12-17	-	1
inventory	TSC	HPPXXWHWDS	HPPXXWHWDS·HP Supply Chain	scale	2021-12-17	-	1
inventory	EM	HUSQVSHMFS	HUSQVSHMFS·Husqvarna	scale	2021-12-17	-	1
inventory	Auto	MICHESHXCS	MICHESHXCS·Michelin	scale	2021-12-17	-	1
inventory	Auto	MICHETCTGS	MICHETCTGS·Michelin	scale	2021-12-17	-	1
inventory	CR	PERNOSHFSS	PERNOSHFSS·Pernod Ricard	scale	2021-12-17	-	1
inventory	CR	RAZERSHSJS	RAZERSHSJS·Razer	scale	2021-12-17	-	1
inventory	HK	REVL1M111S	REVL1M111S·Revlon (Hong Kong)	scale	2021-12-17	-	1
inventory	LSH	SQUIBSHHTS	SQUIBSHHTS·Squibb	scale	2021-12-17	-	1
inventory	TSC	ZEBRASHALS	ZEBRASHALS·Zebra	scale	2021-12-17	-	1
outbound_header	TSC	FUJIXSYXXS	FUJIXSYXXS·Fuji	scale	2021-12-03	2021-12-02	1
outbound_header	CR	RAZERSHSJS	RAZERSHSJS·Razer	scale	2021-08-16	2021-07-22	1
working_hours	CM	APPLESHWHW	APPLESHWHW·Apple	scale	2021-12-21	-	1
working_hours	TSC	BOSEXSHKQS	BOSEXSHKQS·Bose	scale	2021-12-21	-	1
working_hours	CR	DIADOXMNSS	DIADOXMNSS·Diadora	scale	2021-12-21	-	1
working_hours	CR	FERRECDXXS	FERRECDXXS·Ferrero	scale	2021-12-21	-	1
working_hours	HK	FRED1M041S	FRED1M041S·Fred Perry	scale	2021-12-21	-	1
working_hours	TSC	FUJIXSYXXS	FUJIXSYXXS·Fuji	scale	2021-12-21	-	1
working_hours	TSC	HPPXXSHMGS	HPPXXSHMGS·HP Supply Chain	scale	2021-12-21	-	1
working_hours	TSC	HPPXXWHWDS	HPPXXWHWDS·HP Supply Chain	scale	2021-12-21	-	1
working_hours	EM	HUSQVSHMFS	HUSQVSHMFS·Husqvarna	scale	2021-12-21	-	1
working_hours	Auto	MICHESHXCS	MICHESHXCS·Michelin	scale	2021-12-21	-	1
working_hours	Auto	MICHETCTGS	MICHETCTGS·Michelin	scale	2021-12-21	-	1
working_hours	CR	PERNOSHFSS	PERNOSHFSS·Pernod Ricard	scale	2021-12-21	-	1
working_hours	CR	RAZERSHSJS	RAZERSHSJS·Razer	scale	-	-	1
working_hours	HK	REVL1M111S	REVL1M111S·Revlon (Hong Kong)	scale	2021-12-21	-	1
working_hours	LSH	SQUIBSHHTS	SQUIBSHHTS·Squibb	scale	2021-12-21	-	1
working_hours	TSC	ZEBRASHALS	ZEBRASHALS·Zebra	scale	2021-12-21	-	1



 
	'DIADOXMNSS',  
	'APPLESHWHW',
	'BOSEXSHKQS', 
	'FERRECDXXS',
	'FRED1M041S',
	'FUJIXSYXXS',
	'HPPXXSHMGS',
	'HPPXXWHWDS',
	'HUSQVSHMFS',
	'MICHESHXCS',
	'MICHETCTGS',
	'PERNOSHFSS', 
	'REVL1M111S',
	'SQUIBSHHTS',
	'ZEBRASHALS'





	
select 
ou_code,
wms_warehouse_id,
sku,
case when fifo_fefo = 'fifo'
then 'received_date'
else 'expired_date' 
end as date_type,
min(received_date) as remained_sku_date,
max(received_date) as distributed_sku_date,
array_agg(location) as location ,
min(start_of_week) as min_date
from  tmp_dsc_dws.dws_dsc_wh_fifo_alert_wi
where mark != 'clear'
group by 
sku, ou_code, fifo_fefo, wms_warehouse_id
order by 	
sku, ou_code
 