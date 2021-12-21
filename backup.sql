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

