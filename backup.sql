#
SELECT  *
FROM dsc_dwd.dwd_wh_dsc_inventory_dtl_di
WHERE src = 'scale'
AND ou_code IN ( 'HPPXXWHWDS,
MICHETCTGS' 'COSTASHHTS,
ZEBRASHALS' )
AND inc_day IN """+ str(fridays) + """
AND usage_flag

 

drop table if exists tmp_dsc_dws.dws_dsc_wh_fifo_alert_wi

CREATE TABLE `dm_dsc_ads.ads_dsc_wh_fifo_alert_wi_dtl`(
  `week_0` double COMMENT '快照week_0',
  `week_1` double COMMENT '快照week_1',
  `week_2` double COMMENT '快照week_2',
  `week_3` double COMMENT '快照week_3',
  `week_4` double COMMENT '快照week_4',
  `week_5` double COMMENT '快照week_5',
  `week_6` double COMMENT '快照week_6',
  `week_7` double COMMENT '快照week_7',
  `received_date` string COMMENT '对应sku的收货时间',
  `sku` string COMMENT 'sku',
  `mark` string COMMENT '清仓标记',
  `lock_codes` string COMMENT '锁货标记',
  `wms_warehouse_id` string COMMENT '仓库名称',
  `fifo_fefo` string COMMENT 'fife还是fifo',
  `location` string COMMENT '库位',
  `ou_code` string COMMENT 'ou_code',
  `start_of_week` string COMMENT '第一周日期',
  `end_of_week` string COMMENT '最后周日期'
) COMMENT '周增,fifo异常管理 2week' 
PARTITIONED BY (`inc_day` string COMMENT '日分区', 
    `weeksize` string comment 'weeksize分区') 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS 
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'



CREATE TABLE `dm_dsc_ads.ads_dsc_wh_fifo_alert_wi_sum`(
`ou_code` string comment 'oucode',
`wms_warehouse_id` string comment '仓库名',
`fifo_fefo` string comment '是fifo还是fefo',
`start_of_week` string comment '开始的周日期',
`end_of_week` string comment '结束的周日期',
`sku` string comment 'sku',
`old_remain_amt` double comment '应该发的货/ 但是没发/ 剩余数量',
`old_remain_location` string comment '应该发的货/ 但是没发/ 库位',
`old_remain_date` string comment '应该发的货/ 但是没发/ inbound日期或者expiration日期',
`old_remain_lock_code` string comment '应该发的货/ 但是没发/ 锁码',
`new_export_amt` double comment '不应该发的货/ 但发了/ 剩余数量',
`new_export_location` string comment '不应该发的货/ 但发了/ 库位',
`new_export_date` string comment '不应该发的货/ 但发了/ inbound日期或者expiration日期',
`new_export_lock_codes` string comment '不应该发的货/ 但发了/ 锁码'
) COMMENT '周增,fifo异常管理 2week 的summary表.' 
PARTITIONED BY (`inc_day` string COMMENT '日分区') 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS 
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'



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




 