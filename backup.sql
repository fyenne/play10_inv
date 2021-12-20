#
SELECT  *
FROM dsc_dwd.dwd_wh_dsc_inventory_dtl_di
WHERE src = 'scale'
AND ou_code IN ( 'HPPXXWHWDS', 'MICHETCTGS' 'COSTASHHTS', 'ZEBRASHALS' )
AND inc_day IN """+ str(fridays) + """
AND usage_flag