with filtered_out_mex as (
  select
    grabpay_grabid
  from food_data_service.merchant_contracts mex_con
  left join datamart.dim_merchants on mex_con.merchant_id = dim_merchants.merchant_id
  where ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])
    and country_id in (1,4) --filter out only for SG and MY
  group by 1
)
select main.*
from
(
select *
from
(
select
tx.id,
Settlementid,
Cashoutid,
Merchantid,
-- fo.merchant_id as zeus_mex_id
mlm_stores.trading_name as merchant_name,
null as Transactioncount,
Amount,
Currency,
Status,
'order details' as record_type,
Updated,
Created,
Orderid as booking_code,
transactioncreatedat,
transactionupdatedat,
fo.order_id as zeus_order_id,
fo.short_order_number,
fo.order_create_date_local,
fo.name as merchant_zeus_name_order,
fo.merchant_id as merchant_zeus_id_order,
mlm_bank_details.bank_statement_code,
'`'||mlm_bank_details.account_number as xm_bank_acc_number,
mlm_bank_details.swift_code as xm_bank_swift_code,
mlm_banks.bank_name as xm_bank_name,
mlm_stores.grab_id,
Partnerid
from grabpay_settlement.merchant_transactions tx
left join xtramile.mlm_stores on cast(tx.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id

left join
(
select
fo.order_id,
fo.last_booking_code,
coalesce(fo.last_booking_code, fo.order_id) as booking_code_order_id,
fo.merchant_id,
fo.short_order_number,
date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local,
gms.name
from public.prejoin_food_order fo
left join public.cities on fo.city_id = cities.id
left join grab_mall.grab_mall_seller gms on gms.id = fo.merchant_id
-- where [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{start_date}}) - interval '3' DAY]]
--     and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{end_date}}) + interval '3' DAY]]
--     and [[fo.partition_date >= date_format(date({{ start_date }}) - interval '3' DAY, '%Y-%m-%d')]]
--     and [[fo.partition_date <= date_format(date({{ end_date }}) + interval '3' DAY, '%Y-%m-%d')]]
--     and [[fo.partition_date >= date_format(date({{ updated_start_date }}) - interval '3' DAY, '%Y-%m-%d')]]
--     and [[fo.partition_date <= date_format(date({{ updated_end_date }}) + interval '3' DAY, '%Y-%m-%d')]]
where [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{start_date}}) - interval {{ interval_window }} DAY]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{end_date}}) + interval {{ interval_window }} DAY]]
    and [[fo.partition_date >= date_format(date({{ start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
    and [[fo.partition_date <= date_format(date({{ end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
    and [[fo.partition_date >= date_format(date({{ updated_start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
    and [[fo.partition_date <= date_format(date({{ updated_end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
)fo on tx.orderid = fo.booking_code_order_id
left join datamart.dim_merchants on fo.merchant_id = dim_merchants.merchant_id
where upper(Partnerid) = 'GRABFOOD'
--and currency IN ('SGD','MYR')
and [[tx.currency in ({{currency}})]]
and txtype = 60
and [[date(created) between date({{start_date}}) and date({{end_date}})]]
and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
and ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])--filter for zeus mex_id
--order by transactioncreatedat desc
)
union all
(
select
ag.id,
Settlementid,
Cashoutid,
Merchantid,
--'null' as zeus_mex_id,
mlm_stores.trading_name as merchant_name,
Transactioncount,
Amount,
Currency,
Status,
'total' as record_type,
Updated,
Created,
null as booking_code,
null as transactioncreatedat,
null as transactionupdatedat,
null as zeus_order_id,
null as short_order_number,
null as order_create_date_local,
null as merchant_zeus_name_order,
null as merchant_zeus_id_order,
mlm_bank_details.bank_statement_code,
'`'||mlm_bank_details.account_number as xm_bank_acc_number,
mlm_bank_details.swift_code as xm_bank_swift_code,
mlm_banks.bank_name as xm_bank_name,
mlm_stores.grab_id,
Partnerid
from grabpay_settlement.settlement_meta ag
left join xtramile.mlm_stores on cast(ag.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
inner join filtered_out_mex on cast(mlm_stores.grab_id as varchar) = cast(filtered_out_mex.grabpay_grabid as varchar)
left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
where
--currency in ('SGD','MYR')
[[currency in ({{currency}})]]
and [[date(created) between date({{start_date}}) and date({{end_date}})]]
and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
and cashoutstatus='CASHOUT_SUCCESS'
and upper(Partnerid) in ('GRABFOOD')

--order by updated desc
)
) main
--left join food_data_service.merchants mex on mex.merchant_id = main.zeus_mex_id
where [[status in ({{cashout_status}})]]
  --and [[Merchantid in ({{merchant_id}})]]
  --and [[chain_number in ({{chain_number}})]]