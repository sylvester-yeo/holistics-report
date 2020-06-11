with cashout_success as (
	select
        user_cashout_ref_id
        ,sum(amount) as amount
	from grabpay_settlement.settle_cashout_relation
	where cashout_status = 'CASHOUT_SUCCESS'
    and upper(partner_id) = 'GRABFOOD'
    and [[currency in ({{currency}})]]
    and [[date(created) between date({{start_date}}) and date({{end_date}})]]
    and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
	group by 1
)
,merchant_transactions as (
  SELECT 
    orderid
    ,max(case when txtype = 60 then settlementid else null end) as settlementid
    ,max(case when txtype = 60 then cashoutid else null end) as cashoutid
    ,max(case when txtype = 60 then merchantid else null end) as merchantid
    ,max(case when txtype = 60 then currency else null end) as currency
    ,max(case when txtype = 60 then partnerid else null end) as partnerid
    ,max(case when txtype = 60 then partnertxid else null end) as partnertxid
    ,max(CASE WHEN txtype = 60 THEN status ELSE NULL end) AS status
    ,max(CASE WHEN txtype = 60 THEN id ELSE NULL end) AS id
    ,min(case when txtype = 60 then created else null end) AS created
    ,max(case when txtype = 60 then updated else null end) AS updated 
    ,min(case when txtype = 60 then transactioncreatedat else null end) AS transactioncreatedat
    ,max(case when txtype = 60 then transactionupdatedat else null end) AS transactionupdatedat 
    ,sum(CASE WHEN txtype = 60 THEN amount ELSE 0 end) - sum(CASE WHEN txtype = 59 AND json_extract_scalar(json_parse(postscript),'$.txCategory') = 'COMMISSION' THEN refundamount ELSE 0 END) AS amount
    ,sum(CASE WHEN txtype = 59 AND json_extract_scalar(json_parse(postscript),'$.txCategory') = 'COMMISSION' THEN refundamount ELSE 0 END) as refundamount
  FROM grabpay_settlement.merchant_transactions tx
  WHERE [[date(tx.year||'-'||tx.month||'-'||tx.day) between date({{start_date}}) and date({{end_date}})]]
    and upper(Partnerid) = 'GRABFOOD'
    --and currency IN ('SGD','MYR')
    and [[tx.currency in ({{currency}})]]
    and txtype in (59,60) -- 59 is for gkmm orders
    and [[date(created) between date({{start_date}}) and date({{end_date}})]]
    and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
    and ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])--filter for zeus mex_id
  GROUP BY 1
)
,filtered_out_mex as (
        select
            mex_con.grabpay_grabid
        from food_data_service.merchant_contracts mex_con
        left join datamart.dim_merchants on mex_con.merchant_id = dim_merchants.merchant_id
        where ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])
                and country_id in (1,4) --filter out only for SG and MY
        group by 1
)
select *
from ((
select
    tx.id
    ,tx.settlementid  as internal_settlement_id
    ,sm.manualcashoutrefid  as external_settlement_id
    ,tx.partnertxid as partnertxid
    ,tx.merchantid
    ,mlm_stores.trading_name as merchant_name
--     ,null as Transactioncount
    ,tx.amount as credit_amount
    -- ,tx.refundamount as debit_amount --to check
    ,tx.currency
    ,case when cashout_success.amount = sm.amount then 'CASHOUT_SUCCESS' else tx.status end as tx_status
    ,'order details' as record_type
    ,from_utc_timestamp(tx.updated, countries.country_timezone) as updated
    ,from_utc_timestamp(tx.created, countries.country_timezone) as created
    ,tx.orderid as booking_code
    ,from_utc_timestamp(tx.transactioncreatedat, countries.country_timezone) as updated
    ,from_utc_timestamp(tx.transactionupdatedat, countries.country_timezone) as created
    ,fo.order_id as zeus_order_id
    ,fo.short_order_number
    ,fo.order_create_date_local
    ,fo.name as merchant_zeus_name_order
    ,fo.merchant_id as merchant_zeus_id_order
    ,mlm_bank_details.bank_statement_code
    ,'`'||mlm_bank_details.account_number as xm_bank_acc_number
    ,mlm_bank_details.swift_code as xm_bank_swift_code
    ,mlm_banks.bank_name as xm_bank_name
    ,mlm_stores.grab_id
    ,tx.partnerid
from merchant_transactions tx
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
        where [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{start_date}}) - interval {{ interval_window }} DAY]]
                and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{end_date}}) + interval {{ interval_window }} DAY]]
                and [[fo.partition_date >= date_format(date({{ start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
                and [[fo.partition_date <= date_format(date({{ end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
                and [[fo.partition_date >= date_format(date({{ updated_start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
                and [[fo.partition_date <= date_format(date({{ updated_end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
)fo on tx.orderid = fo.booking_code_order_id
left join grabpay_settlement.settlement_meta sm on sm.merchantid = tx.merchantid  and tx.settlementid = sm.settlementid and sm.status = 'SETTLE_INITIATED'
left join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
left join xtramile.mlm_stores on cast(tx.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
left join public.countries on tx.currency = countries.currency_symbol and countries.id <= 6
where upper(tx.Partnerid) = 'GRABFOOD'
        and [[tx.currency in ({{currency}})]]
        --     and tx.txtxpe = 60
        --and [[date(tx.year||'-'||tx.month||'-'||tx.day) between date({{start_date}}) and date({{end_date}})]]
        -- and [[date(created) between date({{start_date}}) and date({{end_date}})]]
        -- and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
)
union all
(
SELECT
        sm.id
        ,sm.settlementid
        ,sm.manualcashoutrefid as external_settlement_id
        ,null as partnertxid
        ,sm.merchantid
        ,mlm_stores.trading_name as merchant_name
        -- ,sm.transactioncount
        ,sm.amount
        ,sm.currency
        ,case when cashout_success.amount = sm.amount then 'CASHOUT_SUCCESS' else sm.status end as tx_status --or is it use new status
        ,'total' as record_type
        ,from_utc_timestamp(sm.updated, countries.country_timezone) as updated
        ,from_utc_timestamp(sm.created, countries.country_timezone) as created
        ,null as booking_code
        ,null as transactioncreatedat
        ,null as transactionupdatedat
        ,null as zeus_order_id
        ,null as short_order_number
        ,null as order_create_date_local
        ,null as merchant_zeus_name_order
        ,null as merchant_zeus_id_order
        ,mlm_bank_details.bank_statement_code
        ,'`'||mlm_bank_details.account_number as xm_bank_acc_number
        ,mlm_bank_details.swift_code as xm_bank_swift_code
        ,mlm_banks.bank_name as xm_bank_name
        ,mlm_stores.grab_id
        ,Partnerid
FROM grabpay_settlement.settlement_meta sm
left join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
left join xtramile.mlm_stores on cast(sm.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
-- left join filtered_out_mex on cast(mlm_stores.grab_id as varchar) = cast(filtered_out_mex.grabpay_grabid as varchar)
left join public.countries on sm.currency = countries.currency_symbol and countries.id <= 6
where sm.status='SETTLED'
      and upper(sm.partnerid) = 'GRABFOOD'
        and [[currency in ({{currency}})]]
        and [[date(created) between date({{start_date}}) and date({{end_date}})]]
        and [[date(updated) between date({{updated_start_date}}) and date({{updated_end_date}})]]
))
where external_settlement_id <> '' --new logic
        and [[tx_status in ({{cashout_status}})]]
        and [[external_settlement_id in ({{external_settlement_id}})]]

