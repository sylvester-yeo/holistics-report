with cashout_success as (
    select
        user_cashout_ref_id
        ,sum(amount) as amount
    from grabpay_settlement.settle_cashout_relation
    where
        cashout_status = 'CASHOUT_SUCCESS'
        and [[upper(partner_id) in ({{partner_id}})]]
        and [[currency in ({{currency}})]]
        and [[date(date_add('hour',8,created)) between date({{start_date}}) and date({{end_date}})]] --convert to local time
        and [[date(date_add('hour',8,updated)) between date({{updated_start_date}}) and date({{updated_end_date}})]] --convert to local time
        and [[user_cashout_ref_id in ({{external_settlement_id}})]]
    group by 1
)
{{#if search_for_specific_merchant == 'yes'}}
,filtered_out_mex as (
    select
        mlm_stores.grab_pay_id
    from food_data_service.merchant_contracts mex_con
    left join xtramile.mlm_stores on mlm_stores.grab_id = mex_con.grabpay_grabid
    left join datamart.dim_merchants on mex_con.merchant_id = dim_merchants.merchant_id
    where ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])
        and country_id in (1,4) --filter out only for SG and MY
    group by 1
)
{{#endif}}
,merchant_transactions as (
    SELECT
        orderid
        ,cashoutid
        ,settlementid
        ,merchantid
        ,currency
        --,max(case when txtype = 60 then settlementid else null end) as settlementid
        --,max(case when txtype = 60 then cashoutid else null end) as cashoutid
        --,max(case when txtype = 60 then merchantid else null end) as merchantid
        -- ,max(case when txtype = 60 or (txtype = 59 and postscript like '%ADJUSTMENT%') then currency else null end) as currency
        ,max(partnerid) as partnerid
        ,max(partnertxid) as partnertxid
        ,max(status) AS status
        ,max(id) AS id
        ,min(created) AS created
        ,max(updated) AS updated
        ,min(transactioncreatedat) AS transactioncreatedat
        ,max(transactionupdatedat) AS transactionupdatedat
        -- ,sum(CASE WHEN txtype = 60 THEN amount ELSE 0 end)
        -- - sum(CASE WHEN txtype = 59 AND json_extract_scalar(try(json_parse(postscript)),'$.txCategory') = 'COMMISSION' THEN refundamount ELSE 0 END)
        -- + sum(CASE WHEN postscript like '%ADJUSTMENT%' THEN (amount - refundamount) ELSE 0 END) AS amount
        ,sum(amount - refundamount) as net_amount
        ,sum(amount) as amount
        ,sum(refundamount) as refundamount
        -- ,sum(CASE WHEN txtype = 59 AND json_extract_scalar(try(json_parse(postscript)),'$.txCategory') = 'COMMISSION' THEN refundamount ELSE 0 END) as refundamount
        ,sum(CASE WHEN postscript like '%ADJUSTMENT%' THEN (amount - refundamount) ELSE 0 END) as mpa_amount
    FROM grabpay_settlement.merchant_transactions tx
    {{#if search_for_specific_merchant == 'yes'}}
        inner join filtered_out_mex on tx.merchantid = cast(filtered_out_mex.grab_pay_id as varchar)
    {{#endif}}
    WHERE [[date(tx.year||'-'||tx.month||'-'||tx.day) between date({{start_date}}) - interval '2' day and date({{end_date}}) + interval '2' day  ]]
        and [[upper(Partnerid) in ({{partner_id}})]]
        --and currency IN ('SGD','MYR')
        and [[tx.currency in ({{currency}})]]
        and txtype in (59,60) -- 59 is for gkmm orders
        and [[date(created) between date({{start_date}}) - interval '2' day and date({{end_date}}) + interval '2' day ]]
        and [[date(updated) between date({{updated_start_date}}) - interval '2' day and date({{updated_end_date}}) + interval '2' day]]
        -- and ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])--filter for zeus mex_id
    GROUP BY 1,2,3,4,5
)
,fo as (
    select
            fo.order_id,
            fo.last_booking_code,
            coalesce(fo.last_booking_code, fo.order_id) as booking_code_order_id,
            fo.merchant_id,
            fo.short_order_number,
            date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local,
            gms.name
    from public.prejoin_food_order fo
    {{#if search_for_specific_merchant == 'yes'}}
    left join datamart.dim_merchants on fo.merchant_id = dim_merchants.merchant_id
    {{#endif}}
    left join public.cities on fo.city_id = cities.id
    left join grab_mall.grab_mall_seller gms on gms.id = fo.merchant_id
    left join public.countries on cities.country_id = countries.id
    where [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{start_date}}) - interval {{ interval_window }} DAY]]
            and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{end_date}}) + interval {{ interval_window }} DAY]]
            and [[fo.partition_date >= date_format(date({{ start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
            and [[fo.partition_date <= date_format(date({{ end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
            and [[fo.partition_date >= date_format(date({{ updated_start_date }}) - interval {{ interval_window }} DAY, '%Y-%m-%d')]]
            and [[fo.partition_date <= date_format(date({{ updated_end_date }}) + interval {{ interval_window }} DAY, '%Y-%m-%d')]]
            and [[countries.iso_currency in ({{currency}})]]
            and ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])--filter for zeus mex_id
)
select *
from ((
/*normal settlement, filtering out for mpa txn*/
        select
        tx.id
        ,tx.settlementid  as internal_settlement_id
        ,sm.manualcashoutrefid  as external_settlement_id
        ,tx.partnertxid as partnertxid
        ,tx.merchantid
        ,mlm_stores.trading_name as merchant_name
        --     ,null as Transactioncount
        ,tx.net_amount as credit_amount
        -- ,tx.refundamount as debit_amount --to check
        ,tx.currency
        ,case when cashout_success.amount = sm.amount then 'CASHOUT_SUCCESS' else tx.status end as tx_status
        ,'order details' as record_type
        ,date_add('hour', 8,tx.updated) as updated
        ,date_add('hour', 8,tx.created) as created
        ,tx.orderid as booking_code
        ,date_add('hour', 8,tx.transactioncreatedat) as updated
        ,date_add('hour', 8,tx.transactionupdatedat) as created
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
        ,case when mpa_amount <> 0 then 'MPA Involved' else NULL end as mpa_indicator
        from fo
        right join merchant_transactions tx on tx.orderid = fo.booking_code_order_id
        left join grabpay_settlement.settlement_meta sm on sm.merchantid = tx.merchantid  and tx.settlementid = sm.settlementid and sm.status = 'SETTLE_INITIATED'
        inner join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
        left join xtramile.mlm_stores on cast(tx.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
        left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
        left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
        where mpa_amount = 0
)
union all
(
/*mpa settlement*/
        select
        tx.id
        ,tx.settlementid  as internal_settlement_id
        ,sm.manualcashoutrefid  as external_settlement_id
        ,tx.partnertxid as partnertxid
        ,tx.merchantid
        ,mlm_stores.trading_name as merchant_name
        --     ,null as Transactioncount
        ,tx.net_amount as credit_amount
        -- ,tx.refundamount as debit_amount --to check
        ,tx.currency
        ,case when cashout_success.amount = sm.amount then 'CASHOUT_SUCCESS' else tx.status end as tx_status
        ,'order details' as record_type
        ,date_add('hour', 8,tx.updated) as updated
        ,date_add('hour', 8,tx.created) as created
        ,tx.orderid as booking_code
        ,date_add('hour', 8,tx.transactioncreatedat) as updated
        ,date_add('hour', 8,tx.transactionupdatedat) as created
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
        ,case when mpa_amount <> 0 then 'MPA Involved' else NULL end as mpa_indicator
        from fo
        right join merchant_transactions tx on tx.orderid = fo.order_id
        left join grabpay_settlement.settlement_meta sm on sm.merchantid = tx.merchantid  and tx.settlementid = sm.settlementid and sm.status = 'SETTLE_INITIATED'
        inner join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
        left join xtramile.mlm_stores on cast(tx.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
        left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
        left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
        where mpa_amount <> 0
)
union all
(
/*total settlement cashout, includes both grablending and grabfood*/
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
                ,date_add('hour', 8,sm.updated) as updated
                ,date_add('hour', 8,sm.created) as created
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
                ,NULL as mpa_indicator
        FROM grabpay_settlement.settlement_meta sm
        inner join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
        left join xtramile.mlm_stores on cast(sm.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
        left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
        left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
        {{#if search_for_specific_merchant == 'yes'}}
        inner join filtered_out_mex on cast(sm.merchantid as varchar) = cast(filtered_out_mex.grab_pay_id as varchar)
        {{#endif}}
        left join (select * from public.countries where id<= 6) countries on sm.currency = countries.currency_symbol
        where sm.status='SETTLED'
        and [[upper(sm.partnerid) in ({{partner_id}})]]
                and [[currency in ({{currency}})]]
                and [[date(created) between date({{start_date}}) - interval '2' day and date({{end_date}}) + interval '2' day ]]
                and [[date(updated) between date({{updated_start_date}}) - interval '2' day and date({{updated_end_date}}) + interval '2' day]]
                --and ([[dim_merchants.merchant_id in ({{zeus_merchant_info}})]] or [[dim_merchants.merchant_name in ({{zeus_merchant_info}})]] or [[dim_merchants.business_name in ({{zeus_merchant_info}})]])--filter for zeus mex_id
)
/*union all (
--individual txn breakdown for grablending, commented out because this should have been captured in mpa = 0 section
        select
                tx.id
                ,tx.settlementid  as internal_settlement_id
                ,sm.manualcashoutrefid  as external_settlement_id
                ,tx.partnertxid as partnertxid
                ,tx.merchantid
                ,mlm_stores.trading_name as merchant_name
                --     ,null as Transactioncount
                ,tx.net_amount as credit_amount
                -- ,tx.refundamount as debit_amount --to check
                ,tx.currency
                ,case when cashout_success.amount = sm.amount then 'CASHOUT_SUCCESS' else tx.status end as tx_status
                ,'order details' as record_type
                ,date_add('hour', 8,tx.updated) as updated
                ,date_add('hour', 8,tx.created) as created
                ,tx.orderid as booking_code
                ,date_add('hour', 8,tx.transactioncreatedat) as updated
                ,date_add('hour', 8,tx.transactionupdatedat) as created
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
                ,tx.partnerid
                ,case when mpa_amount <> 0 then 'MPA Involved' else NULL end as mpa_indicator
        from merchant_transactions tx
        left join grabpay_settlement.settlement_meta sm on sm.merchantid = tx.merchantid  and tx.settlementid = sm.settlementid and sm.status = 'SETTLE_INITIATED'
        inner join cashout_success on cashout_success.user_cashout_ref_id = sm.manualcashoutrefid
        left join xtramile.mlm_stores on cast(tx.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
        left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mlm_stores.grab_id
        left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
        where upper(tx.partner_id) = 'GRABLENDING'
)*/
)
where external_settlement_id <> '' --new logic
        and [[tx_status in ({{cashout_status}})]]
        and [[external_settlement_id in ({{external_settlement_id}})]]