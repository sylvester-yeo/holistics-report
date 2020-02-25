-- Report version that shows GKMM breakdown
{{#if report_version == 'new'}}
/*116820*/
with fo as (
    SELECT
        json_extract(snapshot_detail,'$.cartWithQuote.discounts') AS array_discount
        -- ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then
        --             cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) - COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0)
        --             else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) end)/100 as order_value_pre_tax
        ,cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceExcludeTaxInMinorUnit') as double)/100 as order_value_pre_tax
        ,COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double)/100,0) as tax_value
        ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double)
                    else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) + COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0) end)/100
                    as order_value_with_tax
        ,(cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission'), '0.0') as double) + cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.gkCommission'),'0.0') as double))/100 as mex_commission
        ,cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission'), '0.0') as double) / 100 as grabfood_commission
        ,cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.gkCommission'), '0.0') as double) / 100 as grabkitchen_commission
        ,coalesce(cast(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.commission') as double), 0)
        + coalesce(cast(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.gkCommissionRate') as double), 0) as mex_commission_rate
        ,cast(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.commission') as double) as grabfood_commission_rate
        ,cast(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.gkCommissionRate') as double) as grabkitchen_commission_rate
        ,coalesce(cast(json_extract_scalar(json_parse(snapshot_detail), '$.cartWithQuote.promoCodes[0].promoAmountInMin') as double),0) / power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0)) as promo_expense
        ,coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.promoCodes[0].promoCode'),'') as promo_code
        ,coalesce(cast(json_extract(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.merchantCartWithQuoteList') as array<json>)[1], '$.merchantInfoObj.taxRate') as double), fo.tax) as tax_perc
        ,case when json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType') in ('GRABKITCHENMIXMATCH', 'GRABKITCHENNORMAL') then TRUE else FALSE end as is_grabkitchen
        ,case when json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType') in ('GRABKITCHENMIXMATCH') then TRUE else FALSE end as is_grabkitchen_mixmatch
        ,json_extract(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.subMerchants') as mixmatch_sub_merchants
        ,json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.merchantClusterID') as merchant_cluster_id
        ,fo.*
    FROM public.prejoin_food_order fo
    inner join public.cities on cities.id = fo.city_id
    WHERE [[fo.country_id in ({{country|noquote}})]]
        and [[fo.city_id in ({{cities|noquote}})]]
        and [[fo.merchant_id in ({{merchant}})]]
        and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{order_create_start_date}})]]
        and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{order_create_end_date}})]]
        and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) >= date(date_format(date({{ order_delivered_start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
        and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) <= date(date_format(date({{ order_delivered_end_date }}) , '%Y-%m-%d'))]]
        and fo.partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')
        and fo.partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')
        and [[fo.order_id in ({{ order_ids }})]]
        {{#if delivery_option == 'takeaway'}}
		AND json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '1'
		{{#endif}}
		{{#if delivery_option == 'delivery'}}
		AND (json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '0'
		OR json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') is null)
		{{#endif}}
        {{#if final_state == 'completed_only'}} --1st addition
        and fo.order_state = 11
        {{#endif}}
        {{#if final_state == 'exclude_completed_orders'}} --2nd addition
        and fo.order_state <> 11
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'no'}}
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is null
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'yes'}} --3rd addition
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is not null
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'no'}} --3rd addition
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is null
        {{#endif}}
        {{#if business_line == 'gf'}}
        and not (coalesce(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType'), '') in ('GRABKITCHENMIXMATCH', 'GRABKITCHENNORMAL'))
        {{#endif}}
        {{#if business_line == 'gk'}}
        and (coalesce(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType'), '') in ('GRABKITCHENMIXMATCH', 'GRABKITCHENNORMAL'))
        {{#endif}}
        {{#if business_line == 'gkreg'}}
        and (coalesce(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType'), '') in ('GRABKITCHENNORMAL'))
        {{#endif}}
        {{#if business_line == 'gkmm'}}
        and (coalesce(json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType'), '') in ('GRABKITCHENMIXMATCH'))
        {{#endif}}
)
,mex_funded_fo as (
    select
        order_id
        ,sum(CAST(json_extract_scalar(discount,'$.mexFundedAmountInMin') AS double)/100) AS total_mex_funded_discount
        ,sum(CAST(json_extract_scalar(discount,'$.mexFundedAmountExcludeTaxInMin') AS double)/100) AS total_mex_funded_discount_exc_tax
    from fo
    CROSS JOIN UNNEST(CAST(array_discount AS array(json))) AS a(discount)
    group by 1
)
,gp_tx as (
    select
        merchant_transactions.txid as transaction_id,
        merchant_transactions.amount as amount,
        merchant_transactions.refundamount as refundamount,
        merchant_transactions.currency,
        merchant_transactions.status,
        merchant_transactions.txtype,
        merchant_transactions.transactioncreatedat,
        merchant_transactions.transactionupdatedat,
        merchant_transactions.orderid as booking_code,
        cast(merchant_contracts.merchant_id as varchar) as merchant_id
    from grabpay_settlement.merchant_transactions
    inner join xtramile.mlm_stores on cast(merchant_transactions.merchantid as varchar) = cast(mlm_stores.grab_pay_id as varchar)
    inner join food_data_service.merchant_contracts on cast(mlm_stores.grab_id as varchar) = cast(merchant_contracts.grabpay_grabid as varchar)
    where
        upper(Partnerid) = 'GRABFOOD'
        and ((currency in ('SGD','MYR') and txtype = 60) or (currency = 'THB' and txtype in (56,60)))
        and concat(YEAR, '-', lpad(MONTH, 2, '0'), '-', lpad(day, 2, '0')) >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')
        and concat(YEAR, '-', lpad(MONTH, 2, '0'), '-', lpad(day, 2, '0')) <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')
)
,fo_gkmm as (
    select
        gkmm_order_breakdown.*
        --merchant details
        ,mex_con.grabpay_grabid as grabpay_grab_id
        ,mex.name as merchant_name
        ,merchant_clusters.name as merchant_cluster_name
        ,mex_con.invoice_email AS invoice_contact_email
        --merchant bank details
        {{#if show_bank_details =='yes'}}
        ,'`'||mlm_bank_details.account_number as xm_bank_acc_number
        ,mlm_bank_details.swift_code as xm_bank_swift_code
        ,mlm_banks.bank_name as xm_bank_name
        ,mlm_bank_details.bank_statement_code
        {{#endif}}
        --gp payout details
        {{#if show_payout_details =='yes'}}
        ,gp_tx.transaction_id
        ,gp_tx.amount
        ,gp_tx.refundamount
        ,gp_tx.currency
        ,gp_tx.status
        ,gp_tx.txtype
        ,gp_tx.transactioncreatedat
        ,gp_tx.transactionupdatedat
        ,gp_tx.booking_code
        {{#endif}}
    from (
        select
            order_id
            ,last_booking_code
            ,merchant_cluster_id
            ,json_extract_scalar(sub_merchant, '$.merchantID') as gkmm_sub_merchant_id
            ,cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.priceExcludeTaxInMinorUnit') as double) / 100 as order_value
            ,cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.taxRate') as double) as tax_perc
            ,(
                coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.priceInMinorUnit') as double), 0)
                - coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.priceExcludeTaxInMinorUnit') as double), 0)
            ) / 100 as tax_value
            ,(coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.mexCommission') as double), 0) +
                coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.gkCommission') as double), 0)
            ) / 100
            as commission_value
            ,(coalesce(cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.commission') as double), 0) +
                coalesce(cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.gkCommissionRate') as double), 0)
            ) as commission_rate
            ,coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.mexCommission') as double), 0)/100 as commission_value_grabfood
            ,coalesce(cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.commission') as double), 0) as commission_rate_grabfood
            ,coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.gkCommission') as double), 0)/100 as commission_value_grabkitchen
            ,coalesce(cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.gkCommissionRate') as double), 0) as commission_rate_grabkitchen
            -- as of right now, there is no MFC that can be applied on a mex level
            ,(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.priceExcludeTaxInMinorUnit') as double) / 100)
            - ((coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.mexCommission') as double), 0) + coalesce(cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.gkCommission') as double), 0)) / 100)
            as net_payable
        from
            fo
        CROSS JOIN UNNEST(CAST(COALESCE(fo.mixmatch_sub_merchants, cast(array['No Sub Merchants'] AS json)) AS array(json))) AS a(sub_merchant)
        inner join public.cities on cities.id = fo.city_id
        where
            fo.is_grabkitchen_mixmatch -- only include data from GKMM orders
    ) gkmm_order_breakdown
    -- GP txn details
    left join gp_tx on
        -- Join needs to be done on booking code and merchant id level since 1 booking code can
        -- have more than 1 merchant contained in them for GKMM orders
        gkmm_order_breakdown.gkmm_sub_merchant_id = gp_tx.merchant_id
        and gkmm_order_breakdown.last_booking_code = gp_tx.booking_code
    left join food_data_service.merchants as mex on mex.merchant_id = gkmm_order_breakdown.gkmm_sub_merchant_id
    left join food_data_service.merchant_contracts as mex_con on mex_con.merchant_id = gkmm_order_breakdown.gkmm_sub_merchant_id
    left join food_data_service.merchant_clusters on merchant_clusters.merchant_cluster_id = gkmm_order_breakdown.merchant_cluster_id
    left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mex_con.grabpay_grabid
    left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
)
,food_cashier as (
    select 
        order_id
        ,booking_code
        ,mex_id as merchant_id
        ,mex_grabid as grabpay_grab_id
        -- ,tx_amount as net_paid_to_mex
        ,cast(json_extract_scalar(py.metadata, '$.netEarning') as double)/100 as net_earning
        ,cast(json_extract_scalar(py.metadata, '$.orderValue') as double)/100 as order_value_pre_tax
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.taxes') as double)/100, 0) as tax_value
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.exDeliveryFee') as double)/100, 0) as pax_delivery_fee
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.mexFundCampaign') as double)/100, 0) + coalesce(cast(json_extract_scalar(py.metadata, '$.mexFundPromo') as double)/100, 0) as total_mex_funded_discount_exc_tax
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.revenue') as double)/100, 0) as revenue_to_mex
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.GKCommission') as double)/100, 0) as gk_commission
        ,coalesce(cast(json_extract_scalar(py.metadata, '$.mexCommission') as double)/100, 0) as gf_commission
    from grab_food.payments py
    where py.year||'-'||py.month||'-'||py.day <= date_format(date({{ order_create_start_date }}) + interval '10' DAY , '%Y-%m-%d')
        and py.year||'-'||py.month||'-'||py.day >= date_format(date({{ order_create_start_date }}) - interval '1' DAY , '%Y-%m-%d')
        and currency in ('SGD','MYR') --tbc on how to filter by country id
        and tx_type = 2 -- pay mex
)
-- All merchants except grabkitchen mix and match
select
    all_orders.city_name
    ,all_orders.country_name
    ,all_orders.city_id
    ,all_orders.country_id
    ,all_orders.order_id
    ,all_orders.last_booking_code as booking_code
    ,all_orders.short_order_id
    --merchant details
    ,all_orders.mex_gamma_id
    ,coalesce(fo_gkmm.gkmm_sub_merchant_id, all_orders.merchant_id) as merchant_id
    ,coalesce(fo_gkmm.grabpay_grab_id, all_orders.grabpay_grab_id) as grabpay_grab_id
    ,coalesce(fo_gkmm.merchant_name, all_orders.merchant_name) as merchant_name
    ,coalesce(fo_gkmm.merchant_cluster_id, all_orders.merchant_cluster_id) as merchant_cluster_id
    ,coalesce(fo_gkmm.merchant_cluster_name, all_orders.merchant_cluster_name) as merchant_cluster_name
    ,coalesce(fo_gkmm.invoice_contact_email, all_orders.invoice_contact_email) as invoice_contact_email
    --merchant bank details
    {{#if show_bank_details =='yes'}}
    ,all_orders.store_bank_acc_number
    ,all_orders.store_bank_swift_code
    ,coalesce(fo_gkmm.xm_bank_acc_number, all_orders.xm_bank_acc_number) as xm_bank_acc_number
    ,coalesce(fo_gkmm.xm_bank_swift_code, all_orders.xm_bank_swift_code) as xm_bank_swift_code
    ,coalesce(fo_gkmm.xm_bank_name, all_orders.xm_bank_name) as xm_bank_name
    ,coalesce(fo_gkmm.bank_statement_code, all_orders.bank_statement_code) as bank_statement_code
    {{#endif}}
    ,all_orders.order_create_date_local
    ,all_orders.order_create_time_local
    ,all_orders.order_allocated_time_local
    ,all_orders.qsr_order_submit_time_local
    ,all_orders.order_dax_at_store_time_local
    ,all_orders.order_food_collected_time_local
    ,all_orders.order_dax_arrive_pax_time_local
    ,all_orders.order_delivery_date_local
    ,all_orders.order_delivery_time_local
    ,all_orders.order_del_cancel_time_local
    ,all_orders.order_pre_cancel_time_local
    ,all_orders.order_st_confirm_time_local
    ,all_orders.pickup_area
    ,all_orders.dropoff_area
    ,all_orders.gamma_driver_id
    ,all_orders.dax_delivery_fee
    ,coalesce(food_cashier.pax_delivery_fee, all_orders.pax_delivery_fee) as pax_delivery_fee
    ,all_orders.promo_code
    ,all_orders.promo_expense
    ,all_orders.mexFunded
    ,coalesce(food_cashier.total_mex_funded_discount_exc_tax, all_orders.mexFundedExcTax) as mexFundedExcTax
    ,coalesce(food_cashier.order_value_pre_tax, fo_gkmm.order_value, all_orders.order_value) as order_value
    ,coalesce(fo_gkmm.tax_perc, all_orders.tax_perc) as tax_perc
    ,coalesce(food_cashier.tax_value, fo_gkmm.tax_value, all_orders.tax_value) as tax_value
    ,coalesce(food_cashier.gk_commission + food_cashier.gf_commission, fo_gkmm.commission_value, all_orders.commission_value) as commission_value
    ,coalesce(fo_gkmm.commission_rate, all_orders.commission_rate) as commission_rate
    ,coalesce(food_cashier.gf_commission, fo_gkmm.commission_value_grabfood, all_orders.commission_value_grabfood) as commission_value_grabfood
    ,coalesce(fo_gkmm.commission_rate_grabfood, all_orders.commission_rate_grabfood) as commission_rate_grabfood
    ,coalesce(food_cashier.gk_commission, fo_gkmm.commission_value_grabkitchen, all_orders.commission_value_grabkitchen) as commission_value_grabkitchen
    ,coalesce(fo_gkmm.commission_rate_grabkitchen, all_orders.commission_rate_grabkitchen) as commission_rate_grabkitchen
    ,coalesce(food_cashier.net_earning, fo_gkmm.net_payable, all_orders.net_payable) as net_payable
    ,all_orders.pay_status
    ,all_orders.mex_accept
    ,all_orders.mex_auto_accept
    ,all_orders.cancel_msg
    ,all_orders.cancel_reason_enum
    ,all_orders.final_state
    ,all_orders.qsr_pos_flag
    ,all_orders.qsr_pos_integration_type
    ,all_orders.qsr_pos_is_submitted
    ,all_orders.qsr_pay_merchant
    ,all_orders.my_mex_auto_accept
    ,all_orders.my_mex_accept_column
    --gp payout details
    {{#if show_payout_details =='yes'}}
    ,coalesce(fo_gkmm.transaction_id, all_orders.transaction_id) as transaction_id
    ,coalesce(fo_gkmm.amount, all_orders.amount) as amount
    ,coalesce(fo_gkmm.refundamount, all_orders.refundamount) as refundamount
    ,coalesce(fo_gkmm.currency, all_orders.currency) as currency
    ,coalesce(fo_gkmm.status, all_orders.status) as status
    ,coalesce(fo_gkmm.txtype, all_orders.txtype) as txtype
    ,coalesce(fo_gkmm.transactioncreatedat, all_orders.transactioncreatedat) as transactioncreatedat
    ,coalesce(fo_gkmm.transactionupdatedat, all_orders.transactionupdatedat) as transactionupdatedat
    ,coalesce(fo_gkmm.booking_code, all_orders.booking_code) as booking_code
    {{#endif}}
from (
    SELECT
        *
    FROM (
        SELECT
            cities.name as city_name
            ,cities.country_code as country_name
            ,fo.city_id
            ,fo.country_id
            ,fo.order_id as order_id
            ,fo.last_booking_code
            ,fo.short_order_number as short_order_id
            --merchant details
            ,drivers.id as mex_gamma_id
            ,fo.merchant_id as merchant_id
            ,mex_con.grabpay_grabid as grabpay_grab_id
            ,mex.name as merchant_name
            ,fo.merchant_cluster_id as merchant_cluster_id
            ,merchant_clusters.name as merchant_cluster_name
            ,mex_con.invoice_email AS invoice_contact_email
            --merchant bank details
            {{#if show_bank_details =='yes'}}
            ,'`'||det.acc_number as store_bank_acc_number
            ,bank.code as store_bank_swift_code
            ,'`'||mlm_bank_details.account_number as xm_bank_acc_number
            ,mlm_bank_details.swift_code as xm_bank_swift_code
            ,mlm_banks.bank_name as xm_bank_name
            ,mlm_bank_details.bank_statement_code as bank_statement_code
            {{#endif}}
            --,1 as is_asap
            --order time details
            ,date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local
            ,from_utc_timestamp(fo.created_time, cities.time_zone) as order_create_time_local
            ,from_utc_timestamp(fo.del_last_picking_up_time, cities.time_zone) as order_allocated_time_local
            ,from_utc_timestamp(cast(from_iso8601_timestamp(json_extract_scalar(fo.metadata, '$.partnerOrderSubmitTime')) as timestamp),cities.time_zone) AS qsr_order_submit_time_local
            ,from_utc_timestamp(fo.del_first_driver_at_store_time, cities.time_zone) as order_dax_at_store_time_local
            ,from_utc_timestamp(fo.del_food_collected_time, cities.time_zone) as order_food_collected_time_local
            ,from_utc_timestamp(fo.del_driver_arrived_time, cities.time_zone) as order_dax_arrive_pax_time_local
            ,date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) as order_delivery_date_local
            ,from_utc_timestamp(fo.ord_completed_time, cities.time_zone) as order_delivery_time_local
            --,from_utc_timestamp(fo.ord_cancelled_time, cities.time_zone) as order_cancel_time_local
            ,from_utc_timestamp(fo.del_cancelled_time, cities.time_zone) as order_del_cancel_time_local
            ,from_utc_timestamp(fo.pre_cancelled_time, cities.time_zone) as order_pre_cancel_time_local
            ,from_utc_timestamp(coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time), cities.time_zone) as order_st_confirm_time_local

            --order location details
            ,bb.pickup_area
            ,bb.dropoff_area
            --,pick_up_latitude
        --,pick_up_longitude

        --driver and fare details
            ,bb.driver_id as gamma_driver_id
            ,bb.dax_fare as dax_delivery_fee
            ,bb.pax_fare as pax_delivery_fee
            ,case when lower(mex.name) like '%domino%' then fo.promo_code else bb.promo_code end as promo_code
            --,bb.promo_code
            ,case when lower(mex.name) like '%domino%' then fo.promo_expense else bb.promo_expense end as promo_expense
            ,ROUND(coalesce(mex_funded_fo.total_mex_funded_discount,0), 2) as mexFunded
            ,ROUND(coalesce(mex_funded_fo.total_mex_funded_discount_exc_tax,0), 2) as mexFundedExcTax
            ,ROUND(coalesce(fo.order_value_pre_tax, bb.food_sub_total),2) as order_value
            ,ROUND(coalesce(fo.tax_perc, fo.tax),2) as tax_perc
            ,ROUND(fo.tax_value,2) as tax_value
            ,ROUND(fo.mex_commission,2) as commission_value
            ,round(fo.mex_commission_rate,2) as commission_rate
            ,ROUND(fo.grabfood_commission ,2) as commission_value_grabfood
            ,round(fo.grabfood_commission_rate, 2) as commission_rate_grabfood
            ,ROUND(fo.grabkitchen_commission ,2) as commission_value_grabkitchen
            ,round(fo.grabkitchen_commission_rate, 2) as commission_rate_grabkitchen
            ,ROUND(fo.order_value_pre_tax + fo.tax_value - coalesce(mex_funded_fo.total_mex_funded_discount_exc_tax,0) - fo.mex_commission, 2) as net_payable
            --,case when fo.country_id = 4 then round(round((coalesce(fo.sub_total,bb.food_sub_total) * (1 + fo.tax)),2) - round((coalesce(fo.sub_total,bb.food_sub_total) * fo.commission * 1.07),2), 2) else round(round((coalesce(fo.sub_total,bb.food_sub_total) * (1 + fo.tax)),2) - round((coalesce(fo.sub_total,bb.food_sub_total) * fo.commission),2), 2) end as net_payable_mex
            --pay status
            ,CASE WHEN fo.pre_accepted_time IS NOT NULL and fo.ord_completed_time IS NOT NULL THEN 'pay'
                    WHEN fo.ord_completed_time IS NOT NULL THEN 'pay'
                    When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = 'UNALLOCATED') THEN 'no pay'
                    When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and ( fo.final_state = 'REALLOCATION_FAILED') THEN 'pay'
                    When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = '' and fo.booking_state = 'UNALLOCATED') THEN 'no pay'
                    WHEN fo.pre_accepted_time IS NOT NULL THEN 'pay'
                    ELSE 'no pay' END AS pay_status
            ,CASE WHEN coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL THEN 'mex_auto_accept' ELSE NULL END AS mex_accept
            ,case when fo.metadata <> '' then
            case when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'auto-accept' end
                when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'manual-accept'
                else null end as mex_auto_accept
        --   ,case when aa.auto_accept_order is not null then 'mex_auto_accept' else null end as mex_auto_accept_status
            ,fo.cancel_msg
            --,logs.op_cancel_reason
            ,map.cancel_reason_enum
            ,case when order_state = 11 then 'COMPLETED'
                when final_state = '' and fo.booking_state is null and fo.pre_expired_time is not null then 'MAX_IGNORE'
                when final_state = '' and fo.booking_state is null and fo.pre_cancelled_time is not null then 'PAX_REQUEST_CANCEL'
                    when final_state = '' then fo.booking_state
                else final_state end as final_state
            ,case when json_extract_scalar(fo.metadata, '$.qsrCode') = 'mcd' then 'QSR POS Integrated' end as qsr_pos_flag
            ,json_extract_scalar(fo.metadata, '$.qsrCode') as qsr_pos_integration_type
            ,case when json_extract_scalar(fo.metadata, '$.partnerOrderSubmitTime') is not null then 1 else 0 end as qsr_pos_is_submitted
            ,case when json_extract_scalar(fo.metadata, '$.payMerchant') is not null then 1 else 0 end as qsr_pay_merchant
            ,case when fo.metadata <> '' then
            case when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'mex_auto_accept' end
            else null end as my_mex_auto_accept
            ,case when fo.metadata <> '' then
                case
                    when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'mex_accept'
                    when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'mex_accept'
                    end
                when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'mex_accept'
                else NULL end as my_mex_accept_column
            --gp payout details
            {{#if show_payout_details =='yes'}}
            ,gp_tx.transaction_id
            ,gp_tx.amount
            ,gp_tx.refundamount
            ,gp_tx.currency
            ,gp_tx.status
            ,gp_tx.txtype
            ,gp_tx.transactioncreatedat
            ,gp_tx.transactionupdatedat
            ,gp_tx.booking_code
            {{#endif}}
            --Food order table
        FROM fo
        -- -- join with gkmm table (coalesce logic for columns that requires breakdown)
        -- left join fo_gkmm on fo_gkmm.order_id = fo.order_id
        --base bookings
        left join datamart_grabfood.base_bookings bb on fo.order_id = bb.order_id
        left join mex_funded_fo on fo.order_id = mex_funded_fo.order_id
        -- GP txn details
        left join gp_tx 
            on fo.last_booking_code = gp_tx.booking_code
            and fo.merchant_id = gp_tx.merchant_id
        --Auto accept details
        left join
        (
            -- Create Table of AA Orders -- individual orders
            SELECT
            --ELEMENT_AT(PROPERTIES, 'date') AS LOCAL_TIME
            --, ELEMENT_AT(PROPERTIES, 'action') AS ACTION
            ELEMENT_AT(PROPERTIES, 'merchantID') AS MERCHANT_ID
            , ELEMENT_AT(PROPERTIES, 'orderID') AS ORDER_ID
            , CASE
            WHEN ELEMENT_AT(PROPERTIES, 'operation.name') = 'max.track.auto.accept.order' THEN 'AA Order'
            ELSE NULL
            END AS AUTO_ACCEPT_ORDER
            , year||'-'||month||'-'||day AS PARTITION_DATE
            FROM LOGS_V2.RAW_LOGS_HOURLY
            WHERE LOG = 'food-max-api'
            AND ELEMENT_AT(PROPERTIES, 'operation.name') = 'max.track.auto.accept.order'
            and [[year||'-'||month||'-'||day >= date_format(date({{ order_create_start_date }}) - interval '3' DAY, '%Y-%m-%d')]]
            and [[year||'-'||month||'-'||day <= date_format(date({{ order_create_end_date }}) , '%Y-%m-%d')]]
            group by 1,2,3,4
        )aa on fo.order_id = aa.order_id
            and fo.merchant_id = aa.merchant_id
        --merchant and bank details
        left join food_data_service.merchants as mex on mex.merchant_id = fo.merchant_id
        left join food_data_service.merchant_contracts as mex_con on mex_con.merchant_id = mex.merchant_id
        left join food_data_service.merchant_clusters on merchant_clusters.merchant_cluster_id = fo.merchant_cluster_id
        left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mex_con.grabpay_grabid
        left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
        left join public.drivers on fo.merchant_id = drivers.identification_card_number
        left join paysi.bank_detail det on drivers.id = det.user_id
        left join paysi.bank bank on det.bank_id = bank.id
        left join public.cities on fo.city_id = cities.id
        left join slide.food_pax_cancel_reason_mapping map on fo.cancel_code = map.cancel_code
        WHERE
            --integrated orders
            order_type = 1
            --SG & MY
            --and fo.country_id in (1,3,4)
            and bb.vertical = 'GrabFood'
            and [[bb.country_id in ({{country|noquote}})]]
            and [[bb.city_id in ({{cities|noquote}})]]
            and [[bb.merchant_id in ({{merchant}})]]
            and [[mex.chain_number in ({{chain_number}})]]
            and [[date(bb.date_local) >= date({{order_create_start_date}})]]
            and [[date(bb.date_local) <= date({{order_create_end_date}})]]
            and [[bb.order_id in ({{ order_ids }})]]
    )
    {{#if auto_accept_order == 'yes'}}
    where
        my_mex_auto_accept = 'mex_auto_accept'
    {{#endif}}
    {{#if auto_accept_order == 'no'}}
    where
        my_mex_auto_accept is null
    {{#endif}}
) all_orders
left join fo_gkmm on fo_gkmm.order_id = all_orders.order_id
left join food_cashier 
    on all_orders.order_id = food_cashier.order_id
    and food_cashier.merchant_id = all_orders.merchant_id 
{{#endif}}
{{#if report_version == 'old'}}
/*116820*/
with fo as (
    SELECT
        json_extract(snapshot_detail,'$.cartWithQuote.discounts') AS array_discount
        -- ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then
        --             cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) - COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0)
        --             else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) end)/100 as order_value_pre_tax
        ,cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceExcludeTaxInMinorUnit') as double)/100 as order_value_pre_tax
        ,COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double)/100,0) as tax_value
        ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double)
                    else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) + COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0) end)/100
                    as order_value_with_tax
        ,(cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.totalQuoteInMin.mexCommission'),'0.0') as double) + cast(coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.totalQuoteInMin.gkCommission'),'0.0') as double))/100 as mex_commission
        ,coalesce(cast(json_extract_scalar(json_parse(snapshot_detail), '$.cartWithQuote.promoCodes[0].promoAmountInMin') as double),0) / power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0))
            as promo_expense
        ,coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.promoCodes[0].promoCode'),'') as promo_code
        ,coalesce(cast(json_extract(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.merchantCartWithQuoteList') as array<json>)[1], '$.merchantInfoObj.commission') as double), fo.commission) as commission_rate
        ,coalesce(cast(json_extract(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.merchantCartWithQuoteList') as array<json>)[1], '$.merchantInfoObj.taxRate') as double), fo.tax) as tax_perc
        ,*
    FROM public.prejoin_food_order fo
    WHERE [[country_id in ({{country|noquote}})]]
        and [[city_id in ({{cities|noquote}})]]
        and [[merchant_id in ({{merchant}})]]
        and [[partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
        and [[partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
        and [[fo.order_id in ({{ order_ids }})]]
        {{#if delivery_option == 'takeaway'}}
		AND json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '1'
		{{#endif}}
		{{#if delivery_option == 'delivery'}}
		AND (json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '0'
		OR json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') is null)
		{{#endif}}
        {{#if final_state == 'completed_only'}} --1st addition
        and fo.order_state = 11
        {{#endif}}
        {{#if final_state == 'exclude_completed_orders'}} --2nd addition
        and fo.order_state <> 11
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'no'}}
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is null
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'yes'}} --3rd addition
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is not null
        {{#endif}}
        {{#if qsr_pos_integrated_only == 'no'}} --3rd addition
        and json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.qsrCode') is null
        {{#endif}}
)
, mex_funded_fo as (
    select
        order_id
        ,sum(CAST(json_extract_scalar(discount,'$.mexFundedAmountInMin') AS double)/100) AS total_mex_funded_discount
        ,sum(CAST(json_extract_scalar(discount,'$.mexFundedAmountExcludeTaxInMin') AS double)/100) AS total_mex_funded_discount_exc_tax
    from fo
    CROSS JOIN UNNEST(CAST(array_discount AS array(json))) AS a(discount)
    group by 1
)
SELECT * FROM (
SELECT
     cities.name as city_name
	  ,cities.country_code as country_name
	  ,fo.city_id
	  ,fo.country_id
        ,fo.order_id as order_id
	  ,fo.last_booking_code as booking_code
	  ,fo.short_order_number as short_order_id

	  --merchant details
	  ,drivers.id as mex_gamma_id
	  ,fo.merchant_id
	  ,mex_con.grabpay_grabid as grabpay_grab_id
	  ,mex.name as merchant_name
	  ,mex_con.invoice_email AS invoice_contact_email

    --merchant bank details
	{{#if show_bank_details =='yes'}}
	  ,'`'||det.acc_number as store_bank_acc_number
	  ,bank.code as store_bank_swift_code
	  ,'`'||mlm_bank_details.account_number as xm_bank_acc_number
  	,mlm_bank_details.swift_code as xm_bank_swift_code
  	,mlm_banks.bank_name as xm_bank_name
  	,mlm_bank_details.bank_statement_code
  	{{#endif}}
	  --,1 as is_asap
	  --order time details
	  ,date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local
	  ,from_utc_timestamp(fo.created_time, cities.time_zone) as order_create_time_local
	  ,from_utc_timestamp(fo.del_last_picking_up_time, cities.time_zone) as order_allocated_time_local
	  ,from_utc_timestamp(cast(from_iso8601_timestamp(json_extract_scalar(fo.metadata, '$.partnerOrderSubmitTime')) as timestamp),cities.time_zone) AS qsr_order_submit_time_local
	  ,from_utc_timestamp(fo.del_first_driver_at_store_time, cities.time_zone) as order_dax_at_store_time_local
	  ,from_utc_timestamp(fo.del_food_collected_time, cities.time_zone) as order_food_collected_time_local
	  ,from_utc_timestamp(fo.del_driver_arrived_time, cities.time_zone) as order_dax_arrive_pax_time_local
	  ,date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) as order_delivery_date_local
	  ,from_utc_timestamp(fo.ord_completed_time, cities.time_zone) as order_delivery_time_local
	  --,from_utc_timestamp(fo.ord_cancelled_time, cities.time_zone) as order_cancel_time_local
	  ,from_utc_timestamp(fo.del_cancelled_time, cities.time_zone) as order_del_cancel_time_local
	  ,from_utc_timestamp(fo.pre_cancelled_time, cities.time_zone) as order_pre_cancel_time_local
	  ,from_utc_timestamp(coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time), cities.time_zone) as order_st_confirm_time_local

	  --order location details
	  ,bb.pickup_area
	  ,bb.dropoff_area
	  --,pick_up_latitude
    --,pick_up_longitude

    --driver and fare details
	  ,bb.driver_id as gamma_driver_id
	  ,bb.dax_fare as dax_delivery_fee
	  ,bb.pax_fare as pax_delivery_fee
	  ,case when lower(mex.name) like '%domino%' then fo.promo_code else bb.promo_code end as promo_code
	  --,bb.promo_code
	  ,case when lower(mex.name) like '%domino%' then fo.promo_expense else bb.promo_expense end as promo_expense
	  ,ROUND(coalesce(fo.order_value_pre_tax, bb.food_sub_total),2) as order_value
	  ,ROUND(coalesce(mex_funded_fo.total_mex_funded_discount,0), 2) as mexFunded
	  ,ROUND(coalesce(mex_funded_fo.total_mex_funded_discount_exc_tax,0), 2) as mexFundedExcTax
	  ,ROUND(coalesce(fo.tax_perc, fo.tax),2) as tax_perc
	  ,ROUND(fo.tax_value,2) as tax_value
	  ,ROUND(fo.commission_rate, 2) as commission_rate
	  ,ROUND(fo.mex_commission,2) as commission_value
	  ,ROUND(fo.order_value_pre_tax + fo.tax_value - coalesce(mex_funded_fo.total_mex_funded_discount_exc_tax,0) - fo.mex_commission, 2) as net_payable
	  --,case when fo.country_id = 4 then round(round((coalesce(fo.sub_total,bb.food_sub_total) * (1 + fo.tax)),2) - round((coalesce(fo.sub_total,bb.food_sub_total) * fo.commission * 1.07),2), 2) else round(round((coalesce(fo.sub_total,bb.food_sub_total) * (1 + fo.tax)),2) - round((coalesce(fo.sub_total,bb.food_sub_total) * fo.commission),2), 2) end as net_payable_mex

	  --pay status
       ,CASE WHEN fo.pre_accepted_time IS NOT NULL and fo.ord_completed_time IS NOT NULL THEN 'pay'
             WHEN fo.ord_completed_time IS NOT NULL THEN 'pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = 'UNALLOCATED') THEN 'no pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and ( fo.final_state = 'REALLOCATION_FAILED') THEN 'pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = '' and fo.booking_state = 'UNALLOCATED') THEN 'no pay'
             WHEN fo.pre_accepted_time IS NOT NULL THEN 'pay'
             ELSE 'no pay' END AS pay_status
      ,CASE WHEN coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL THEN 'mex_auto_accept' ELSE NULL END AS mex_accept
      ,case when fo.metadata <> '' then
        case when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'auto-accept' end
            when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'manual-accept'
            else null end as mex_auto_accept
    --   ,case when aa.auto_accept_order is not null then 'mex_auto_accept' else null end as mex_auto_accept_status
      ,fo.cancel_msg
      --,logs.op_cancel_reason
      ,map.cancel_reason_enum
      ,case when order_state = 11 then 'COMPLETED'
            when final_state = '' and fo.booking_state is null and fo.pre_expired_time is not null then 'MAX_IGNORE'
            when final_state = '' and fo.booking_state is null and fo.pre_cancelled_time is not null then 'PAX_REQUEST_CANCEL'
		      	when final_state = '' then fo.booking_state
            else final_state end as final_state
      ,case when json_extract_scalar(fo.metadata, '$.qsrCode') = 'mcd' then 'QSR POS Integrated' end as qsr_pos_flag
      ,json_extract_scalar(fo.metadata, '$.qsrCode') as qsr_pos_integration_type
      ,case when json_extract_scalar(fo.metadata, '$.partnerOrderSubmitTime') is not null then 1 else 0 end as qsr_pos_is_submitted
      ,case when json_extract_scalar(fo.metadata, '$.payMerchant') is not null then 1 else 0 end as qsr_pay_merchant

       ,case when fo.metadata <> '' then
        case when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'mex_auto_accept' end
        else null end as my_mex_auto_accept
    ,case when fo.metadata <> '' then
        case
            when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'mex_accept'
            when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'mex_accept'
            end
        when coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time) IS NOT NULL then 'mex_accept'
        else NULL end as my_mex_accept_column



      --gp payout details
      {{#if show_payout_details =='yes'}}
      ,gp_tx.*
      {{#endif}}

    --Food order table
    FROM fo
    --base bookings
    left join datamart_grabfood.base_bookings bb on fo.order_id = bb.order_id

    left join mex_funded_fo on fo.order_id = mex_funded_fo.order_id

    -- GP txn details
    left join
    (
    select
    txid as transaction_id,
    amount as amount,
    refundamount as refundamount,
    currency,
    status,
    txtype,
    transactioncreatedat,
    transactionupdatedat,
    orderid as booking_code
    from grabpay_settlement.merchant_transactions
    where upper(Partnerid) = 'GRABFOOD'
    and ((currency in ('SGD','MYR') and txtype = 60) or (currency = 'THB' and txtype in (56,60)))
    )gp_tx on fo.last_booking_code = gp_tx.booking_code
    --Auto accept details
    left join
    (
    -- Create Table of AA Orders -- individual orders
    SELECT
    --ELEMENT_AT(PROPERTIES, 'date') AS LOCAL_TIME
    --, ELEMENT_AT(PROPERTIES, 'action') AS ACTION
     ELEMENT_AT(PROPERTIES, 'merchantID') AS MERCHANT_ID
    , ELEMENT_AT(PROPERTIES, 'orderID') AS ORDER_ID
    , CASE
       WHEN ELEMENT_AT(PROPERTIES, 'operation.name') = 'max.track.auto.accept.order' THEN 'AA Order'
       ELSE NULL
     END AS AUTO_ACCEPT_ORDER
    , year||'-'||month||'-'||day AS PARTITION_DATE
    FROM LOGS_V2.RAW_LOGS_HOURLY
    WHERE LOG = 'food-max-api'
    AND ELEMENT_AT(PROPERTIES, 'operation.name') = 'max.track.auto.accept.order'
    and [[year||'-'||month||'-'||day >= date_format(date({{ order_create_start_date }}) - interval '3' DAY, '%Y-%m-%d')]]
    and [[year||'-'||month||'-'||day <= date_format(date({{ order_create_end_date }}) , '%Y-%m-%d')]]
    group by 1,2,3,4
    )aa on fo.order_id = aa.order_id
        and fo.merchant_id = aa.merchant_id

    --merchant and bank details
    left join food_data_service.merchants as mex on mex.merchant_id = fo.merchant_id
    left join food_data_service.merchant_contracts as mex_con on mex_con.merchant_id = mex.merchant_id
    left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mex_con.grabpay_grabid
	left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
    left join public.drivers on fo.merchant_id = drivers.identification_card_number
    left join paysi.bank_detail det on drivers.id = det.user_id
    left join paysi.bank bank on det.bank_id = bank.id
    left join public.cities on fo.city_id = cities.id
    left join slide.food_pax_cancel_reason_mapping map on fo.cancel_code = map.cancel_code
    WHERE
    --integrated orders
    order_type = 1
    --SG & MY
    --and fo.country_id in (1,3,4)
    and bb.vertical = 'GrabFood'
    and [[bb.country_id in ({{country|noquote}})]]
    and [[bb.city_id in ({{cities|noquote}})]]
    and [[bb.merchant_id in ({{merchant}})]]
    and [[mex.chain_number in ({{chain_number}})]]
    and [[bb.order_id in ({{ order_ids }})]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{order_create_start_date}})]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{order_create_end_date}})]]
    -- and [[fo.partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
    -- and [[fo.partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
    and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) >= date(date_format(date({{ order_delivered_start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
    and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) <= date(date_format(date({{ order_delivered_end_date }}) , '%Y-%m-%d'))]]

    and [[date(bb.date_local) >= date({{order_create_start_date}})]]
    and [[date(bb.date_local) <= date({{order_create_end_date}})]]
)
{{#if auto_accept_order == 'yes'}}
where my_mex_auto_accept = 'mex_auto_accept'
{{#endif}}
{{#if auto_accept_order == 'no'}}
where my_mex_auto_accept is null
{{#endif}}
{{#endif}}