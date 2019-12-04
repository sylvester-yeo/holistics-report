/*84271*/
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
	  ,bb.promo_code
	  ,bb.promo_expense as promo_amount
      ,ROUND(fo.order_value_pre_tax,2) as order_value_pre_tax
      ,ROUND(fo.order_value_with_tax,2) as order_value_with_tax
	  ,fo.tax as tax_perc
	  ,ROUND(fo.tax_value,2) as tax_value
      --,round((coalesce(fo.sub_total,bb.food_sub_total) * fo.tax),2) as tax_value
	  ,fo.commission as commission_rate
	  ,ROUND(fo.mex_commission,2) as mex_commission
	  ,ROUND((fo.order_value_pre_tax - fo.mex_commission + fo.tax_value), 2) AS net_payable_mex

	  --pay status
       ,CASE WHEN fo.pre_accepted_time IS NOT NULL and fo.ord_completed_time IS NOT NULL THEN 'pay'
             WHEN fo.ord_completed_time IS NOT NULL THEN 'pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = 'UNALLOCATED') THEN 'no pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and ( fo.final_state = 'REALLOCATION_FAILED') THEN 'pay'
             When fo.pre_accepted_time IS NOT NULL and aa.auto_accept_order is not null and (fo.final_state = '' and fo.booking_state = 'UNALLOCATED') THEN 'no pay'
             WHEN fo.pre_accepted_time IS NOT NULL and fo.final_state = 'CANCELLED_OPERATOR' and fo.cancel_msg = 'Food is out of stock' THEN 'no pay'
             WHEN fo.pre_accepted_time IS NOT NULL THEN 'pay' 
             ELSE 'no pay' END AS pay_status
      ,case when json_extract_scalar(json_parse(fo.metadata), '$.FPTAcceptedBy') = '3' or aa.auto_accept_order is not null then 'auto-accept' else null end as mex_auto_accept
      ,case when aa.auto_accept_order is not null then 'mex_auto_accept' else null end as mex_auto_accept_status
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



      --gp payout details
      {{#if show_payout_details =='yes'}}
      ,gp_tx.*
      {{#endif}}

    --Food order table
    FROM (
        	select
            prejoin_food_order.*
            ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then
                cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) - COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0)
                else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) end)/100 as order_value_pre_tax
            ,COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double)/100,0) as tax_value
            ,(case when json_extract_scalar(snapshot_detail, '$.newTaxFlow') = 'true' then cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double)
                else cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double) + COALESCE(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0) end)/100
                as order_value_with_tax
            ,cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.totalQuoteInMin.mexCommission') as double)/100 as mex_commission
        from public.prejoin_food_order
        left join public.cities on prejoin_food_order.city_id = cities.id
        where --partition_date = '2019-09-10'
            [[prejoin_food_order.country_id in ({{country|noquote}})]]
            and [[prejoin_food_order.city_id in ({{cities|noquote}})]]
            and [[merchant_id in ({{merchant}})]]
            and [[partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
            and [[partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
            and [[date(from_utc_timestamp(ord_completed_time, cities.time_zone)) >= date(date_format(date({{ order_delivered_start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
            and [[date(from_utc_timestamp(ord_completed_time, cities.time_zone)) <= date(date_format(date({{ order_delivered_end_date }}) , '%Y-%m-%d'))]]
            and [[date(from_utc_timestamp(created_time, cities.time_zone)) >= date({{order_create_start_date}})]]
            and [[date(from_utc_timestamp(created_time, cities.time_zone)) <= date({{order_create_end_date}})]]
            and prejoin_food_order.country_id in (1,4)

    ) fo
    --base bookings
    left join datamart.base_bookings bb on fo.order_id = bb.order_id

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
    and currency in ('SGD','MYR')
    and txtype = 60
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
    --LEFT JOIN grab_mall.grab_mall_seller ON grab_mall_seller.id = fo.merchant_id

    --merchant and bank details
    left join food_data_service.merchants as mex on mex.merchant_id = fo.merchant_id
    left join food_data_service.merchant_contracts as mex_con on mex_con.merchant_id = mex.merchant_id
    left join xtramile.mlm_bank_details on mlm_bank_details.store_grab_id = mex_con.grabpay_grabid
	  left join xtramile.mlm_banks on mlm_banks.id = mlm_bank_details.bank_name_id
    left join public.drivers on fo.merchant_id = drivers.identification_card_number
    left join paysi.bank_detail det on drivers.id = det.user_id
    left join paysi.bank bank on det.bank_id = bank.id
    left join slide.food_pax_cancel_reason_mapping map on fo.cancel_code = map.cancel_code
    left join public.cities on fo.city_id = cities.id
    WHERE
    --integrated orders
    order_type = 1
    --SG & MY
    and bb.vertical = 'GrabFood'
    and [[mex.chain_number in ({{chain_number}})]]
    and [[date(bb.date_local) >= date({{order_create_start_date}})]]
    and [[date(bb.date_local) <= date({{order_create_end_date}})]]