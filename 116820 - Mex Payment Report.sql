/*116820*/
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
	  ,ROUND(coalesce(fo.order_value, bb.food_sub_total),2) as order_value
	  ,ROUND(fo.mexFunded, 2) as mexFunded
	  ,ROUND(coalesce(fo.tax_perc, fo.tax),2) as tax_perc
	  ,ROUND(fo.tax_value,2) as tax_value
	  ,ROUND(fo.commission_rate, 2) as commission_rate
	  ,ROUND(fo.commission_value,2) as commission_value
	  ,ROUND(fo.order_value - fo.mexFunded + fo.tax_value - fo.commission_value, 2) as net_payable
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



      --gp payout details
      {{#if show_payout_details =='yes'}}
      ,gp_tx.*
      {{#endif}}

    --Food order table
    FROM (select *
            ,cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.foodQuoteInMin.priceInMinorUnit') as double)/100 as order_value
	          ,coalesce(cast(json_extract_scalar(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.discounts') as array<json>)[1], '$.mexFundedAmountInMin') as double), 0)/100 as mexFunded
	          ,coalesce(cast(json_extract(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.merchantCartWithQuoteList') as array<json>)[1], '$.merchantInfoObj.taxRate') as double), prejoin_food_order.tax) as tax_perc
	          ,coalesce(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0)/100  as tax_value
	          ,coalesce(cast(json_extract(cast(json_extract(json_parse(snapshot_detail), '$.cartWithQuote.merchantCartWithQuoteList') as array<json>)[1], '$.merchantInfoObj.commission') as double), prejoin_food_order.commission) as commission_rate
	          ,coalesce(cast(json_extract(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission') as double),0)/100 AS commission_value
            ,coalesce(json_extract_scalar(snapshot_detail, '$.cartWithQuote.promoCodes[0].promoCode'),'') as promo_code
            ,coalesce(cast(json_extract_scalar(json_parse(snapshot_detail), '$.cartWithQuote.promoCodes[0].promoAmountInMin') as double),0) / power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0))
			    as promo_expense
            from public.prejoin_food_order
            where  [[country_id in ({{country|noquote}})]]
            and [[city_id in ({{cities|noquote}})]]
            and [[merchant_id in ({{merchant}})]]
            and [[partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
            and [[partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]) fo
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
    and [[fo.country_id in ({{country|noquote}})]]
    and [[fo.city_id in ({{cities|noquote}})]]
    and [[fo.merchant_id in ({{merchant}})]]
    and [[mex.chain_number in ({{chain_number}})]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{order_create_start_date}})]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{order_create_end_date}})]]
    and [[fo.partition_date >= date_format(date({{ order_create_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
    and [[fo.partition_date <= date_format(date({{ order_create_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
    and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) >= date(date_format(date({{ order_delivered_start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
    and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) <= date(date_format(date({{ order_delivered_end_date }}) , '%Y-%m-%d'))]]

    and [[date(bb.date_local) >= date({{order_create_start_date}})]]
    and [[date(bb.date_local) <= date({{order_create_end_date}})]]
    {{#if delivery_option == 'takeaway'}}
		AND json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '1'
		{{#endif}}
		{{#if delivery_option == 'delivery'}}
		AND (json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') = '0'
		OR json_extract_scalar(fo.snapshot_detail, '$.deliveryOption') is null)
		{{#endif}}