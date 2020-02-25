with countries as (
    select 
        id
        ,name
        ,currency_symbol
        ,iso_currency
        ,country_timezone
    from public.countries 
    where id <= 6 --filter out the main 6 countries
)
,refunds as (
    select
        ptb.passenger_id
        ,refund.booking_code
        ,ptb.initial_amount
        ,count(1) as no_of_entries_in_grab_money_schema
        ,sum(refund.credit) as credit
        ,max(refund.balance) as balance
        ,min(from_utc_timestamp(refund.tx_time, countries.country_timezone)) as first_transaction_time
        ,max(from_utc_timestamp(refund.tx_time, countries.country_timezone)) as last_transaction_time
    from grab_money.booking_refund_approved refund
    inner join grab_money.payment_to_booking ptb
        on refund.booking_code=ptb.booking_code
    left join countries on refund.currency = countries.iso_currency
    -- left join public.cities on cities.country_id = countries.id
    where ptb.driver_id=-999 and refund.tx_action <> 4
        and [[(concat(refund.year,'-',refund.month,'-',refund.day)) >= date_format(date({{transaction_start_date}}) - interval '1' DAY, '%Y-%m-%d')]]
        and [[(concat(refund.year,'-',refund.month,'-',refund.day)) <= date_format(date({{transaction_end_date}}) + interval '1' DAY , '%Y-%m-%d')]]
        and [[date(refund.tx_time) >= date({{transaction_start_date}})]]
        and [[date(refund.tx_time) <= date({{transaction_end_date}})]]
        and [[(concat(ptb.year,'-',ptb.month,'-',ptb.day)) >= date_format(date({{transaction_start_date}}) - interval '30' DAY, '%Y-%m-%d')]]
        and [[(concat(ptb.year,'-',ptb.month,'-',ptb.day)) <= date_format(date({{transaction_end_date}}) + interval '1' DAY , '%Y-%m-%d')]]
        and [[countries.id in ({{country|noquote}})]]
        -- and [[cities.id in ({{cities|noquote}})]]
    group by 1,2,3
)
/*,tolong_base as (
    select
        *
        ,json_parse(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(state_data,'\\',''), '"{','{'), '}","','},"'),'"","','","'),':","',':"","')) as json_data
    from tolong.case_states cs
    left join tolong.cases c
        on cs.case_id = c.id
    where state_data like '%"Action\":\"Appease\"%'
        and state = 'CHECK_FOOD_POLICY_ENGINE'
        and state_data like '%\"totalReducedPrice\":0%'
        and category = 'FoodMissingWrongItem'
        and date(c.created_at) >=  date({{transaction_start_date}})
)
,tolong_item_level as (
    select
        json_extract_scalar(json_data, '$.AmountPaid') as amount_paid
        ,json_extract_scalar(json_data, '$.AmountToGive') as amount_to_give
        ,json_extract_scalar(json_data, '$.OrderID') as order_id
        ,*
    from tolong_base
    cross join unnest(cast(json_extract(json_data,'$.RefundEntities') as array<json>)) x(indiv_items)
)*/
,tickets as (
    select tickets.*
    from zendesk.tickets
    left join public.countries on tickets.country = lower(countries.code)
    -- left join public.cities on cities.country_id = countries.id
    where tickets.service = 'grabfood'
        and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) >= date( {{transaction_start_date}} ) - interval '14' DAY]]
        and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) <= date( {{transaction_end_date}} ) + interval '1' DAY]]
        and [[countries.id in ({{country|noquote}})]]
        -- and [[cities.id in ({{cities|noquote}})]]
)
{{#if show_past_pax_order_history == 'yes'}}
,past_pax_history as (
    select
        passenger_id
        ,date({{transaction_start_date}}) - interval '14' day as start_date
        ,date({{transaction_start_date}}) as end_date
        ,count(1) as total_attempted_orders
        ,sum(case when booking_state_simple = 'COMPLETED' then 1 else 0 end) as total_completed_orders
        ,sum(case when booking_state_simple = 'COMPLETED' then basket_size/fx_one_usd else 0 end) as total_basket_size_pre_discount_usd
        ,sum(case when booking_state_simple = 'COMPLETED' then (basket_size - promo_expense)/fx_one_usd else 0 end) as total_basket_size_after_discount_usd
    from datamart_grabfood.base_bookings
    where [[date(date_local) >= date_add('day', - cast(({{past_number_of_days}}) as int), date({{transaction_start_date}}))]]
        and [[date(date_local) < date({{transaction_start_date}})]]
        and [[country_id in ({{country|noquote}})]]
        and [[city_id in ({{cities|noquote}})]]
    group by 1,2,3
)
{{#endif}}
{{#if show_item_level_breakdown == 'yes'}}
,raw_order_level_refund as (
    select
        refund_id
        ,customer_id as passenger_id
        ,vendor_id as merchant_id
        ,customer_sequence_number
        ,status
        ,reason_code
        ,reason_category_code
        ,source_id as order_id
        ,cast(regexp_replace(refunds.created_at,'T', ' ') as timestamp) as created_at
        ,cast(regexp_replace(refunds.updated_at,'T', ' ') as timestamp) as updated_at
        -- ,fo.last_booking_code
        ,total_refund_amount / power(double '10.0', currency_exponent) as total_refund_amount
        ,paid_amount / power(double '10.0', currency_exponent) as paid_amount
        ,promo_amount / power(double '10.0', currency_exponent) as promo_amount
        ,total_clawback_amount / power(double '10.0', currency_exponent) as total_clawback_amount
        ,currency_exponent
        ,refund_type
        ,replace(ticket_id, 'zendesk:') as ticket_id /*remove the word zendesk to join on zendesk table*/
        ,remarks
        ,json_parse(refunds.refund_items) as refund_items
    from pax.refunds
    left join countries on refunds.currency = countries.iso_currency
    -- left join public.cities on cities.country_id = countries.id
    where [[date(refunds.year||'-'||refunds.month||'-'||refunds.day) >= date( {{transaction_start_date}} ) - interval '30' DAY]]
        and [[date(refunds.year||'-'||refunds.month||'-'||refunds.day) <= date( {{transaction_end_date}} ) + interval '1' DAY]]
        and [[date(substr(refunds.created_at,1,10)) >= date({{transaction_start_date}})]]
        and [[date(substr(refunds.created_at,1,10)) <= date({{transaction_end_date}})]]
        and source_category = 'FOOD'
        and [[countries.id in ({{country|noquote}})]]
        -- and [[cities.id in ({{cities|noquote}})]]
)
,item_level_breakdown as (
    select
        raw_order_level_refund.*
        ,item_meta
        ,json_extract_scalar(item_meta, '$.ItemName') as item_name
        ,cast(json_extract_scalar(item_meta, '$.ItemPriceInMinorUnits') as double)/ power(double '10.0', currency_exponent) as item_price
        ,cast(json_extract_scalar(item_meta, '$.ItemQuantity') as double) as item_quantity
        ,cast(json_extract_scalar(item_meta, '$.TotalItemPriceInMinorUnits') as double)/ power(double '10.0', currency_exponent) as total_price
        ,cast(json_extract_scalar(item_meta, '$.TotalRefundPriceInMinorUnits') as double)/ power(double '10.0', currency_exponent) as total_refund
    from raw_order_level_refund
    cross join unnest (cast(refund_items as array<json>)) x(item_meta)
)
select
	item_level_breakdown.passenger_id
	,passengers.name as passenger_name

    /*order details*/
	,fo.last_booking_code as booking_code
    -- ,from_utc_timestamp(refunds.tx_time, cities.time_zone)  as transaction_time
    ,refunds.first_transaction_time
    ,refunds.last_transaction_time
    ,refunds.no_of_entries_in_grab_money_schema
	,item_level_breakdown.order_id as order_id
 	,fo.short_order_number as short_order_id
	,fo.merchant_id as merchant_id
	,mex.name as merchant_name
	,mex.chain_name as chain_name
    ,mex.chain_number as chain_number
    -- ,date(substr(item_level_breakdown.created_at,1,10)) as created_time_in_phoenix_utc
    ,from_utc_timestamp(item_level_breakdown.created_at, cities.time_zone)  as refund_created_time_in_phoenix

	/*from grab_money schema */
    ,refunds.initial_amount as booking_amount
	,refunds.credit as refund_amount
    ,refunds.balance as account_balance
	,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status

    /*item_level breakdown*/
    ,item_level_breakdown.item_name
    ,item_level_breakdown.item_price
    ,item_level_breakdown.item_quantity
    ,item_level_breakdown.total_price
    ,item_level_breakdown.total_refund as total_refund_per_item

    /*overall details*/
    ,item_level_breakdown.total_refund_amount
    ,item_level_breakdown.paid_amount as pax_paid_amount
    ,item_level_breakdown.promo_amount as order_promo_amount
    -- ,item_level_breakdown.total_clawback_amount

    /*phoenix reasons*/
    ,item_level_breakdown.reason_code
    ,item_level_breakdown.reason_category_code

    /*order time stamps*/
	,date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local
	,from_utc_timestamp(coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time), cities.time_zone) as merchant_accept_time
	, case when fo.order_state = 11 then 'COMPLETED'
            when fo.final_state = '' and fo.booking_state is null and fo.pre_expired_time is not null then 'MAX_IGNORE'
            when fo.final_state = '' and fo.booking_state is null and fo.pre_cancelled_time is not null then 'PAX_REQUEST_CANCEL'
		      	when fo.final_state = '' then fo.booking_state
            else final_state end as final_state
    ,date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) as order_delivery_date_local

    /* zendesk ticket */
    ,coalesce(cast(tickets.id as varchar), item_level_breakdown.ticket_id) as zendesk_ticket_id
    ,tickets.disposition as zendesk_disposition

    {{#if show_past_pax_order_history == 'yes'}}
    ,past_pax_history.start_date
    ,past_pax_history.end_date
    ,past_pax_history.total_attempted_orders
    ,past_pax_history.total_completed_orders
    ,past_pax_history.total_basket_size_pre_discount_usd
    ,past_pax_history.total_basket_size_after_discount_usd
    {{#endif}}

from item_level_breakdown
left join public.passengers on item_level_breakdown.passenger_id = passengers.id
left join (
    select *
    from public.prejoin_food_order fo
    where [[fo.country_id in ({{country|noquote}})]]
        and [[fo.city_id in ({{cities|noquote}})]]
        and [[fo.merchant_id in ({{merchant}})]]
        and [[fo.partition_date >= date_format(date( {{transaction_start_date}} ) - interval '45' DAY, '%Y-%m-%d')]]
        and [[fo.partition_date <= date_format(date( {{transaction_end_date}} ) + interval '1' DAY , '%Y-%m-%d')]]
    )
fo on item_level_breakdown.order_id = fo.order_id
left join refunds on refunds.booking_code = fo.last_booking_code
LEFT JOIN tickets on item_level_breakdown.ticket_id = cast(tickets.id as varchar)
{{#if show_past_pax_order_history == 'yes'}}
    left join past_pax_history on item_level_breakdown.passenger_id = past_pax_history.passenger_id
{{#endif}}
LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = item_level_breakdown.merchant_id
LEFT JOIN public.cities ON fo.city_id = cities.id

{{#else}}
select
	refunds.passenger_id
	,passengers.name as passenger_name
	,refunds.booking_code as booking_code
	,refunds.first_transaction_time
    ,refunds.last_transaction_time
    ,refunds.no_of_entries_in_grab_money_schema
	,refunds.initial_amount as booking_amount
	,refunds.credit as refund_amount
    ,refunds.balance as account_balance
	,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status
	,od.order_id as order_id
 	,fo.short_order_number as short_order_id
	,fo.merchant_id as merchant_id
	,mex.name as merchant_name
	,mex.chain_name as chain_name
    ,mex.chain_number as chain_number
	,date(from_utc_timestamp(fo.created_time, cities.time_zone)) as order_create_date_local
	,from_utc_timestamp(coalesce(fo.pre_accepted_time,fo.ord_order_in_prepare_time), cities.time_zone) as merchant_accept_time
	, case when fo.order_state = 11 then 'COMPLETED'
            when fo.final_state = '' and fo.booking_state is null and fo.pre_expired_time is not null then 'MAX_IGNORE'
            when fo.final_state = '' and fo.booking_state is null and fo.pre_cancelled_time is not null then 'PAX_REQUEST_CANCEL'
		      	when fo.final_state = '' then fo.booking_state
            else final_state end as final_state
    ,date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) as order_delivery_date_local
    ,tickets.id as zendesk_ticket_id
    ,tickets.disposition as zendesk_disposition

    {{#if show_past_pax_order_history == 'yes'}}
    ,past_pax_history.start_date
    ,past_pax_history.end_date
    ,past_pax_history.total_attempted_orders
    ,past_pax_history.total_completed_orders
    ,past_pax_history.total_basket_size_pre_discount_usd
    ,past_pax_history.total_basket_size_after_discount_usd
    {{#endif}}
from refunds
left join public.passengers on refunds.passenger_id = passengers.id
left join grab_food.order_details od on od.booking_code = refunds.booking_code
left join public.prejoin_food_order fo on od.order_id = fo.order_id
LEFT JOIN tickets on refunds.booking_code = tickets.booking_code
{{#if show_past_pax_order_history == 'yes'}}
    left join past_pax_history on refunds.passenger_id = past_pax_history.passenger_id
{{#endif}}
LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = fo.merchant_id
LEFT JOIN public.cities ON fo.city_id = cities.id
where
    [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{ start_date }})]]
    and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{ end_date }})]]
    and [[fo.partition_date >= date_format(date({{ transaction_start_date }}) - interval '45' DAY, '%Y-%m-%d')]]
    and [[fo.partition_date <= date_format(date({{ transaction_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
    --and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) >= date(date_format(date({{ start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
    --and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) <= date(date_format(date({{ end_date }}) , '%Y-%m-%d'))]]
    and [[(concat(od.year,'-',od.month,'-',od.day)) >= date_format(date({{ transaction_start_date }}) - interval '45' DAY, '%Y-%m-%d')]]
    and [[(concat(od.year,'-',od.month,'-',od.day)) <= date_format(date({{ transaction_end_date }}) + interval '1' DAY, '%Y-%m-%d')]]
{{#endif}}