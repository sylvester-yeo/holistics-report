with refunds as (
  select refund.*
    ,ptb.passenger_id
    ,ptb.booking_fee
    ,ptb.initial_amount
  from grab_money.booking_refund_approved refund
  inner join grab_money.payment_to_booking ptb
  on refund.booking_code=ptb.booking_code
  where ptb.driver_id=-999 and refund.tx_action <> 4
  and [[(concat(refund.year,'-',refund.month,'-',refund.day)) >= date_format(date({{ transaction_start_date }}) - interval '1' DAY, '%Y-%m-%d')]]
  and [[(concat(refund.year,'-',refund.month,'-',refund.day)) <= date_format(date({{ transaction_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
  and [[date(refund.tx_time) >= date({{transaction_start_date}})]]
  and [[date(refund.tx_time) <= date({{transaction_end_date}})]]
  and [[(concat(ptb.year,'-',ptb.month,'-',ptb.day)) >= date_format(date({{ transaction_start_date }}) - interval '30' DAY, '%Y-%m-%d')]]
  and [[(concat(ptb.year,'-',ptb.month,'-',ptb.day)) <= date_format(date({{ transaction_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
)
,tickets as (
  select *
  from zendesk.tickets
    where tickets.service = 'grabfood'
        and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) >= date({{ transaction_start_date }}) - interval '1' DAY]]
        and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) <= date({{ transaction_end_date }}) + interval '1' DAY ]]
)
select
	refunds.passenger_id
	,passengers.name as passenger_name
	,refunds.booking_code as booking_code
	,from_utc_timestamp(refunds.tx_time, cities.time_zone)  as transaction_time
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
from
	refunds
left join public.passengers on refunds.passenger_id = passengers.id
left join grab_food.order_details od on od.booking_code = refunds.booking_code
left join public.prejoin_food_order fo on od.order_id = fo.order_id
LEFT JOIN tickets on refunds.booking_code = tickets.booking_code
LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = fo.merchant_id
LEFT JOIN public.cities ON fo.city_id = cities.id
where [[fo.country_id in ({{country|noquote}})]]
  and [[fo.city_id in ({{cities|noquote}})]]
  and [[fo.merchant_id in ({{merchant}})]]
	--and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) >= date({{ start_date }})]]
  --and [[date(from_utc_timestamp(fo.created_time, cities.time_zone)) <= date({{ end_date }})]]
  and [[fo.partition_date >= date_format(date({{ transaction_start_date }}) - interval '45' DAY, '%Y-%m-%d')]]
  and [[fo.partition_date <= date_format(date({{ transaction_end_date }}) + interval '1' DAY , '%Y-%m-%d')]]
  --and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) >= date(date_format(date({{ start_date }}) - interval '1' DAY, '%Y-%m-%d'))]]
  --and [[date(from_utc_timestamp(fo.ord_completed_time, cities.time_zone)) <= date(date_format(date({{ end_date }}) , '%Y-%m-%d'))]]
  and [[(concat(od.year,'-',od.month,'-',od.day)) >= date_format(date({{ transaction_start_date }}) - interval '45' DAY, '%Y-%m-%d')]]
  and [[(concat(od.year,'-',od.month,'-',od.day)) <= date_format(date({{ transaction_end_date }}) + interval '1' DAY, '%Y-%m-%d')]]