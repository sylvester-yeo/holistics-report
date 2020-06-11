select 
    base.* 
    {{#if show_past_pax_order_history == 'yes'}}
        ,past_pax_history.start_date
        ,past_pax_history.end_date
        ,past_pax_history.total_attempted_orders
        ,past_pax_history.total_completed_orders
        ,past_pax_history.total_basket_size_pre_discount_usd
        ,past_pax_history.total_basket_size_after_discount_usd
    {{#endif}}
from (
    with countries as (
        select
            id
            ,name
            ,currency_symbol
            ,iso_currency
            ,country_timezone
            ,code
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
            -- and cities.id in ({{cities|noquote}})
        group by 1,2,3
    )
    ,tickets as (
        select tickets.*
        from zendesk.tickets
        left join public.countries on tickets.country = lower(countries.code)
        -- left join public.cities on cities.country_id = countries.id
        where tickets.service = 'grabfood'
            and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) >= date( {{transaction_start_date}} ) - interval '14' DAY]]
            and [[date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,-2))) <= date( {{transaction_end_date}} ) + interval '1' DAY]]
            and [[countries.id in ({{country|noquote}})]]
            -- and cities.id in ({{cities|noquote}})
    )
    ,fo as (
        select 
            order_id 
            ,pax_id
            ,last_booking_code
            ,created_time
            ,short_order_number
            ,merchant_id
            ,booking_state
            ,final_state
            ,order_state
            ,pre_accepted_time
            ,ord_order_in_prepare_time
            ,pre_expired_time
            ,pre_cancelled_time
            ,ord_completed_time
            ,city_id
        from public.prejoin_food_order fo
        where [[fo.country_id in ({{country|noquote}})]]
            -- and fo.city_id in ({{cities|noquote}})
            and [[fo.merchant_id in ({{merchant}})]]
            and [[fo.partition_date >= date_format(date( {{transaction_start_date}} ) - interval '45' DAY, '%Y-%m-%d')]]
            and [[fo.partition_date <= date_format(date( {{transaction_end_date}} ) + interval '1' DAY , '%Y-%m-%d')]]
    )
    {{#if source_of_refund == 'tolong'}}
    ,tolong_cs AS (
        SELECT
            case_id
            ,max(case when cs.state = 'GET_ZENDESK_TICKET_DETAILS' then cast(json_extract(json_extract(replace(replace(replace(cs.state_data,'\',''),'"{','{'),'}"','}') , '$.stateData') , '$.Issue')as varchar) else null end ) issue_type
            ,max(case when cs.state = 'CHECK_FOOD_POLICY_ENGINE' then replace(replace(replace(cs.state_data,'\',''),'"{','{'),'}"','}') end) state_data_clean
        from tolong.case_states cs
        where [[date(cs.created_at) >= date({{transaction_start_date}})]]
            and [[date(cs.created_at) <= date({{transaction_end_date}})]]
            -- AND state IN ('GET_ZENDESK_TICKET_DETAILS','CHECK_FOOD_POLICY_ENGINE')
        group by 1
    )
    ,tolong_base AS (
        SELECT
            CAST(element_at(refund_entities_unnested, 'name') AS varchar) AS item_name
            ,CAST(element_at(refund_entities_unnested, 'ID') AS varchar) AS item_id
            ,CAST(element_at(refund_entities_unnested, 'quantity') AS int) AS item_quantity
            ,CAST(element_at(refund_entities_unnested, 'menuType') AS varchar) AS menu_type
            ,CAST(element_at(refund_entities_unnested, 'singleReducedPrice') AS double) AS singleReducedPrice
            ,booking_code
            ,payout_amount
            ,zendesk_ticket_id
            ,issue_type
            ,c.passenger_id
            ,c.created_at
        FROM tolong.cases c
        LEFT JOIN tolong_cs ON c.id = tolong_cs.case_id
        LEFT JOIN tolong.payments ON c.id = payments.case_id AND payments.payout_target_type = 'PAX_GRABPAY'
        left join countries on c.region = countries.code
        cross join unnest(cast(json_extract(json_extract(state_data_clean,'$.stateData'),'$.RefundEntities') as array<map<varchar, json>>)) as refund_entities(refund_entities_unnested)
        WHERE c.category = 'FoodMissingWrongItem'
            and [[countries.id in ({{country|noquote}})]]
            and [[date(c.created_at) >= date({{transaction_start_date}})]]
            and [[date(c.created_at) <= date({{transaction_end_date}})]]
    )
    ,tolong_refunds as (
        select
            tolong_base.passenger_id as pax_id
            ,passengers.name as passenger_name

            /*order details*/
            ,fo.last_booking_code as booking_code
            -- ,from_utc_timestamp(refunds.tx_time, cities.time_zone)  as transaction_time
            ,refunds.first_transaction_time
            ,refunds.last_transaction_time
            ,refunds.no_of_entries_in_grab_money_schema
            ,fo.order_id as order_id
            ,fo.short_order_number as short_order_id
            ,fo.merchant_id as merchant_id
            ,mex.name as merchant_name
            ,mex.chain_name as chain_name
            ,mex.chain_number as chain_number
            -- ,date(substr(item_level_breakdown.created_at,1,10)) as created_time_in_phoenix_utc
            ,from_utc_timestamp(tolong_base.created_at, cities.time_zone)  as refund_created_time_in_tolong

            /*from grab_money schema */
            ,refunds.initial_amount as booking_amount
            ,refunds.credit as refund_amount
            ,refunds.balance as account_balance
            ,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status

            /*tolong related details*/
            ,'Tolong' as source_of_refund
            ,tolong_base.item_name
            ,tolong_base.item_id
            ,cast(tolong_base.singleReducedPrice as double) as singleReducedPrice
            ,tolong_base.item_quantity
            ,tolong_base.singleReducedPrice * tolong_base.item_quantity as total_price
            ,NULL as total_refund_per_item

            /*overall details*/
            ,cast(tolong_base.payout_amount as double) as refund_amount
            ,0 as pax_paid_amount
            ,0 as order_promo_amount
            -- ,item_level_breakdown.total_clawback_amount

            ,tolong_base.issue_type as tolong_issue_type
            ,NULL as placeholder

            {{#if show_sub_item_breakdown == 'yes'}}
            ,NULL as sub_item_name
            ,NULL as sub_item_id
            ,NULL as sub_item_price
            ,NULL as sub_item_quantity
            ,NULL as sub_total_price
            ,NULL as sub_total_refund
            {{#endif}}

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
            ,coalesce(cast(tickets.id as varchar), tolong_base.zendesk_ticket_id) as zendesk_ticket_id
            ,tickets.disposition as zendesk_disposition

        from tolong_base
        left join public.passengers on tolong_base.passenger_id = passengers.id
        left join fo on tolong_base.booking_code = fo.last_booking_code
        left join refunds on refunds.booking_code = tolong_base.booking_code
        LEFT JOIN tickets on tolong_base.zendesk_ticket_id = cast(tickets.id as varchar)
        LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = fo.merchant_id
        LEFT JOIN public.cities ON fo.city_id = cities.id
    )
    {{#endif}}
    {{#if source_of_refund == 'phoenix'}}
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
            -- ,json_parse(refunds.refund_items) as refund_items
            ,replace(replace(replace(refunds.refund_items,'\',''),'"{','{'),'}"','}') as refund_items
        from pax.refunds
        left join countries on refunds.currency = countries.iso_currency
        -- left join public.cities on cities.country_id = countries.id
        where [[date(refunds.year||'-'||refunds.month||'-'||refunds.day) >= date({{transaction_start_date}}) - interval '30' DAY]]
            and [[date(refunds.year||'-'||refunds.month||'-'||refunds.day) <= date({{transaction_end_date}}) + interval '1' DAY]]
            and [[date(substr(refunds.created_at,1,10)) >= date({{transaction_start_date}})]]
            and [[date(substr(refunds.created_at,1,10)) <= date({{transaction_end_date}})]]
            and source_category = 'FOOD'
            and [[countries.id in ({{country|noquote}})]]
            -- and cities.id in ({{cities|noquote}})
    )
    ,item_level_breakdown as (
        select
            raw_order_level_refund.*
            ,item_meta
            ,CAST(element_at(item_meta, 'ItemName') AS varchar) AS item_name
            ,CAST(element_at(item_meta, 'ItemID') AS varchar) AS item_id
            ,CAST(element_at(item_meta, 'ItemPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS item_price
            ,CAST(element_at(item_meta, 'ItemQuantity') AS int) AS item_quantity
            ,CAST(element_at(item_meta, 'TotalItemPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS total_price
            ,CAST(element_at(item_meta, 'TotalRefundPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS total_refund
            ,CASE WHEN json_extract(element_at(item_meta,'RefundSubItems'),'$') = json_parse('[]') then 1 else 0 end as null_indicator
        from raw_order_level_refund
        cross join unnest (cast(json_extract(refund_items,'$') as array<map<varchar, json>>)) x(item_meta)
    )
    ,sub_item_breakdown as (
        select
            *
            ,CAST(element_at(sub_item, 'itemName') AS varchar) AS sub_item_name
            ,CAST(element_at(sub_item, 'itemID') AS varchar) AS sub_item_id
            ,CAST(element_at(sub_item, 'itemPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS sub_item_price
            ,CAST(element_at(sub_item, 'itemQuantity') AS int) AS sub_item_quantity
            ,CAST(element_at(sub_item, 'totalItemPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS sub_total_price
            ,CAST(element_at(sub_item, 'totalRefundPriceInMinorUnits') AS double)/ power(double '10.0', currency_exponent) AS sub_total_refund
        from item_level_breakdown
        cross join unnest(cast(json_extract(element_at(item_meta,'RefundSubItems'),'$') as array<map<varchar, json>>)) x(sub_item)
        where null_indicator = 0 
        union all 
        (
            select 
                *
                ,NULL AS sub_item
                ,NULL AS sub_item_name
                ,NULL as sub_item_id
                ,NULL AS sub_item_price
                ,NULL AS sub_item_quantity
                ,NULL AS sub_total_price
                ,NULL AS sub_total_refund
            from item_level_breakdown
            where null_indicator = 1
        )
    )
    {{#if show_sub_item_breakdown == 'yes'}}
    ,phoenix_refunds as (
        select
            phoenix.passenger_id as pax_id
            ,passengers.name as passenger_name

            /*order details*/
            ,fo.last_booking_code as booking_code
            -- ,from_utc_timestamp(refunds.tx_time, cities.time_zone)  as transaction_time
            ,refunds.first_transaction_time
            ,refunds.last_transaction_time
            ,refunds.no_of_entries_in_grab_money_schema
            ,phoenix.order_id as order_id
            ,fo.short_order_number as short_order_id
            ,fo.merchant_id as merchant_id
            ,mex.name as merchant_name
            ,mex.chain_name as chain_name
            ,mex.chain_number as chain_number
            -- ,date(substr(item_level_breakdown.created_at,1,10)) as created_time_in_phoenix_utc
            ,from_utc_timestamp(phoenix.created_at, cities.time_zone)  as refund_created_time_in_phoenix

            /*from grab_money schema */
            ,refunds.initial_amount as booking_amount
            ,refunds.credit as refund_amount
            ,refunds.balance as account_balance
            ,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status

            /*from phoenix*/
            ,'Phoenix' as source_of_refund
            ,phoenix.item_name
            ,phoenix.item_id as item_id
            ,phoenix.item_price
            ,phoenix.item_quantity
            ,phoenix.total_price
            ,phoenix.total_refund as total_refund_per_item

            /*overall details*/
            ,cast(phoenix.total_refund_amount as double) as total_refund_amount
            ,phoenix.paid_amount as pax_paid_amount
            ,phoenix.promo_amount as order_promo_amount
            -- ,item_level_breakdown.total_clawback_amount

            /*phoenix reasons*/
            ,phoenix.reason_code
            ,phoenix.reason_category_code

            ,sub_item_name
            ,sub_item_id
            ,sub_item_price
            ,sub_item_quantity
            ,sub_total_price
            ,sub_total_refund
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
            ,coalesce(cast(tickets.id as varchar), phoenix.ticket_id) as zendesk_ticket_id
            ,tickets.disposition as zendesk_disposition

        from sub_item_breakdown phoenix
        left join public.passengers on phoenix.passenger_id = passengers.id
        left join fo on phoenix.order_id = fo.order_id
        left join refunds on refunds.booking_code = fo.last_booking_code
        LEFT JOIN tickets on phoenix.ticket_id = cast(tickets.id as varchar)
        LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = phoenix.merchant_id
        LEFT JOIN public.cities ON fo.city_id = cities.id
    )
    {{#endif}}
    {{#if show_sub_item_breakdown == 'no'}}
    ,phoenix_refunds as (
        select
            phoenix.passenger_id as pax_id
            ,passengers.name as passenger_name

            /*order details*/
            ,fo.last_booking_code as booking_code
            -- ,from_utc_timestamp(refunds.tx_time, cities.time_zone)  as transaction_time
            ,refunds.first_transaction_time
            ,refunds.last_transaction_time
            ,refunds.no_of_entries_in_grab_money_schema
            ,phoenix.order_id as order_id
            ,fo.short_order_number as short_order_id
            ,fo.merchant_id as merchant_id
            ,mex.name as merchant_name
            ,mex.chain_name as chain_name
            ,mex.chain_number as chain_number
            -- ,date(substr(item_level_breakdown.created_at,1,10)) as created_time_in_phoenix_utc
            ,from_utc_timestamp(phoenix.created_at, cities.time_zone)  as refund_created_time_in_phoenix

            /*from grab_money schema */
            ,refunds.initial_amount as booking_amount
            ,refunds.credit as refund_amount
            ,refunds.balance as account_balance
            ,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status

            /*from phoenix*/
            ,'Phoenix' as source_of_refund
            ,phoenix.item_name
            ,phoenix.item_id as item_id
            ,phoenix.item_price
            ,phoenix.item_quantity
            ,phoenix.total_price
            ,phoenix.total_refund as total_refund_per_item

            /*overall details*/
            ,cast(phoenix.total_refund_amount as double) as total_refund_amount
            ,phoenix.paid_amount as pax_paid_amount
            ,phoenix.promo_amount as order_promo_amount
            -- ,item_level_breakdown.total_clawback_amount

            /*phoenix reasons*/
            ,phoenix.reason_code
            ,phoenix.reason_category_code

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
            ,coalesce(cast(tickets.id as varchar), phoenix.ticket_id) as zendesk_ticket_id
            ,tickets.disposition as zendesk_disposition
        from item_level_breakdown phoenix
        left join public.passengers on phoenix.passenger_id = passengers.id
        left join fo on phoenix.order_id = fo.order_id
        left join refunds on refunds.booking_code = fo.last_booking_code
        LEFT JOIN tickets on phoenix.ticket_id = cast(tickets.id as varchar)
        LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = phoenix.merchant_id
        LEFT JOIN public.cities ON fo.city_id = cities.id
    )
    {{#endif}}
    {{#endif}}
    --final query
    {{#if show_item_level_breakdown == 'yes'}}
    select * from (
        {{#if source_of_refund == 'phoenix'}}
        select * from phoenix_refunds
        {{#if source_of_refund == 'tolong'}}
        union all
        (select * from tolong_refunds)
        {{#endif}}
        {{#else}}
        select * from tolong_refunds
        {{#endif}}
    )
    where [[merchant_id in {{merchant}}]]

    --old query
    {{#else}}
    {{#if source_of_refund == 'tolong'}}
    ,tolong_agg as (
        SELECT
            booking_code
            ,avg(payout_amount) as payout_amount
        from tolong_base 
        group by 1
    )
    {{#endif}}
    select
        fo.pax_id
        ,passengers.name as passenger_name
        ,fo.last_booking_code as booking_code
        ,refunds.first_transaction_time
        ,refunds.last_transaction_time
        ,refunds.no_of_entries_in_grab_money_schema
        ,refunds.initial_amount as booking_amount
        ,refunds.credit as refund_amount
        ,refunds.balance as account_balance
        ,case when refunds.credit = refunds.initial_amount then 'Full Refund' else 'Partial Refund' end as refund_status
        ,case
        {{#if source_of_refund == 'tolong'}}
        when tolong_agg.booking_code is not null then 'Tolong'
        {{#endif}}
        {{#if source_of_refund == 'phoenix'}}
        when raw_order_level_refund.order_id is not null then 'Phoenix'
        {{#endif}}
        else NULL end as source_of_refund
        ,coalesce(
            {{#if source_of_refund == 'phoenix'}}
            raw_order_level_refund.total_refund_amount, 
            {{#endif}}
            {{#if source_of_refund == 'tolong'}}
            tolong_agg.payout_amount,
            {{#endif}}
            0) as amount_captured_in_phoenix_tolong
        ,fo.order_id as order_id
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
    from fo
    left join refunds on refunds.booking_code = fo.last_booking_code
    {{#if source_of_refund == 'tolong'}}
    left join tolong_agg on tolong_agg.booking_code = fo.last_booking_code
    {{#endif}}
    {{#if source_of_refund == 'phoenix'}}
    left join raw_order_level_refund on raw_order_level_refund.order_id = fo.order_id
    {{#endif}}
    left join public.passengers on fo.pax_id = passengers.id
    LEFT JOIN tickets on fo.last_booking_code = tickets.booking_code
    LEFT JOIN food_data_service.merchants mex ON mex.merchant_id = fo.merchant_id
    LEFT JOIN public.cities ON fo.city_id = cities.id   

    where 
        refunds.booking_code is not null 
        {{#if source_of_refund == 'tolong'}}
        or tolong_agg.booking_code is not null 
        {{#endif}}
        {{#if source_of_refund == 'phoenix'}}
        or raw_order_level_refund.order_id is not null 
        {{#endif}}
    {{#endif}}
) base 

{{#if show_past_pax_order_history == 'yes'}}
left join (
    select
        passenger_id
        ,date({{past_pax_history_start_date}}) as start_date
        ,date({{past_pax_history_end_date}}) as end_date
        ,count(1) as total_attempted_orders
        ,sum(case when booking_state_simple = 'COMPLETED' then 1 else 0 end) as total_completed_orders
        ,sum(case when booking_state_simple = 'COMPLETED' then basket_size/fx_one_usd else 0 end) as total_basket_size_pre_discount_usd
        ,sum(case when booking_state_simple = 'COMPLETED' then (basket_size - promo_expense)/fx_one_usd else 0 end) as total_basket_size_after_discount_usd
    from datamart_grabfood.base_bookings
    where [[date(date_local) >= date({{past_pax_history_start_date}})]]
        and [[date(date_local) <= date({{past_pax_history_end_date}})]]
        and [[country_id in ({{country|noquote}})]]
        and [[city_id in ({{cities|noquote}})]]
    group by 1,2,3
) past_pax_history on base.pax_id = past_pax_history.passenger_id
{{#endif}}