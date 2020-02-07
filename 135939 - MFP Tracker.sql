with list_of_promo_code as (
    select distinct manual_input.country, promo_code, reward_id from
    (
        select * from temptables.mex_funded_promo_code_id
        union all
        select * from temptables.mex_funded_promo_code_sg
        union all
        select * from temptables.mex_funded_promo_code_my
        union all
        select * from temptables.mex_funded_promo_code_ph
        union all
        select * from temptables.mex_funded_promo_code_th
        union all
        select * from temptables.mex_funded_promo_code_vn
    ) manual_input
    -- left join public.countries on manual_input.country = countries.name
    where [[start_date <= {{order_create_end_date}}]]
        and [[end_date >= {{order_create_start_date}}]]
        and [[manual_input.country in ({{country}})]]
        and [[reward_id in ({{reward_id}})]]
)
,fo as (
    select
        order_id
        ,short_order_number
        ,last_booking_code
        ,order_state
        ,booking_state
        ,merchant_id
        ,created_time
        ,json_extract_scalar(snapshot_detail, '$.cartWithQuote.promoCodes[0].promoCode') as promo_code
        ,prejoin_food_order.reward_id
        ,cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.promoCodes[0].promoAmountInMin') as bigint) / 100  as total_promo_code_expense
        ,(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.reducedPriceInMinorUnit') as bigint) + cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.promoAmountInMinorUnit') as bigint))/100 as total_basket_size
    from public.prejoin_food_order
    right join list_of_promo_code on cast(prejoin_food_order.reward_id as varchar) = trim(list_of_promo_code.reward_id)
    where [[date(partition_date) >= date({{order_create_start_date}}) - interval '1' day]]
        and [[date(partition_date) <= date({{order_create_end_date}}) + interval '1' day]]
        and [[date(created_time) >= date({{order_create_start_date}})]]
        and [[date(created_time) <= date({{order_create_end_date}})]]
        and json_extract(snapshot_detail, '$.cartWithQuote.promoCodes') is not null --only retrieve orders with promo codes
        and [[city_id in ({{city|noquote}})]]
        and [[country_id in ({{country|noquote}})]]
)
select
    mfp.country_name
    ,mfp.city_name
    ,fo.order_id
    ,fo.short_order_number
    ,fo.last_booking_code
    ,fo.order_state
    ,fo.booking_state
    ,fo.merchant_id
    ,mex.business_name
    ,date(fo.created_time) as date_local
    ,fo.created_time
    ,fo.total_basket_size
    ,fo.promo_code
    ,fo.total_promo_code_expense
    ,mfp.mex_promo_spend as mex_promo_code_spend
    ,case when mfp.order_id is null then 'Not flagged in dashboard' else mfp.product_flag end as indicator
    ,case when fo.order_id is null then 'Not in PFO' else 'In PFO' end as indicator_2
    ,mfp.order_id as dashboard_order_id
from fo
full outer join slide.mfp_orders mfp on fo.order_id = mfp.order_id
left join datamart.dim_merchants mex on fo.merchant_id = mex.merchant_id
where [[date(mfp.date_local) >= date({{order_create_start_date}}) ]]
    and [[date(mfp.date_local) <= date({{order_create_end_date}})]]
    and [[mfp.city_id in ({{city|noquote}})]]
    and [[mfp.country_id in ({{country|noquote}})]]
    and [[cast(mfp.reward_id as varchar) in ({{reward_id}})]]