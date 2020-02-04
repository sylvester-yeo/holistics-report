with base as (
    select
        ratings.*
        ,cities.country_id
        ,dim_merchants.business_name
        ,dim_merchants.merchant_name
    from slide.voc_ratings_feedbacks ratings
    left join datamart.dim_merchants on ratings.merchant_id = dim_merchants.merchant_id
    inner join datamart.dim_taxi_types on ratings.taxi_type_id = dim_taxi_types.id
    left join public.cities on ratings.city_id = cities.id
    where
        ratings.date_local >= date_format(dim_taxi_types.start_at, '%Y-%m-%d')
        and ratings.date_local < date_format(dim_taxi_types.end_at, '%Y-%m-%d')
        and dim_taxi_types.name = 'GrabFood'
        and [[date(date_local) >= date({{start_date}}) ]]
        and [[date(date_local) <= date({{end_date}}) ]]
        and ([[ratings.merchant_id in ({{merchant_id}})]] or [[dim_merchants.business_name in ({{merchant_id}})]])
        and [[ratings.city_id in ({{cities|noquote}})]]
        and [[cities.country_id in ({{country|noquote}})]]
        {{#if only_completed_orders == 'completed_orders'}}
        and booking_state_simple = 'COMPLETED'
        {{#endif}}
)
select
    country_id
    ,city_id
    {{#if merchant_level_breakdown == 'aggregate_by_merchant'}}
    ,merchant_id
    ,merchant_name
    {{#else}}
    ,null as merchant_id
    ,null as merchant_name
    {{#endif}}
    ,business_name
    ,null as order_id
    {{#if aggregate_by == 'By Week'}}
    ,date_trunc('week', date(date_local)) as week_of
    {{#endif}}
    {{#if aggregate_by == 'By Month'}}
    ,date_trunc('month', date(date_local)) as month_of
    {{#endif}}
    {{#if aggregate_by == 'By Day'}}
    ,date(date_local) as date_local
    {{#endif}}
    ,'Aggregated by Ratings' as aggregation_level
    ,pax_rating_for_merchant
    ,'All' as rating_category
    ,null as specific_feedback
    ,count(1) as no_of_orders
from base
group by 1,2,3,4,5,6,7,8,9,10,11

union all
(
    select
        country_id
        ,city_id
        {{#if merchant_level_breakdown == 'aggregate_by_merchant'}}
        ,merchant_id
        ,merchant_name
        {{#else}}
        ,null as merchant_id
        ,null as merchant_name
        {{#endif}}
        ,business_name
        ,null as order_id
        {{#if aggregate_by == 'By Week'}}
        ,date_trunc('week', date(date_local)) as week_of
        {{#endif}}
        {{#if aggregate_by == 'By Month'}}
        ,date_trunc('month', date(date_local)) as month_of
        {{#endif}}
        {{#if aggregate_by == 'By Day'}}
        ,date(date_local) as date_local
        {{#endif}}
        ,'Aggregated by Ratings and Category' as aggregation_level
        ,pax_rating_for_merchant
        ,trim(rating_category) as rating_category
        ,null as specific_feedback
        ,count(1) as no_of_orders
    from base
    cross join unnest (split(pax_rating_category,';')) x(rating_category)
    group by 1,2,3,4,5,6,7,8,9,10,11
)

{{#if include_individual_order == 'yes'}}
union all
(
    select
        country_id
        ,city_id
        ,merchant_id
        ,merchant_name
        ,business_name
        ,order_id
        ,date(date_local) as date_local
        ,'Order Level' as aggregation_level
        ,pax_rating_for_merchant
        ,pax_rating_category
        ,pax_feedback_for_merchant as specific_feedback
        ,null as no_of_orders
    from base
    where pax_rating_for_merchant is not null
)
{{#endif}}