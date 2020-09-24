with agg as (
    select
        {{#if dimension_split == 'merchant'}}
            mex.merchant_name as merchant_business_name
            ,base.merchant_id as merchant_id_business_name
            ,mex.business_name as business_name
            ,'By Merchant' as business_merchant_aggregation
            ,mex.vertical
        {{#else}}
            mex.business_name as merchant_business_name
            ,mex.business_name as merchant_id_business_name
            ,mex.business_name as business_name
            ,'By Brand' as business_merchant_aggregation
            ,mex.vertical
        {{#endif}}

        ,base.country_name
        ,base.city_name
        ,base.partner_status
        ,is_bd_account
        ,is_bd_partner
        ,base.business_model
        ,case when mex.am_name is null then 'Non-AM' else 'AM' end as am_tagging

        {{#if level_of_aggregation == 'Total'}}
            ,'Total' as time_aggregation
            ,[[date({{start_date}})]] as start_date
            ,[[date({{end_date}})]] as end_date
        {{#endif}}
        {{#if level_of_aggregation == 'By Month'}}
            ,'By Month' as time_aggregation
            ,case when date_trunc('month',date(date_local)) <= date({{start_date}})
                then date({{start_date}})
                else date_trunc('month',date(date_local))
                end as start_date
            ,case when (date_trunc('month', date(date_local)) + interval '1' month - interval '1' day) <= date({{end_date}})
                then (date_trunc('month', date(date_local)) + interval '1' month - interval '1' day)
                else date({{end_date}})
                end as end_date
        {{#endif}}
        {{#if level_of_aggregation == 'By Week'}}
            ,'By Week' as time_aggregation
            ,case when date_trunc('week',date(date_local)) <= date({{start_date}})
                then date({{start_date}})
                else date_trunc('week',date(date_local))
                end as start_date
            ,case when (date_trunc('week', date(date_local)) + interval '6' day) <= date({{end_date}})
                then (date_trunc('week', date(date_local)) + interval '6' day)
                else date({{end_date}})
                end as end_date
        {{#endif}}
        {{#if level_of_aggregation == 'By Day'}}
            ,'By Day' as time_aggregation
            ,date(date_local) as start_date
            ,date(date_local) as end_date
        {{#endif}}

        ,sum(gmv_usd_gf) as gmv_usd
        ,sum(gmv_usd_gf + mfc_gsheet_mex_promo_spend_usd + mfc_gsheet_grab_promo_spend_usd) as adj_gmv
        ,sum(basket_size) as basket_size_usd
        ,sum(basket_size + mfc_gsheet_mex_promo_spend_usd + mfc_gsheet_grab_promo_spend_usd) as adj_basket_size
        ,sum(sub_total) as sub_total_usd
        ,sum(all_incoming_orders_gf) as all_incoming_orders
        ,sum(completed_orders_gf) as completed_orders
        ,sum(cancellations) as total_cancellations
        ,sum(delivery_fare_gf) as delivery_fare_usd
        ,sum(dax_delivery_fare) as dax_delivery_fare_usd
        ,sum(mex_commission) as mex_commission_usd
        ,sum(driver_commission) as driver_commission_usd
        ,sum(coalesce(sof_usd,0) + coalesce(convenience_fee_usd,0)+ coalesce(pax_platform_fee_usd,0) + coalesce(shopper_fee_usd,0) ) as order_fee_usd
        -- ,sum(pax_platform_fee_usd) as platform_fee_usd
        ,sum(coalesce(incentives_usd,0) + coalesce(spot_incentive_bonus_usd,0)+coalesce(tsp_subsidy_usd,0)) as total_incentives_spend_usd
        ,sum(incentives_usd) as incentives_usd
        ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
        ,sum(tsp_subsidy_usd) as tsp_subsidy_usd
        ,sum(total_partner_base_for_mex_commission) as total_partner_base_for_mex_commission
        ,sum(total_promo_spend_usd) as total_promo_spend_usd
        ,sum(total_grab_promo_spend_usd) as total_grab_promo_spend_usd
        ,sum(total_mfd_usd) as total_mfd_usd
        ,sum(total_mfc_mex_promo_spend_usd) as total_mfc_mex_promo_spend_usd
        ,sum(mfp_mex_promo_spend_usd) as mfp_mex_promo_spend_usd
        ,sum(ssc_expense_usd) as ssc_expense_usd
        ,sum(ad_spend_usd) as ad_spend_usd
        ,sum(batched_orders) as batched_orders

        -- ,avg(fx_one_usd) as avg_fx_one_usd
        ,avg(rer.exchange_one_usd) as exchange_one_usd
    from slide.gf_mex_bd_base_v2 base
    left join datamart.dim_merchants mex on base.merchant_id = mex.merchant_id
    left join public.cities on cities.name = base.city_name
    left join public.countries on countries.name = base.country_name
    LEFT JOIN datamart.ref_exchange_rates rer on countries.id = rer.country_id and (base.date_local between rer.start_date and rer.end_date)
    where [[partition_date_local >= date({{start_date}})]]
        and [[partition_date_local <= date({{end_date}})]]
        and [[cities.id in ({{cities|noquote}})]]
        and [[countries.id in ({{country_|noquote}})]]
        and ([[mex.merchant_id in ({{merchant_id}})]] or [[mex.business_name in ({{merchant_id}})]])
        and [[mex.business_type_name in ({{business_type}})]]
        and [[mex.vertical in ({{vertical}})]]
    {{#if level_of_aggregation == 'Total'}}
        group by 1,2,3,4,5,6,7,8,9,10,11,12
    {{#else}}
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    {{#endif}}
)
select
    agg.*
    ,driver_commission_usd + mex_commission_usd + order_fee_usd + ad_spend_usd as gross_revenue
    ,cast((driver_commission_usd + mex_commission_usd + order_fee_usd + ad_spend_usd - total_incentives_spend_usd - total_grab_promo_spend_usd) as double)/ adj_gmv as ppgmv
    ,driver_commission_usd + mex_commission_usd + order_fee_usd + ad_spend_usd - total_incentives_spend_usd - total_grab_promo_spend_usd as pbo

    {{#if show_unit_economics_metrics == 'yes'}}
    ,cast(basket_size_usd as double) / completed_orders as avg_basket_size
    ,cast(mex_commission_usd as double) / total_partner_base_for_mex_commission as blended_commission_rate
    ,cast(mex_commission_usd as double) / completed_orders as avg_mex_commission
    ,cast(driver_commission_usd as double) / completed_orders as avg_dax_del_commission
    ,cast(order_fee_usd as double) / completed_orders as avg_order_fee_usd
    -- ,cast(platform_fee_usd as double) / completed_orders as avg_platform_fee_usd
    ,cast(ad_spend_usd as double) / completed_orders as avg_ad_spend_per_order
    ,cast(total_incentives_spend_usd as double) / completed_orders as incentive_cpr
    ,cast(total_grab_promo_spend_usd as double) / completed_orders as grab_promo_cpr
    ,cast(ssc_expense_usd as double)/ completed_orders as avg_ssc_expense_cpr
    ,cast((driver_commission_usd + mex_commission_usd + order_fee_usd + ad_spend_usd - total_incentives_spend_usd - total_grab_promo_spend_usd) as double) / completed_orders as profit_per_order
    {{#endif}}
from agg