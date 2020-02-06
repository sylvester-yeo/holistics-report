with mex_datamart as (
    select
        mex.*
    from datamart.dim_merchants mex
    where ([[merchant_id in ({{merchant_id}})]] or [[business_name in ({{merchant_id}})]])
        and [[city_id in ({{cities|noquote}})]]
        and [[country_id in ({{country_|noquote}})]]
)
,mex_con as (
  select *
  from
    (
      select
        contracts.merchant_id
        ,mex_datamart.merchant_name --new change, take from datamart straight
        ,mex_datamart.business_name --new change, take from datamart straight
        ,contracts.partner
        ,mex_datamart.is_bd_account --new change, take from datamart straight
        ,mex_datamart.is_bd_partner --new change, take from datamart straight
        ,date(contracts.valid_from) as date_mex_snapshots
        ,contracts.valid_from
        ,row_number() over (partition by contracts.merchant_id, date(contracts.valid_from) order by contracts.valid_from asc) as row_num
      from snapshots.food_data_service_merchant_contracts contracts
      inner join mex_datamart on mex_datamart.merchant_id = contracts.merchant_id --only relevant merchant
      where [[(concat(contracts.year,'-',contracts.month,'-',contracts.day)) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]] --holistics filter
        and [[(concat(contracts.year,'-',contracts.month,'-',contracts.day)) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]] --holistics filter
        and date(concat(contracts.year,'-',contracts.month,'-',contracts.day)) > date('2019-06-20')
    )
    where row_num = 1
    union all (
        select *
        from (
            select
                contracts.id as merchant_id
                ,mex_datamart.merchant_name --new change, take from datamart straight
                ,mex_datamart.business_name --new change, take from datamart straight
                ,case when json_extract_scalar(contracts.contract,'$.partner') = '1' then true else false end as partner
                ,mex_datamart.is_bd_account --new change, take from datamart straight
                ,mex_datamart.is_bd_partner --new change, take from datamart straight
                ,date(contracts.valid_from + interval '1' second) as date_mex_snapshots
                ,contracts.valid_from
                ,row_number() over (partition by contracts.id, date(contracts.valid_from + interval '1' second) order by contracts.valid_from + interval '1' second asc) as row_num
            from snapshots.grab_mall_grab_mall_seller contracts
            inner join mex_datamart on mex_datamart.merchant_id = contracts.id --only relevant merchant
            where date(concat(contracts.year,'-',contracts.month,'-',contracts.day)) < date('2019-06-20') --use only if before June 20th
	            and [[(concat(contracts.year,'-',contracts.month,'-',contracts.day)) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]] --holistics filter
                and [[(concat(contracts.year,'-',contracts.month,'-',contracts.day)) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]] --holistics filter
            )
        where row_num = 1
    )
)
,mex as (
  select *
  from (
    select
        snapshots.merchant_id
        ,mex_datamart.merchant_name --new change, take from datamart straight
        ,mex_datamart.business_name --new change, take from datamart straight
        ,snapshots.model_type
        ,date(snapshots.valid_from) as date_mex_snapshots
        ,snapshots.valid_from
        --,chain_name as brand_name
        ,row_number() over (partition by snapshots.merchant_id, date(snapshots.valid_from) order by snapshots.valid_from asc) as row_num
    from snapshots.food_data_service_merchants snapshots
    inner join mex_datamart on mex_datamart.merchant_id = snapshots.merchant_id
    where [[(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]] --holistics filter
        and [[(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]] --holistics filter
        and date(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) > date('2019-06-20')
    )
  where row_num = 1
  union all
  select * from (
  	select
  		snapshots.id as merchant_id
        ,mex_datamart.merchant_name --new change, take from datamart straight
        ,mex_datamart.business_name --new change, take from datamart straight
  		,snapshots.model_type
  		,date(snapshots.valid_from + interval '1' second) as date_mex_snapshots
  		,snapshots.valid_from
  		,row_number() over (partition by snapshots.id, date(snapshots.valid_from + interval '1' second) order by snapshots.valid_from + interval '1' second asc) as row_num
  	from snapshots.grab_mall_grab_mall_seller snapshots
    inner join mex_datamart on mex_datamart.merchant_id = snapshots.id
  	where [[(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]] --holistics filter
        and [[(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]] --holistics filter
  		and date(concat(snapshots.year,'-',snapshots.month,'-',snapshots.day)) < date('2019-06-20') -- use only if before June 20th
  )
  where row_num = 1
)
,mex_snapshots as (
  select
    mex.merchant_id
    ,mex.merchant_name as merchant_name
    ,mex.business_name as business_name
    ,mex_con.is_bd_account --straight from datamart
    ,mex_con.is_bd_partner --straight from datamart
    ,mex.date_mex_snapshots
    ,mex.model_type as original_model_type
    ,mex_con.partner as partner_status
  from mex
  left join mex_con
    on mex.merchant_id = mex_con.merchant_id
    and mex.date_mex_snapshots = mex_con.date_mex_snapshots
)
,orders as (
  SELECT
    a.date_local
    ,a.merchant_id
    ,b.merchant_name as merchant_name
    ,b.business_name as business_name
    ,a.country_name
    ,a.city_name
    ,CASE WHEN b.partner_status = TRUE THEN 'Partner' ELSE 'Non-Partner' END AS partner_status
    ,case when b.is_bd_account = TRUE then 'BD Account' else 'Non-BD' END as bd_account_status
    ,case when b.is_bd_partner = TRUE then 'BD Partner' else 'Non BD-Partner' end as bd_partner_status
    ,(CASE WHEN b.original_model_type = 1 THEN 'Integrated' ELSE 'Concierge' END) AS business_model

    /*general metrics*/
    ,sum(a.gmv_usd_gf) as gmv_usd
    ,sum(a.gmv_local) as gmv_local
    ,sum(a.basket_size) as basket_size_usd
    ,sum(a.basket_size_local) as basket_size_local
    ,sum(a.sub_total) as sub_total_usd
    ,sum(a.sub_total_local) as sub_total_local
    ,sum(a.all_incoming_orders_gf) as all_incoming_orders
    ,sum(a.completed_orders_gf) as completed_orders
    ,sum(a.allocated_orders) as allocated_orders
    ,sum(a.unallocated_orders) as unallocated_orders
    ,sum(a.mex_commission) as mex_commission_usd
    ,sum(a.mex_commission_local) as mex_commission_local
    ,sum(a.base_for_mex_commission) as base_for_mex_commission_usd
    ,sum(a.base_for_mex_commission_local) as base_for_mex_commission_local
    ,sum(a.delivery_fare_gf) as delivery_fare_usd
    ,sum(a.delivery_fare_gf_local) as delivery_fare_local
    ,sum(a.dax_delivery_fare) as dax_delivery_fare_usd
    ,sum(a.dax_delivery_fare_local) as dax_delivery_fare_local
    ,sum(a.driver_commission) as driver_commission_usd
    ,sum(a.driver_commission_local) as driver_commission_local
    ,sum(a.cancellations) as total_cancellations
    ,sum(a.cancellations_passenger) as total_pax_cancellations
    ,sum(a.cancellations_driver) as total_dax_cancellations
    ,sum(a.cancellations_operator) as total_operator_cancellations
    ,sum(a.cancellations_merchant) as total_mex_cancellations
    ,sum(a.incentives_local) as incentives_local
    ,sum(a.incentives_usd) as incentives_usd
    ,sum(a.spot_incentive_bonus_local) as spot_incentive_bonus_local
    ,sum(a.spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(a.promo_expense) as promo_expense_usd
    ,sum(a.promo_expense_local) as promo_expense_local
    ,sum(a.promo_incoming_orders) as promo_incoming_orders
    ,sum(a.promo_completed_orders) as promo_completed_orders
    ,sum(a.pre_accept_cancellations) as pre_accept_cancellations
    ,sum(a.pre_accept_cancellations_pax) as pre_accept_cancellations_pax
    ,sum(a.pre_accept_cancellations_operator) as pre_accept_cancellations_operator
    ,sum(a.pre_allocation_cancellations) as pre_allocation_cancellations
    ,sum(a.pre_allocation_cancellations_pax) as pre_allocation_cancellations_pax
    ,sum(a.pre_allocation_cancellations_operator) as pre_allocation_cancellations_operator

    /*case for total partner metrics*/
    ,sum(case when a.restaurant_partner_status = 'partner' then a.gmv_usd_gf END) as total_partner_gmv_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.gmv_local END) as total_partner_gmv_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.basket_size END) as total_partner_basket_size_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.basket_size_local END) as total_partner_basket_size_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sub_total END) as total_partner_sub_total_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sub_total_local END) as total_partner_sub_total_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.all_incoming_orders_gf END) as total_partner_all_incoming_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.completed_orders_gf END) as total_partner_completed_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.allocated_orders END) as total_partner_allocated_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.unallocated_orders END) as total_partner_unallocated_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.mex_commission END) as total_partner_mex_commission_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.mex_commission_local END) as total_partner_mex_commission_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission END) as total_partner_base_for_mex_commission_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission_local END) as total_partner_base_for_mex_commission_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.delivery_fare_gf END) as total_partner_delivery_fare_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.delivery_fare_gf_local END) as total_partner_delivery_fare_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.dax_delivery_fare END) as total_partner_dax_delivery_fare_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.dax_delivery_fare_local END) as total_partner_dax_delivery_fare_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.driver_commission END) as total_partner_driver_commission_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.driver_commission_local END) as total_partner_driver_commission_local_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations END) as total_partner_total_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_passenger END) as total_partner_total_pax_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_driver END) as total_partner_total_dax_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_operator END) as total_partner_total_operator_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_merchant END) as total_partner_total_mex_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.incentives_local END) as total_partner_incentives_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.incentives_usd END) as total_partner_incentives_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_local END) as total_partner_spot_incentive_bonus_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_usd END) as total_partner_spot_incentive_bonus_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense END) as total_partner_promo_expense_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense_local END) as total_partner_promo_expense_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_incoming_orders END) as total_partner_promo_incoming_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_completed_orders END) as total_partner_promo_completed_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations END) as total_partner_pre_accept_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_pax END) as total_partner_pre_accept_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_operator END) as total_partner_pre_accept_cancellations_operator
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations END) as total_partner_pre_allocation_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_pax END) as total_partner_pre_allocation_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_operator END) as total_partner_pre_allocation_cancellations_operator

    /*specific metrics*/
    ,sum(case when a.business_model = 'Integrated' then gmv_usd_gf else 0 end) as im_gmv_usd
    ,sum(case when a.business_model = 'Integrated' then gmv_local else 0 end) as im_gmv_local

  FROM
    slide.gf_mex_level_daily_metrics a
  INNER JOIN mex_snapshots b --only within the date range
    on a.merchant_id = b.merchant_id
    AND date(a.date_local) = b.date_mex_snapshots
  left join public.cities on a.city_name = cities.name --get city, country id
  WHERE [[date(a.partition_date_local) >= date({{start_date}})]]
    and [[date(a.partition_date_local) <= date({{end_date}})]]
    and [[cities.id in ({{cities|noquote}})]]
    and [[cities.country_id in ({{country_|noquote}})]]
  GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,biz_outlet as (
    SELECT
        business_name,
        country_name,
        city_name,
        date_local,
        count(merchant_id) AS active_outlet
    FROM orders
    GROUP BY 1,2,3,4
)
/*,mf_promo_code_per_outlet as (
    select * from (
        select
            biz_outlet.business_name,
            biz_outlet.country_name,
            biz_outlet.city_name,
            biz_outlet.date_local,
            sum(mf_promo_code.mex_funding_amount_perday_local/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_local,
            sum(mf_promo_code.mex_funding_amount_perday_usd/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_usd
        FROM
            slide.mex_funded_promo_code_by_brand_cg mf_promo_code
        inner join biz_outlet
        ON lower(trim(biz_outlet.business_name)) = lower(trim(mf_promo_code.business_name))
            AND biz_outlet.city_name = mf_promo_code.city
            AND biz_outlet.country_name = mf_promo_code.country
            AND biz_outlet.date_local = mf_promo_code.date_local
        where date(mf_promo_code.date_local) < date('2019-07-01') and biz_outlet.date_local < date('2019-07-01') --hard coded to before q3
            and [[mf_promo_code.date_local >= date({{start_date}})]]
            and [[mf_promo_code.date_local <= date({{end_date}})]]
        group by 1,2,3,4
    )
)*/
,batching as (
    select
        date_local, merchant_id, city as city_name
        ,sum(case when tried_batching = 'true' then 1 else 0 end) as tried_batching
        ,sum(is_batched_order) as batched_orders
    from slide.food_order_batching_dashboard_order_lvl
    where [[(partition_date) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]]
        and [[(partition_date) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]]
        and [[date(date_local) >= date({{start_date}})]]
        and [[date(date_local) <= date({{end_date}})]]
        and [[merchant_id in ({{merchant_id}})]]
        and order_state = 'COMPLETED'
    group by 1,2,3
)
,mbp as (
    select
      mbp.restaurant_id as merchant_id
      ,mbp.date_local
      ,cities.name as city_name
      ,sum(mbp_paid_by_mex) as mbp_paid_by_mex
      ,sum(mbp_paid_by_pax) as mbp_paid_by_pax
      ,sum(tsp_paid_by_us) as tsp_paid_by_us
    from slide.gf_tsp_mbp_breakdown mbp
    left join public.cities on mbp.city_id = cities.id
    where [[date(date_local) >= date({{start_date}})]]
        and [[date(date_local) <= date({{end_date}})]]
        and [[city_id in ({{cities|noquote}})]]
        and [[cities.country_id in ({{country_|noquote}})]]
        and [[mbp.restaurant_id in ({{merchant_id}})]]
        and state = 'COMPLETED' --add to P&L scripts
    group by 1,2,3
)
,mfc as (
    select
      mfc.city
      ,mfc.country
      ,mfc.merchant_id
      ,date_local
      ,sum(mfc.completed_orders_promo_item) as completed_orders_promo_item
      --,sum(mfc.gf_promo_spend_usd) as gf_promo_spend_usd
      --,sum(mfc.gf_promo_spend_local) as gf_promo_spend_local
      --,sum(mfc.mex_promo_spend_usd) as mex_promo_spend_usd
      --,sum(mfc.mex_promo_spend_local) as mex_promo_spend_local
      ,sum(mfc.mex_promo_spend_n_usd) as mex_promo_spend_n_usd
      ,sum(mfc.mex_promo_spend_n_local) as mex_promo_spend_n_local
      --,sum(mfc.grab_promo_spend_usd) as grab_promo_spend_usd
      --,sum(mfc.grab_promo_spend_local) as grab_promo_spend_local
      ,sum(mfc.grab_promo_spend_n_usd) as grab_promo_spend_n_usd
      ,sum(mfc.grab_promo_spend_n_local) as grab_promo_spend_n_local
      --,sum(mfc.promo_item_normal_price_usd - mfc.promo_item_promo_price_usd) as general_promo_item_price_diff_usd
      --,sum(mfc.promo_item_normal_price_local - mfc.promo_item_promo_price_local) as general_promo_item_price_diff_local
      ,sum(mfc.promo_item_n_normal_price_usd - mfc.promo_item_n_promo_price_usd) as general_promo_item_price_diff_n_usd
      ,sum(mfc.promo_item_n_normal_price_local - mfc.promo_item_n_promo_price_local) as general_promo_item_price_diff_n_local

      --,sum(mfc.gf_promo_spend_usd_non_mfc) as gf_promo_spend_usd_non_mfc
      --,sum(mfc.gf_promo_spend_local_non_mfc) as gf_promo_spend_local_non_mfc
      --,sum(mfc.mex_promo_spend_usd_non_mfc) as mex_promo_spend_usd_non_mfc
      --,sum(mfc.mex_promo_spend_local_non_mfc) as mex_promo_spend_local_non_mfc
      ,sum(mfc.mex_promo_spend_n_usd_non_mfc) as mex_promo_spend_n_usd_non_mfc
      ,sum(mfc.mex_promo_spend_n_local_non_mfc) as mex_promo_spend_n_local_non_mfc
      --,sum(mfc.grab_promo_spend_usd_non_mfc) as grab_promo_spend_usd_non_mfc
      --,sum(mfc.grab_promo_spend_local_non_mfc) as grab_promo_spend_local_non_mfc
      ,sum(mfc.grab_promo_spend_n_usd_non_mfc) as grab_promo_spend_n_usd_non_mfc
      ,sum(mfc.grab_promo_spend_n_local_non_mfc) as grab_promo_spend_n_local_non_mfc
      --,sum(mfc.promo_item_normal_price_usd_non_mfc - mfc.promo_item_promo_price_usd_non_mfc) as general_promo_item_price_diff_usd_non_mfc
      --,sum(mfc.promo_item_normal_price_local_non_mfc - mfc.promo_item_promo_price_local_non_mfc) as general_promo_item_price_diff_local_non_mfc
      ,sum(mfc.promo_item_n_normal_price_usd_non_mfc - mfc.promo_item_n_promo_price_usd_non_mfc) as general_promo_item_price_diff_n_usd_non_mfc
      ,sum(mfc.promo_item_n_normal_price_local_non_mfc - mfc.promo_item_n_promo_price_local_non_mfc) as general_promo_item_price_diff_n_local_non_mfc

      --,sum(mfc.partner_gf_promo_spend_usd) as partner_gf_promo_spend_usd
      --,sum(mfc.partner_gf_promo_spend_local) as partner_gf_promo_spend_local
      --,sum(mfc.partner_mex_promo_spend_usd) as partner_mex_promo_spend_usd
      --,sum(mfc.partner_mex_promo_spend_local) as partner_mex_promo_spend_local
      ,sum(mfc.partner_mex_promo_spend_n_usd) as partner_mex_promo_spend_n_usd
      ,sum(mfc.partner_mex_promo_spend_n_local) as partner_mex_promo_spend_n_local
      --,sum(mfc.partner_grab_promo_spend_usd) as partner_grab_promo_spend_usd
      --,sum(mfc.partner_grab_promo_spend_local) as partner_grab_promo_spend_local
      ,sum(mfc.partner_grab_promo_spend_n_usd) as partner_grab_promo_spend_n_usd
      ,sum(mfc.partner_grab_promo_spend_n_local) as partner_grab_promo_spend_n_local
      --,sum(mfc.partner_promo_item_normal_price_usd - mfc.partner_promo_item_promo_price_usd) as partner_promo_item_price_diff_usd
      --,sum(mfc.partner_promo_item_normal_price_local - mfc.partner_promo_item_promo_price_local) as partner_promo_item_price_diff_local
      ,sum(mfc.partner_promo_item_n_normal_price_usd - mfc.partner_promo_item_n_promo_price_usd) as partner_promo_item_price_diff_n_usd
      ,sum(mfc.partner_promo_item_n_normal_price_local - mfc.partner_promo_item_n_promo_price_local) as partner_promo_item_price_diff_n_local

      --,sum(mfc.partner_gf_promo_spend_usd_non_mfc) as partner_gf_promo_spend_usd_non_mfc
      --,sum(mfc.partner_gf_promo_spend_local_non_mfc) as partner_gf_promo_spend_local_non_mfc
      --,sum(mfc.partner_mex_promo_spend_usd_non_mfc) as partner_mex_promo_spend_usd_non_mfc
      --,sum(mfc.partner_mex_promo_spend_local_non_mfc) as partner_mex_promo_spend_local_non_mfc
      ,sum(mfc.partner_mex_promo_spend_n_usd_non_mfc) as partner_mex_promo_spend_n_usd_non_mfc
      ,sum(mfc.partner_mex_promo_spend_n_local_non_mfc) as partner_mex_promo_spend_n_local_non_mfc
      --,sum(mfc.partner_grab_promo_spend_usd_non_mfc) as partner_grab_promo_spend_usd_non_mfc
      --,sum(mfc.partner_grab_promo_spend_local_non_mfc) as partner_grab_promo_spend_local_non_mfc
      ,sum(mfc.partner_grab_promo_spend_n_usd_non_mfc) as partner_grab_promo_spend_n_usd_non_mfc
      ,sum(mfc.partner_grab_promo_spend_n_local_non_mfc) as partner_grab_promo_spend_n_local_non_mfc
      --,sum(mfc.partner_promo_item_normal_price_usd_non_mfc - mfc.partner_promo_item_promo_price_usd_non_mfc) as partner_promo_item_price_diff_usd_non_mfc
      --,sum(mfc.partner_promo_item_normal_price_local_non_mfc - mfc.partner_promo_item_promo_price_local_non_mfc) as partner_promo_item_price_diff_local_non_mfc
      ,sum(mfc.partner_promo_item_n_normal_price_usd_non_mfc - mfc.partner_promo_item_n_promo_price_usd_non_mfc) as partner_promo_item_price_diff_n_usd_non_mfc
      ,sum(mfc.partner_promo_item_n_normal_price_local_non_mfc - mfc.partner_promo_item_n_promo_price_local_non_mfc) as partner_promo_item_price_diff_n_local_non_mfc
    from slide.gf_mfc_brand mfc
    left join datamart.dim_merchants mex on mfc.merchant_id = mex.merchant_id
    left join public.cities on mfc.city = cities.name
    where [[date(date_local) >= date({{start_date}})]]
        and [[date(date_local) <= date({{end_date}})]]
        and ([[mex.merchant_id in ({{merchant_id}})]] or [[mex.business_name in ({{merchant_id}})]])
        and [[cities.id in ({{cities|noquote}})]]
        and [[cities.country_id in ({{country_|noquote}})]]
    group by 1,2,3,4
)
,mfp as (
    select
        date_local
        ,mfp.merchant_id
        ,mfp.city
        ,sum(mex_mfp_spend_usd) as mex_mfp_spend_usd
        ,sum(mex_mfp_spend_local) as mex_mfp_spend_local
    from slide.gf_mfp_merchant mfp
    left join datamart.dim_merchants mex on mfp.merchant_id = mex.merchant_id
    where [[date(date_local) >= (date({{start_date}}) - interval '1' day)]]
        and [[date(date_local) <= (date({{end_date}}) + interval '1' day)]]
        and ([[mfp.merchant_id in ({{merchant_id}})]] or [[mex.business_name in ({{merchant_id}})]])
    group by 1,2,3
)
,am_list as (
  SELECT am_merchant_id, MAX(am_name) as am_name FROM (
    (SELECT merchant_id as am_merchant_id, MAX(am) as am_name FROM holistics.gf_am_mapping_nondelta group by 1)
    UNION all
    (SELECT merchant_id as am_merchant_id, MAX(am) as am_name FROM holistics.gf_am_mapping_delta group by 1))
  GROUP BY 1
)
,final_table as (
  SELECT
    orders.date_local
    ,orders.merchant_id
    ,orders.merchant_name
    ,orders.business_name
    ,orders.country_name
    ,orders.city_name
    ,orders.partner_status
    ,orders.bd_account_status
    ,orders.bd_partner_status
    ,orders.business_model
    ,case when am_name is not null then 'AM' else 'Non-AM' end as am_tagging

    ,orders.all_incoming_orders
    ,orders.completed_orders
    ,orders.total_cancellations

    --USD metrics
    ,orders.gmv_usd
    ,orders.basket_size_usd
    ,orders.sub_total_usd
    ,orders.mex_commission_usd
    ,orders.delivery_fare_usd
    ,orders.dax_delivery_fare_usd
    ,orders.driver_commission_usd
    ,orders.incentives_usd
    ,orders.spot_incentive_bonus_usd
    ,orders.promo_expense_usd

    --Partner specific metric, usd
    ,orders.total_partner_gmv_usd
    ,orders.total_partner_basket_size_usd
    ,orders.total_partner_sub_total_usd
    ,orders.total_partner_mex_commission_usd
    ,orders.total_partner_base_for_mex_commission_usd
    ,orders.total_partner_delivery_fare_usd
    ,orders.total_partner_dax_delivery_fare_usd
    ,orders.total_partner_driver_commission_usd
    ,orders.total_partner_incentives_usd
    ,orders.total_partner_spot_incentive_bonus_usd
    ,orders.total_partner_promo_expense_usd

    --Local currency metrics
    ,orders.gmv_local
    ,orders.basket_size_local
    ,orders.sub_total_local
    ,orders.mex_commission_local
    ,orders.delivery_fare_local
    ,orders.dax_delivery_fare_local
    ,orders.driver_commission_local
    ,orders.incentives_local
    ,orders.spot_incentive_bonus_local
    ,orders.promo_expense_local

    ,orders.total_partner_gmv_local
    ,orders.total_partner_basket_size_local
    ,orders.total_partner_sub_total_local
    ,orders.total_partner_mex_commission_local
    ,orders.total_partner_delivery_fare_local
    ,orders.total_partner_dax_delivery_fare_local
    ,orders.total_partner_driver_commission_local_usd
    ,orders.total_partner_incentives_local
    ,orders.total_partner_spot_incentive_bonus_local
    ,orders.total_partner_promo_expense_local

    --Partner specific metric, local
    ,orders.total_partner_all_incoming_orders
    ,orders.total_partner_completed_orders
    ,orders.total_partner_allocated_orders
    ,orders.total_partner_unallocated_orders

    /* Metrics not required for holistics report
    ,orders.pre_accept_cancellations
    ,orders.pre_accept_cancellations_pax
    ,orders.pre_accept_cancellations_operator
    ,orders.pre_allocation_cancellations
    ,orders.pre_allocation_cancellations_pax
    ,orders.pre_allocation_cancellations_operator
    */

    --for MFC
    ,coalesce(mfc.mex_promo_spend_n_usd,0) as mex_promo_spend_n_usd
    ,coalesce(mfc.grab_promo_spend_n_usd,0) as grab_promo_spend_n_usd
    ,coalesce(mfc.mex_promo_spend_n_usd_non_mfc,0) as mex_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.grab_promo_spend_n_usd_non_mfc,0) as grab_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.partner_mex_promo_spend_n_usd,0) as partner_mex_promo_spend_n_usd
    ,coalesce(mfc.partner_grab_promo_spend_n_usd,0) as partner_grab_promo_spend_n_usd
    ,coalesce(mfc.partner_mex_promo_spend_n_usd_non_mfc,0) as partner_mex_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.partner_grab_promo_spend_n_usd_non_mfc,0) as partner_grab_promo_spend_n_usd_non_mfc

    ,coalesce(mfc.mex_promo_spend_n_local,0) as mex_promo_spend_n_local
    ,coalesce(mfc.grab_promo_spend_n_local,0) as grab_promo_spend_n_local
    ,coalesce(mfc.mex_promo_spend_n_local_non_mfc,0) as mex_promo_spend_n_local_non_mfc
    ,coalesce(mfc.grab_promo_spend_n_local_non_mfc,0) as grab_promo_spend_n_local_non_mfc
    ,coalesce(mfc.partner_mex_promo_spend_n_local,0) as partner_mex_promo_spend_n_local
    ,coalesce(mfc.partner_grab_promo_spend_n_local,0) as partner_grab_promo_spend_n_local
    ,coalesce(mfc.partner_mex_promo_spend_n_local_non_mfc,0) as partner_mex_promo_spend_n_local_non_mfc
    ,coalesce(mfc.partner_grab_promo_spend_n_local_non_mfc,0) as partner_grab_promo_spend_n_local_non_mfc

    -- MFP
    ,--case
      --  when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_usd,0)
      --  else
        coalesce(mfp.mex_mfp_spend_usd, 0)
    --end AS
    mf_promo_code_perday_outlet_usd

    ,--case
    	--when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_local,0) else
    	coalesce(mfp.mex_mfp_spend_local, 0)
	--end AS mf_promo_code_perday_outlet_local

    /* batching related metrics*/
    ,COALESCE(batching.tried_batching, 0) as tried_batching
    ,COALESCE(batching.batched_orders, 0) as batched_orders

  /* mbp related metrics*/
    ,COALESCE(mbp.mbp_paid_by_mex/rer.exchange_one_usd,0) as mbp_paid_by_mex
    ,COALESCE(mbp.mbp_paid_by_pax/rer.exchange_one_usd,0) as mbp_paid_by_pax
    ,COALESCE(mbp.tsp_paid_by_us/rer.exchange_one_usd,0) as tsp_paid_by_us

    /*comms related*/
    --,COALESCE(case when orders.business_model = 'Integrated' then 100 else comms.blended_collection_rate end,0) as blended_collection_rate

    /*fx for MBP*/
    ,rer.exchange_one_usd as fx_one_usd

  FROM orders

  LEFT JOIN mfc
    on orders.merchant_id = mfc.merchant_id
    and orders.city_name = mfc.city
    and date(orders.date_local) = date(mfc.date_local)

  /*LEFT JOIN mf_promo_code_per_outlet
    ON lower(trim(orders.business_name)) = lower(trim(mf_promo_code_per_outlet.business_name))
    AND orders.country_name = mf_promo_code_per_outlet.country_name
    AND orders.city_name = mf_promo_code_per_outlet.city_name
    AND orders.date_local = date(mf_promo_code_per_outlet.date_local)*/

  LEFT JOIN mfp
    on mfp.merchant_id = orders.merchant_id
    and orders.city_name = mfp.city
    and date(orders.date_local) = date(mfp.date_local)

  LEFT JOIN batching
    on orders.merchant_id = batching.merchant_id
    and orders.date_local = batching.date_local
    and orders.city_name = batching.city_name

  LEFT JOIN mbp
    on orders.merchant_id = mbp.merchant_id
    and orders.date_local = mbp.date_local
    and orders.city_name = mbp.city_name

  left join am_list
    on orders.merchant_id = am_list.am_merchant_id

--   LEFT JOIN comms
--     on orders.merchant_id = comms.merchant_id
--     and date_trunc('month',date(orders.date_local)) = comms.start_date_of_month
--     and orders.city_name = comms.city_name

  LEFT JOIN public.countries on orders.country_name = countries.name

  LEFT JOIN datamart.ref_exchange_rates rer on countries.id = rer.country_id and (orders.date_local between rer.start_date and rer.end_date)
)
select
    {{#if dimension_split == 'merchant'}}
        merchant_name as merchant_business_name
        ,merchant_id as merchant_id_business_name
        ,business_name as business_name
        ,'By Merchant' as business_merchant_aggregation
    {{#else}}
        business_name as merchant_business_name
        ,business_name as merchant_id_business_name
        ,business_name as business_name
        ,'By Brand' as business_merchant_aggregation
    {{#endif}}

    ,country_name
    ,city_name
    ,partner_status
    ,bd_account_status
    ,bd_partner_status
    ,business_model
    ,am_tagging

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

    ,sum(total_cancellations) as total_cancellations

    --main metrics
    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_gmv_usd
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_basket_size_usd
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(total_partner_base_for_mex_commission_usd) as total_partner_base_for_mex_commission_usd
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(mex_commission_usd + driver_commission_usd) as total_gross_revenue_usd
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd) as total_incentives_spend_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd - mbp_paid_by_mex) as total_incentives_spend_usd_excl_mbp
    ,sum(mex_promo_spend_n_usd) as mfc_mex_promo_spend_usd
    ,sum(mf_promo_code_perday_outlet_usd) as mfp_mex_promo_spend_usd
    ,sum(mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd) as total_mfd_usd
    ,sum(promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as promo_expense_usd
    ,cast(sum(mex_commission_usd + driver_commission_usd - (incentives_usd + spot_incentive_bonus_usd + dax_delivery_fare_usd - delivery_fare_usd) - (promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) + (mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd)) as double) / sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as ppgmv

    ,avg(fx_one_usd) as avg_fx_one_usd

from final_table
{{#if level_of_aggregation == 'Total'}}
    group by 1,2,3,4,5,6,7,8,9,10,11,12
{{#else}}
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
{{#endif}}




/* Previous code
select
    {{#if dimension_split == 'merchant'}}
         merchant_name as merchant_business_name
        ,merchant_id as merchant_id_business_name
        ,business_name as business_name
        ,'By Merchant' as business_merchant_aggregation
    {{#else}}
        business_name as merchant_business_name
        ,business_name as merchant_id_business_name
        ,business_name as business_name
        ,'By Brand' as business_merchant_aggregation
    {{#endif}}

    ,'Total' as time_aggregation
    ,country_name
    ,city_name
    ,partner_status
    ,bd_account_status
    ,bd_partner_status
    ,business_model
    ,am_tagging
    ,[[date({{start_date}})]] as start_date
    ,[[date({{end_date}})]] as end_date

    ,sum(total_cancellations) as total_cancellations

    --main metrics
    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_gmv_usd
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_basket_size_usd
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(total_partner_base_for_mex_commission_usd) as total_partner_base_for_mex_commission_usd
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(mex_commission_usd + driver_commission_usd) as total_gross_revenue_usd
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd) as total_incentives_spend_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd - mbp_paid_by_mex) as total_incentives_spend_usd_excl_mbp
    ,sum(mex_promo_spend_n_usd) as mfc_mex_promo_spend_usd
    ,sum(mf_promo_code_perday_outlet_usd) as mfp_mex_promo_spend_usd
    ,sum(mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd) as total_mfd_usd
    ,sum(promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as promo_expense_usd
    ,cast(sum(mex_commission_usd + driver_commission_usd - (incentives_usd + spot_incentive_bonus_usd + dax_delivery_fare_usd - delivery_fare_usd) - (promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) + (mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd)) as double) / sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as ppgmv

    ,avg(fx_one_usd) as avg_fx_one_usd

from final_table
group by 1,2,3,4,5,6,7,8,9,10,11,12
{{#if breakdown_by_month == 'yes'}}
union all (
    select
    {{#if dimension_split == 'merchant'}}
         merchant_name as merchant_business_name
        ,merchant_id as merchant_id_business_name
        ,business_name as business_name
        ,'By Merchant' as business_merchant_aggregation
    {{#else}}
        business_name as merchant_business_name
        ,business_name as merchant_id_business_name
        ,business_name as business_name
        ,'By Brand' as business_merchant_aggregation
    {{#endif}}

    ,'By Month' as time_aggregation
    ,country_name
    ,city_name
    ,partner_status
    ,bd_account_status
    ,bd_partner_status
    ,business_model
    ,am_tagging
    ,case when date_trunc('month',date(date_local)) <= date({{start_date}})
            then date({{start_date}})
            else date_trunc('month',date(date_local))
            end as start_date
    ,case when (date_trunc('month', date(date_local)) + interval '1' month - interval '1' day) <= date({{end_date}})
        then (date_trunc('month', date(date_local)) + interval '1' month - interval '1' day)
        else date({{end_date}})
        end as end_date

    ,sum(total_cancellations) as total_cancellations

    --main metrics
    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_gmv_usd
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_basket_size_usd
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(total_partner_base_for_mex_commission_usd) as total_partner_base_for_mex_commission_usd
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(mex_commission_usd + driver_commission_usd) as total_gross_revenue_usd
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd) as total_incentives_spend_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd - mbp_paid_by_mex) as total_incentives_spend_usd_excl_mbp
    ,sum(mex_promo_spend_n_usd) as mfc_mex_promo_spend_usd
    ,sum(mf_promo_code_perday_outlet_usd) as mfp_mex_promo_spend_usd
    ,sum(mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd) as total_mfd_usd
    ,sum(promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as promo_expense_usd
    ,cast(sum(mex_commission_usd + driver_commission_usd - (incentives_usd + spot_incentive_bonus_usd + dax_delivery_fare_usd - delivery_fare_usd) - (promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) + (mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd)) as double) / sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as ppgmv

    ,avg(fx_one_usd) as avg_fx_one_usd

    from final_table
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
{{#endif}}
{{#if breakdown_by_week == 'yes'}}
union all (
    select
    {{#if dimension_split == 'merchant'}}
         merchant_name as merchant_business_name
        ,merchant_id as merchant_id_business_name
        ,business_name as business_name
        ,'By Merchant' as business_merchant_aggregation
    {{#else}}
        business_name as merchant_business_name
        ,business_name as merchant_id_business_name
        ,business_name as business_name
        ,'By Brand' as business_merchant_aggregation
    {{#endif}}

    ,'By Week' as time_aggregation
    ,country_name
    ,city_name
    ,partner_status --might have different status within the same brand, how to resolve this
    ,bd_account_status
    ,bd_partner_status
    ,business_model
    ,am_tagging
    ,case when date_trunc('week',date(date_local)) <= date({{start_date}})
            then date({{start_date}})
            else date_trunc('week',date(date_local))
            end as start_date
    ,case when (date_trunc('week', date(date_local)) + interval '6' day) <= date({{end_date}})
        then (date_trunc('week', date(date_local)) + interval '6' day)
        else date({{end_date}})
        end as end_date

    ,sum(total_cancellations) as total_cancellations

    --main metrics
    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_gmv_usd
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_basket_size_usd
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(total_partner_base_for_mex_commission_usd) as total_partner_base_for_mex_commission_usd
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(mex_commission_usd + driver_commission_usd) as total_gross_revenue_usd
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd) as total_incentives_spend_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd - mbp_paid_by_mex) as total_incentives_spend_usd_excl_mbp
    ,sum(mex_promo_spend_n_usd) as mfc_mex_promo_spend_usd
    ,sum(mf_promo_code_perday_outlet_usd) as mfp_mex_promo_spend_usd
    ,sum(mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd) as total_mfd_usd
    ,sum(promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as promo_expense_usd
    ,cast(sum(mex_commission_usd + driver_commission_usd - (incentives_usd + spot_incentive_bonus_usd + dax_delivery_fare_usd - delivery_fare_usd) - (promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) + (mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd)) as double) / sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as ppgmv

    ,avg(fx_one_usd) as avg_fx_one_usd

    from final_table
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
{{#endif}}
{{#if breakdown_by_day == 'yes'}}
union all (
    select
    {{#if dimension_split == 'merchant'}}
         merchant_name as merchant_business_name
        ,merchant_id as merchant_id_business_name
        ,business_name as business_name
        ,'By Merchant' as business_merchant_aggregation
    {{#else}}
        business_name as merchant_business_name
        ,business_name as merchant_id_business_name
        ,business_name as business_name
        ,'By Brand' as business_merchant_aggregation
    {{#endif}}

    ,'By Day' as time_aggregation
    ,country_name
    ,city_name
    ,partner_status --might have different status within the same brand, how to resolve this
    ,bd_account_status
    ,bd_partner_status
    ,business_model
    ,am_tagging
    ,date(date_local) as start_date
    ,date(date_local) as end_date

    ,sum(total_cancellations) as total_cancellations

    --main metrics
    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_gmv_usd
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as adj_basket_size_usd
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(total_partner_base_for_mex_commission_usd) as total_partner_base_for_mex_commission_usd
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(mex_commission_usd + driver_commission_usd) as total_gross_revenue_usd
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd) as total_incentives_spend_usd
    ,sum(dax_delivery_fare_usd-delivery_fare_usd+incentives_usd+spot_incentive_bonus_usd - mbp_paid_by_mex) as total_incentives_spend_usd_excl_mbp
    ,sum(mex_promo_spend_n_usd) as mfc_mex_promo_spend_usd
    ,sum(mf_promo_code_perday_outlet_usd) as mfp_mex_promo_spend_usd
    ,sum(mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd) as total_mfd_usd
    ,sum(promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as promo_expense_usd
    ,cast(sum(mex_commission_usd + driver_commission_usd - (incentives_usd + spot_incentive_bonus_usd + dax_delivery_fare_usd - delivery_fare_usd) - (promo_expense_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) + (mex_promo_spend_n_usd + mf_promo_code_perday_outlet_usd)) as double) / sum(gmv_usd + mex_promo_spend_n_usd_non_mfc + grab_promo_spend_n_usd_non_mfc) as ppgmv

    ,avg(fx_one_usd) as avg_fx_one_usd

    from final_table
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
{{#endif}}
*/