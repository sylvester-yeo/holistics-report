with food_cashier_base as (
  SELECT
    user_id
    ,booking_code
    ,order_id
	,created_at
	,case when tx_type = 2 then mex_id else '' end as merchant_id
    ,CASE WHEN order_mode <= 2 THEN 'Cash' ELSE 'Cashless' END as booking_type
    ,(CASE WHEN tx_type = 1 THEN CAST(json_extract_scalar(metadata, '$.daxDeliveryFee') AS DOUBLE) ELSE CAST(0 as double) END)/100 as delivery_fee_plus_adjustments
    ,CAST(CASE WHEN tx_type = 1 THEN tx_amount ELSE 0 END as double)/100 as order_value
    ,CAST(CASE WHEN tx_type = 14 THEN tx_amount ELSE 0 END as double)/100 as earning_adjustments
    ,COALESCE(CASE WHEN tx_type = 1 THEN CAST(json_extract_scalar(metadata,'$.promotion') as double)/100 END,
                CASE WHEN tx_type >= 1000 and tx_type < 1100 THEN CAST(json_extract_scalar(metadata,'$.promotion') as double)/100 END,
                CAST(0 as double)) as promo
  FROM grab_food.payments
  WHERE
    user_type = 2 --type of user is DAX
    AND [[date(payments.created_at) >= date({{order_create_start_date}}) - interval '1' day]]
    AND [[date(payments.created_at) <= date({{order_create_end_date}}) + interval '1' day]]
    and [[date(concat(payments.year,'-',payments.month,'-',payments.day)) >= date({{order_create_start_date}}) - interval '1' day]]
    and [[date(concat(payments.year,'-',payments.month,'-',payments.day)) <= date({{order_create_end_date}}) + interval '1' day]]
    AND [[payments.mex_id in ({{merchant}})]]
    and [[payments.currency in ({{ Country }})]]
)
,food_cashier as (
	select
		user_id
		,booking_code
		,order_id
		,booking_type
		,min(created_at) as created_at
		,max(delivery_fee_plus_adjustments) as delivery_fee_plus_adjustments
		,max(order_value) as order_value
		,max(earning_adjustments) as earning_adjustments
		,max(promo) as promo
	from food_cashier_base
	group by 1,2,3,4
)
SELECT
  --dim_taxi_types.taxi_type_simple AS taxi_type
  grabfood.taxi_type_name
  ,fleets.name AS fleet
  ,drivers.id AS driver_id
  ,drivers.name AS driver_name
  ,mex.merchant_name AS merchant_name
  ,date(food_cashier.created_at) AS date_local
	,food_cashier.booking_code AS booking_id
  ,food_cashier.booking_type
  ,ROUND(SUM(food_cashier.earning_adjustments),2) as earning_adjustments
  ,ROUND(SUM(food_cashier.delivery_fee_plus_adjustments) - SUM(food_cashier.earning_adjustments),2) as delivery_fee
  ,ROUND(SUM(food_cashier.order_value) + SUM(food_cashier.promo),2) as order_value --before promo discount
  ,ROUND(SUM(food_cashier.promo),2) as promo
  ,ROUND(SUM(food_cashier.delivery_fee_plus_adjustments) - SUM(food_cashier.earning_adjustments) + SUM(food_cashier.order_value),2) as cash_collection
  ,ROUND(dac.amount,2) AS credit_deduction
FROM
  food_cashier
--LEFT JOIN grab_food.order_details od ON food_cashier.order_id = od.order_id -- check if it is only completed
--left join public.prejoin_food_order pfo on food_cashier.order_id = pfo.order_id
left join public.prejoin_grabfood grabfood on grabfood.booking_code = food_cashier.booking_code
LEFT JOIN public.drivers ON food_cashier.user_id = drivers.id
--LEFT JOIN datamart.dim_taxi_types
--	on cast(pfo.taxi_type_id as integer) = dim_taxi_types.id
--	and pfo.city_id = dim_taxi_types.city_id
	--ON drivers.taxi_type_id  = dim_taxi_types.id
	--AND drivers.country_id = dim_taxi_types.country_id
	--AND drivers.city_id = dim_taxi_types.city_id
LEFT JOIN public.fleets ON grabfood.driver_fleet_id = fleets.id
LEFT JOIN datamart.dim_merchants mex on grabfood.restaurant_id = mex.merchant_id
LEFT JOIN public.driver_account_transactions dac ON food_cashier.booking_code = dac.method
WHERE
  [[date(grabfood.partition_date) >= date({{order_create_start_date}}) - interval '1' day]]
  and [[date(grabfood.partition_date) <= date({{order_create_end_date}}) + interval '1' day]]
  and [[date(concat(dac.year,'-',dac.month,'-',dac.day)) >= date({{order_create_start_date}}) - interval '1' day]]
  and [[date(concat(dac.year,'-',dac.month,'-',dac.day)) <= date({{order_create_end_date}}) + interval '1' day]]
GROUP BY 1,2,3,4,5,6,7,8,14