-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

SELECT format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
      sum(totals.visits) as visits, 
      sum(totals.pageviews) as pageviews, 
      sum(totals.transactions) as transactions,
      sum(totals.totaltransactionRevenue)/1000000 as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _TABLE_SUFFIX between "0101" and "0331"
group by month
order by month;

-- Query 02: Bounce rate per traffic source in July 2017

select 
      trafficSource.source,
      sum(totals.bounces) as total_no_of_bounces,
      count(fullVisitorId) as total_visits,
      (sum(totals.bounces)*100/count(fullVisitorId)) as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by Source
order by total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017

SELECT 'month' as type_time,
       format_date("%Y%m", parse_DATE("%Y%m%d", date)) as time, 
       trafficSource.source as source,
       sum(totals.transactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
group by type_time, time, source

union all 
SELECT 'week' as type_time,
       format_date("%Y%w", parse_DATE("%Y%m%d", date)) as time,   
       trafficSource.source as source,
       sum(totals.transactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
where totals.transactionRevenue is not null
group by type_time,time, source
order by revenue DESC 

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 

with pageview_purchasers as (
  SELECT format_date("%Y%m" , parse_DATE("%Y%m%d", date)) as month,
      sum(totals.pageviews)/ count( distinct fullVisitorId) as avg_pageview_purchasers
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where totals.transactions is not null 
and _TABLE_SUFFIX between "0601" and "0731"
group by month
),

pageview_non_purchasers as (
SELECT format_date("%Y%m" , parse_DATE("%Y%m%d", date)) as month,
      sum(totals.pageviews)/ count( distinct fullVisitorId) as avg_pageview_non_purchasers
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where totals.transactions is null 
and _TABLE_SUFFIX between "0601" and "0731"
group by month
)

SELECT month,
       avg_pageview_purchasers,
       avg_pageview_non_purchasers
FROM pageview_purchasers
LEFT JOIN pageview_non_purchasers
using(month)
order by month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT format_date("%Y%m" , parse_DATE("%Y%m%d", date)) as month,
      sum(totals.transactions)/ count( distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
where totals.transactions is not null 
group by month

-- Query 06: Average amount of money spent per session

SELECT format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
       sum(totals.totalTransactionRevenue)/count(totals.transactions) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
where totals.transactions IS NOT NULL
group by month


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 
Output should show product name and the quantity was ordered.

with product as(
SELECT fullVisitorId,
       product.v2ProductName, 
       product.productQuantity,
       product.productRevenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) as hits,
unnest(hits.product) as product
where product.productRevenue is not null
),
same_order as (
select distinct fullVisitorId
  from product
  where product.v2ProductName = "YouTube Men's Vintage Henley"
)

select distinct product.v2ProductName as other_purchased_products,
       sum(product.productQuantity) as quantity 
from product
inner join same_order
using(fullVisitorId)
where product.v2ProductName != "YouTube Men's Vintage Henley"
group by other_purchased_products
order by quantity DESC



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview
then 40% add_to_cart and 10% purchase.


with view as(
SELECT format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
       COUNT(product.v2ProductName) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest(hits) as hits,
unnest(hits.product) as product
WHERE hits.ecommerceaction.action_type = "2"
and _TABLE_SUFFIX between "0101" and "0331"
group by month
),
add_to_cart as(
SELECT format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
       COUNT(product.v2ProductName) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest(hits) as hits,
unnest(hits.product) as product
WHERE hits.eCommerceAction.action_type = '3' 
and _TABLE_SUFFIX between "0101" and "0331"
group by month
),
purchase as(
SELECT format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
       COUNT(product.v2ProductName) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest(hits) as hits,
unnest(hits.product) as product
WHERE hits.eCommerceAction.action_type = '6'
and _TABLE_SUFFIX between "0101" and "0331"
group by month
)

SELECT month,
       num_product_view,
       num_addtocart,
       round((num_addtocart*100/ num_product_view),2) as add_to_cart_rate,
       round((num_purchase*100/ num_product_view),2) as purchase_rate
from view
left join add_to_cart 
using(month)
left join purchase
using(month)
order by month




