-- ============================================================================
-- Data Pipeline Simulation for Lineage Generation
-- 
-- This script creates lineage relationships between tables.
-- Recorded in system.access.table_lineage.
-- ============================================================================

-- ============================================================================
-- BRONZE → SILVER Pipeline (Lineage Generation)
-- ============================================================================

-- 1. bronze.user_events → silver.user_events_cleaned
INSERT INTO hurcy.silver.user_events_cleaned
SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp,
    CAST(event_timestamp AS DATE) AS event_date,
    page_url,
    CASE 
        WHEN page_url LIKE '%/product%' THEN 'product'
        WHEN page_url LIKE '%/cart%' THEN 'cart'
        WHEN page_url LIKE '%/checkout%' THEN 'checkout'
        ELSE 'other'
    END AS page_category,
    session_id,
    device_type,
    device_type IN ('mobile', 'tablet') AS is_mobile
FROM hurcy.bronze.user_events
WHERE event_timestamp IS NOT NULL;

-- 2. bronze.orders_raw → silver.orders_cleaned
INSERT INTO hurcy.silver.orders_cleaned
SELECT 
    order_id,
    customer_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    total_amount,
    CASE currency 
        WHEN 'USD' THEN total_amount 
        WHEN 'KRW' THEN total_amount / 1300 
        ELSE total_amount 
    END AS total_amount_usd,
    status,
    status = 'COMPLETED' AS is_completed
FROM hurcy.bronze.orders_raw
WHERE order_id IS NOT NULL;

-- 3. bronze.products_master → silver.products_enriched
INSERT INTO hurcy.silver.products_enriched
SELECT 
    product_id,
    product_name,
    category_id,
    COALESCE(category_id, 'Unknown') AS category_name,
    subcategory_id,
    brand,
    price,
    cost,
    ROUND((price - cost) / price * 100, 2) AS margin_pct,
    status = 'ACTIVE' AS is_active
FROM hurcy.bronze.products_master;

-- 4. bronze.customers → silver.customer_profiles
INSERT INTO hurcy.silver.customer_profiles
SELECT 
    customer_id,
    email,
    CONCAT(first_name, ' ', last_name) AS full_name,
    FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) AS age,
    CASE 
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 20 THEN 'Teen'
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 30 THEN '20s'
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 40 THEN '30s'
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 50 THEN '40s'
        ELSE '50+'
    END AS age_group,
    gender,
    registration_date,
    DATEDIFF(CURRENT_DATE(), registration_date) AS tenure_days,
    country,
    COALESCE(country, 'Unknown') AS region
FROM hurcy.bronze.customers;

-- 5. bronze.payments + bronze.orders_raw → silver.payment_details
INSERT INTO hurcy.silver.payment_details
SELECT 
    p.payment_id,
    p.order_id,
    o.customer_id,
    p.payment_method,
    CASE 
        WHEN p.payment_method IN ('VISA', 'MASTERCARD', 'AMEX') THEN 'CARD'
        WHEN p.payment_method IN ('PAYPAL', 'APPLEPAY') THEN 'DIGITAL'
        ELSE 'OTHER'
    END AS payment_category,
    p.amount,
    CASE p.currency 
        WHEN 'USD' THEN p.amount 
        WHEN 'KRW' THEN p.amount / 1300 
        ELSE p.amount 
    END AS amount_usd,
    p.status,
    p.status = 'SUCCESS' AS is_successful,
    CAST(p.processed_at AS DATE) AS processed_date
FROM hurcy.bronze.payments p
JOIN hurcy.bronze.orders_raw o ON p.order_id = o.order_id;

-- 6. bronze.reviews → silver.review_analysis
INSERT INTO hurcy.silver.review_analysis
SELECT 
    review_id,
    product_id,
    customer_id,
    rating,
    (rating - 3) / 2.0 AS sentiment_score,
    CASE 
        WHEN rating >= 4 THEN 'POSITIVE'
        WHEN rating = 3 THEN 'NEUTRAL'
        ELSE 'NEGATIVE'
    END AS sentiment_label,
    LENGTH(review_text) AS review_length,
    review_date,
    verified_purchase AS is_verified
FROM hurcy.bronze.reviews;

-- 7. bronze.inventory → silver.inventory_status
INSERT INTO hurcy.silver.inventory_status
SELECT 
    product_id,
    warehouse_id,
    quantity - reserved_quantity AS available_quantity,
    reserved_quantity,
    quantity AS total_quantity,
    CASE 
        WHEN quantity - reserved_quantity > 100 THEN 'IN_STOCK'
        WHEN quantity - reserved_quantity > 0 THEN 'LOW_STOCK'
        ELSE 'OUT_OF_STOCK'
    END AS stock_status,
    CAST(last_updated AS DATE) AS last_updated
FROM hurcy.bronze.inventory;

-- 8. bronze.shipments + bronze.orders_raw → silver.shipment_tracking
INSERT INTO hurcy.silver.shipment_tracking
SELECT 
    s.shipment_id,
    s.order_id,
    o.customer_id,
    s.carrier,
    s.status,
    s.shipped_date,
    COALESCE(s.actual_delivery, s.estimated_delivery) AS delivery_date,
    DATEDIFF(COALESCE(s.actual_delivery, s.estimated_delivery), s.shipped_date) AS delivery_days,
    s.actual_delivery <= s.estimated_delivery AS is_on_time
FROM hurcy.bronze.shipments s
JOIN hurcy.bronze.orders_raw o ON s.order_id = o.order_id;

-- 9. bronze.sessions + bronze.user_events → silver.session_analysis
INSERT INTO hurcy.silver.session_analysis
SELECT 
    s.session_id,
    s.user_id,
    CAST(s.start_time AS DATE) AS session_date,
    UNIX_TIMESTAMP(s.end_time) - UNIX_TIMESTAMP(s.start_time) AS session_duration_sec,
    COUNT(DISTINCT e.page_url) AS page_views,
    COUNT(e.event_id) AS events_count,
    s.device_type,
    s.country,
    COUNT(e.event_id) <= 1 AS is_bounce
FROM hurcy.bronze.sessions s
LEFT JOIN hurcy.bronze.user_events e ON s.session_id = e.session_id
GROUP BY s.session_id, s.user_id, s.start_time, s.end_time, s.device_type, s.country;

-- ============================================================================
-- SILVER → GOLD Pipeline (Lineage Generation)
-- ============================================================================

-- 10. silver.orders_cleaned + silver.payment_details → gold.daily_sales_summary
INSERT INTO hurcy.gold.daily_sales_summary
SELECT 
    o.order_date AS sales_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount_usd) AS total_revenue,
    AVG(o.total_amount_usd) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(CASE WHEN o.is_completed THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN NOT o.is_completed THEN 1 ELSE 0 END) AS cancelled_orders
FROM hurcy.silver.orders_cleaned o
LEFT JOIN hurcy.silver.payment_details p ON o.order_id = p.order_id
GROUP BY o.order_date;

-- 11. silver.customer_profiles + silver.orders_cleaned → gold.customer_segments
INSERT INTO hurcy.gold.customer_segments
SELECT 
    c.customer_id,
    CASE 
        WHEN SUM(o.total_amount_usd) > 10000 THEN 'VIP'
        WHEN SUM(o.total_amount_usd) > 1000 THEN 'Premium'
        WHEN COUNT(o.order_id) > 5 THEN 'Loyal'
        ELSE 'Regular'
    END AS segment,
    CONCAT(
        CASE WHEN MAX(o.order_date) > DATE_SUB(CURRENT_DATE(), 30) THEN 'H' ELSE 'L' END,
        CASE WHEN COUNT(o.order_id) > 3 THEN 'H' ELSE 'L' END,
        CASE WHEN SUM(o.total_amount_usd) > 500 THEN 'H' ELSE 'L' END
    ) AS rfm_score,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount_usd) AS total_spend,
    AVG(o.total_amount_usd) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    c.tenure_days AS customer_lifetime_days
FROM hurcy.silver.customer_profiles c
LEFT JOIN hurcy.silver.orders_cleaned o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.tenure_days;

-- 12. silver.order_items + silver.products_enriched + silver.review_analysis → gold.product_performance
INSERT INTO hurcy.gold.product_performance
SELECT 
    p.product_id,
    p.product_name,
    p.category_name,
    COALESCE(SUM(oi.quantity), 0) AS total_quantity_sold,
    COALESCE(SUM(oi.total_price), 0) AS total_revenue,
    AVG(r.rating) AS avg_rating,
    COUNT(DISTINCT r.review_id) AS review_count,
    0.05 AS return_rate  -- placeholder
FROM hurcy.silver.products_enriched p
LEFT JOIN hurcy.silver.order_items oi ON p.product_id = oi.product_id
LEFT JOIN hurcy.silver.review_analysis r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name, p.category_name;

-- 13. silver multiple tables → gold.monthly_kpi
INSERT INTO hurcy.gold.monthly_kpi
SELECT 
    DATE_FORMAT(o.order_date, 'yyyy-MM') AS year_month,
    SUM(o.total_amount_usd) AS total_revenue,
    0.0 AS revenue_growth_pct,
    COUNT(DISTINCT CASE WHEN c.tenure_days <= 30 THEN c.customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN c.tenure_days > 30 THEN c.customer_id END) AS returning_customers,
    AVG(o.total_amount_usd) AS avg_order_value,
    COUNT(DISTINCT o.order_id) * 1.0 / NULLIF(COUNT(DISTINCT sa.session_id), 0) AS conversion_rate
FROM hurcy.silver.orders_cleaned o
LEFT JOIN hurcy.silver.customer_profiles c ON o.customer_id = c.customer_id
LEFT JOIN hurcy.silver.session_analysis sa ON c.customer_id = sa.user_id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM');

-- 14. silver.session_analysis + silver.orders_cleaned → gold.channel_performance
INSERT INTO hurcy.gold.channel_performance
SELECT 
    sa.device_type AS channel,
    sa.session_date AS report_date,
    COUNT(DISTINCT sa.session_id) AS sessions,
    COUNT(DISTINCT sa.user_id) AS unique_users,
    COUNT(DISTINCT o.order_id) AS orders,
    COALESCE(SUM(o.total_amount_usd), 0) AS revenue,
    COUNT(DISTINCT o.order_id) * 100.0 / NULLIF(COUNT(DISTINCT sa.session_id), 0) AS conversion_rate,
    AVG(sa.session_duration_sec) AS avg_session_duration
FROM hurcy.silver.session_analysis sa
LEFT JOIN hurcy.silver.orders_cleaned o ON sa.user_id = o.customer_id 
    AND sa.session_date = o.order_date
GROUP BY sa.device_type, sa.session_date;

-- ============================================================================
-- LEGACY Table Pipeline (Duplicate Lineage Generation - Same Source)
-- ============================================================================

-- 15. bronze.user_events → legacy.tb_user_activity_log (same source as silver.user_events_cleaned)
INSERT INTO hurcy.legacy.tb_user_activity_log
SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp,
    CAST(event_timestamp AS DATE) AS event_date,
    page_url,
    CASE 
        WHEN page_url LIKE '%/product%' THEN 'product'
        WHEN page_url LIKE '%/cart%' THEN 'cart'
        ELSE 'other'
    END AS page_category,
    session_id,
    device_type,
    device_type IN ('mobile', 'tablet') AS is_mobile
FROM hurcy.bronze.user_events;

-- 16. bronze.orders_raw → legacy.order_transaction_history (same source as silver.orders_cleaned)
INSERT INTO hurcy.legacy.order_transaction_history
SELECT 
    order_id AS ord_id,
    customer_id AS cust_id,
    order_date AS ord_dt,
    YEAR(order_date) AS ord_year,
    MONTH(order_date) AS ord_month,
    total_amount AS tot_amt,
    total_amount AS tot_amt_usd,
    status AS ord_status,
    status = 'COMPLETED' AS is_complete
FROM hurcy.bronze.orders_raw;

-- 17. bronze.customers → legacy.customer_master_old (same source as silver.customer_profiles)
INSERT INTO hurcy.legacy.customer_master_old
SELECT 
    customer_id,
    email,
    CONCAT(first_name, ' ', last_name) AS full_name,
    FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) AS age,
    CASE 
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 30 THEN 'Young'
        WHEN FLOOR(DATEDIFF(CURRENT_DATE(), birth_date) / 365) < 50 THEN 'Middle'
        ELSE 'Senior'
    END AS age_group,
    gender,
    registration_date,
    DATEDIFF(CURRENT_DATE(), registration_date) AS tenure_days,
    country
FROM hurcy.bronze.customers;

-- 18. silver.orders_cleaned + silver.payment_details → legacy.daily_revenue_report (same source as gold.daily_sales_summary)
INSERT INTO hurcy.legacy.daily_revenue_report
SELECT 
    o.order_date AS report_date,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount_usd) AS gross_revenue,
    AVG(o.total_amount_usd) AS average_order_amount,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    SUM(CASE WHEN p.is_successful THEN 1 ELSE 0 END) AS successful_orders,
    SUM(CASE WHEN NOT p.is_successful THEN 1 ELSE 0 END) AS failed_orders
FROM hurcy.silver.orders_cleaned o
LEFT JOIN hurcy.silver.payment_details p ON o.order_id = p.order_id
GROUP BY o.order_date;

-- 19. bronze.products_master → legacy.product_catalog_v1 (same source as silver.products_enriched)
INSERT INTO hurcy.legacy.product_catalog_v1
SELECT 
    product_id AS prod_id,
    product_name AS prod_name,
    category_id AS cat_id,
    category_id AS cat_name,
    subcategory_id AS subcat_id,
    brand AS brand_name,
    price AS retail_price,
    cost AS wholesale_cost,
    ROUND((price - cost) / price * 100, 2) AS profit_margin,
    status = 'ACTIVE' AS active_flag
FROM hurcy.bronze.products_master;

-- 20. bronze.payments + bronze.orders_raw → legacy.payment_transaction_log (same source as silver.payment_details)
INSERT INTO hurcy.legacy.payment_transaction_log
SELECT 
    p.payment_id AS pay_id,
    p.order_id AS ord_id,
    o.customer_id AS cust_id,
    p.payment_method AS pay_method,
    CASE 
        WHEN p.payment_method IN ('VISA', 'MASTERCARD') THEN 'CARD'
        ELSE 'OTHER'
    END AS pay_type,
    p.amount AS pay_amount,
    p.amount AS pay_amount_usd,
    p.status AS pay_status,
    p.status = 'SUCCESS' AS success_flag,
    CAST(p.processed_at AS DATE) AS process_date
FROM hurcy.bronze.payments p
JOIN hurcy.bronze.orders_raw o ON p.order_id = o.order_id;

-- 21. silver.customer_profiles + silver.orders_cleaned → legacy.customer_segmentation_old (same source as gold.customer_segments)
INSERT INTO hurcy.legacy.customer_segmentation_old
SELECT 
    c.customer_id AS cust_id,
    CASE 
        WHEN SUM(o.total_amount_usd) > 5000 THEN 'High Value'
        WHEN COUNT(o.order_id) > 3 THEN 'Frequent'
        ELSE 'Standard'
    END AS segment_name,
    'HHH' AS rfm,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount_usd) AS lifetime_value,
    AVG(o.total_amount_usd) AS aov,
    MAX(o.order_date) AS last_purchase_date,
    c.tenure_days AS days_as_customer
FROM hurcy.silver.customer_profiles c
LEFT JOIN hurcy.silver.orders_cleaned o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.tenure_days;

-- 22. bronze.reviews → legacy.product_review_stats (same source as silver.review_analysis)
INSERT INTO hurcy.legacy.product_review_stats
SELECT 
    review_id AS rev_id,
    product_id AS prod_id,
    customer_id AS cust_id,
    rating AS star_rating,
    (rating - 3) / 2.0 AS sentiment,
    CASE WHEN rating >= 4 THEN 'POS' WHEN rating = 3 THEN 'NEU' ELSE 'NEG' END AS sentiment_category,
    LENGTH(review_text) AS text_length,
    review_date AS review_dt,
    verified_purchase AS verified_flag
FROM hurcy.bronze.reviews;

-- 23. silver.order_items + silver.products_enriched → legacy.item_sales_analytics (same source as gold.product_performance)
INSERT INTO hurcy.legacy.item_sales_analytics
SELECT 
    p.product_id AS item_id,
    p.product_name AS item_name,
    p.category_name AS category,
    COALESCE(SUM(oi.quantity), 0) AS units_sold,
    COALESCE(SUM(oi.total_price), 0) AS sales_amount,
    4.0 AS avg_customer_rating,
    10 AS num_reviews,
    0.03 AS return_percentage
FROM hurcy.silver.products_enriched p
LEFT JOIN hurcy.silver.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category_name;

-- 24. bronze.shipments + bronze.orders_raw → legacy.delivery_tracking_old (same source as silver.shipment_tracking)
INSERT INTO hurcy.legacy.delivery_tracking_old
SELECT 
    s.shipment_id AS ship_id,
    s.order_id,
    o.customer_id AS buyer_id,
    s.carrier AS shipping_company,
    s.status AS delivery_status,
    s.shipped_date AS ship_date,
    s.actual_delivery AS arrive_date,
    DATEDIFF(s.actual_delivery, s.shipped_date) AS transit_days,
    s.actual_delivery <= s.estimated_delivery AS on_time_flag
FROM hurcy.bronze.shipments s
JOIN hurcy.bronze.orders_raw o ON s.order_id = o.order_id;

-- 25. bronze.sessions + bronze.user_events → legacy.web_session_metrics (same source as silver.session_analysis)
INSERT INTO hurcy.legacy.web_session_metrics
SELECT 
    s.session_id AS sess_id,
    s.user_id AS visitor_id,
    CAST(s.start_time AS DATE) AS visit_date,
    UNIX_TIMESTAMP(s.end_time) - UNIX_TIMESTAMP(s.start_time) AS duration_seconds,
    COUNT(DISTINCT e.page_url) AS pages_viewed,
    COUNT(e.event_id) AS total_events,
    s.device_type AS device,
    s.country AS geo_country,
    COUNT(e.event_id) <= 1 AS bounced
FROM hurcy.bronze.sessions s
LEFT JOIN hurcy.bronze.user_events e ON s.session_id = e.session_id
GROUP BY s.session_id, s.user_id, s.start_time, s.end_time, s.device_type, s.country;

-- 26. bronze.inventory → legacy.stock_levels (same source as silver.inventory_status)
INSERT INTO hurcy.legacy.stock_levels
SELECT 
    product_id AS sku_id,
    warehouse_id AS location_id,
    quantity - reserved_quantity AS qty_available,
    reserved_quantity AS qty_reserved,
    quantity AS qty_total,
    CASE WHEN quantity > 0 THEN 'AVAILABLE' ELSE 'EMPTY' END AS inventory_status,
    CAST(last_updated AS DATE) AS updated_date
FROM hurcy.bronze.inventory;

-- 27. silver multiple tables → legacy.monthly_business_metrics (same source as gold.monthly_kpi)
INSERT INTO hurcy.legacy.monthly_business_metrics
SELECT 
    DATE_FORMAT(o.order_date, 'yyyy-MM') AS period,
    SUM(o.total_amount_usd) AS revenue,
    0.0 AS growth_rate,
    COUNT(DISTINCT CASE WHEN c.tenure_days <= 30 THEN c.customer_id END) AS new_customer_count,
    COUNT(DISTINCT CASE WHEN c.tenure_days > 30 THEN c.customer_id END) AS repeat_customer_count,
    AVG(o.total_amount_usd) AS basket_size,
    0.05 AS cvr
FROM hurcy.silver.orders_cleaned o
LEFT JOIN hurcy.silver.customer_profiles c ON o.customer_id = c.customer_id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM');

-- 28. silver.session_analysis + silver.orders_cleaned → legacy.marketing_channel_stats (same source as gold.channel_performance)
INSERT INTO hurcy.legacy.marketing_channel_stats
SELECT 
    sa.device_type AS channel_name,
    sa.session_date AS stat_date,
    COUNT(sa.session_id) AS session_count,
    COUNT(DISTINCT sa.user_id) AS visitor_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.total_amount_usd) AS channel_revenue,
    COUNT(DISTINCT o.order_id) * 100.0 / NULLIF(COUNT(sa.session_id), 0) AS cvr_pct,
    AVG(sa.session_duration_sec) AS avg_duration
FROM hurcy.silver.session_analysis sa
LEFT JOIN hurcy.silver.orders_cleaned o ON sa.user_id = o.customer_id
GROUP BY sa.device_type, sa.session_date;

-- 29. bronze.orders_raw + bronze.products_master → legacy.order_line_details (similar to silver.order_items)
INSERT INTO hurcy.legacy.order_line_details
SELECT 
    CONCAT(o.order_id, '-1') AS line_id,
    o.order_id AS order_num,
    p.product_id AS item_id,
    p.product_name AS item_name,
    1 AS qty,
    p.price AS price_each,
    p.price AS line_total,
    0 AS discount
FROM hurcy.bronze.orders_raw o
CROSS JOIN hurcy.bronze.products_master p
LIMIT 1000;

-- ============================================================================
-- MART Table Pipeline (Additional Duplicate Lineage)
-- ============================================================================

-- 30. silver.orders_cleaned + silver.payment_details → mart.sales_dashboard_daily (same source as gold.daily_sales_summary)
INSERT INTO hurcy.mart.sales_dashboard_daily
SELECT 
    o.order_date AS date_key,
    COUNT(o.order_id) AS num_orders,
    SUM(o.total_amount_usd) AS total_sales,
    AVG(o.total_amount_usd) AS mean_order_value,
    COUNT(DISTINCT o.customer_id) AS distinct_customers,
    SUM(CASE WHEN o.is_completed THEN 1 ELSE 0 END) AS completed_count,
    SUM(CASE WHEN NOT o.is_completed THEN 1 ELSE 0 END) AS cancelled_count
FROM hurcy.silver.orders_cleaned o
LEFT JOIN hurcy.silver.payment_details p ON o.order_id = p.order_id
GROUP BY o.order_date;

-- 31. silver.customer_profiles + silver.orders_cleaned → mart.customer_value_tiers (similar to gold.customer_segments)
INSERT INTO hurcy.mart.customer_value_tiers
SELECT 
    c.customer_id AS customer_key,
    CASE 
        WHEN SUM(o.total_amount_usd) > 10000 THEN 'Platinum'
        WHEN SUM(o.total_amount_usd) > 5000 THEN 'Gold'
        WHEN SUM(o.total_amount_usd) > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END AS value_tier,
    'HML' AS rfm_combined,
    COUNT(o.order_id) AS order_frequency,
    SUM(o.total_amount_usd) AS monetary_value,
    AVG(o.total_amount_usd) AS avg_basket,
    MAX(o.order_date) AS recency_date,
    c.tenure_days AS customer_age_days,
    CASE 
        WHEN MAX(o.order_date) < DATE_SUB(CURRENT_DATE(), 90) THEN 'HIGH'
        WHEN MAX(o.order_date) < DATE_SUB(CURRENT_DATE(), 30) THEN 'MEDIUM'
        ELSE 'LOW'
    END AS churn_risk
FROM hurcy.silver.customer_profiles c
LEFT JOIN hurcy.silver.orders_cleaned o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.tenure_days;

