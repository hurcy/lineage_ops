-- ============================================================================
-- 의미적 중복 탐지 테스트용 DDL
-- 카탈로그: hurcy
-- 
-- 구조:
--   1. 소스 테이블 (bronze): raw 데이터
--   2. 파생 테이블 (silver): 정제된 데이터
--   3. 중복 테이블: 이름은 다르지만 스키마가 유사한 테이블들
--   4. 집계 테이블 (gold): 비즈니스 지표
-- ============================================================================

-- ============================================================================
-- 스키마 생성
-- ============================================================================

-- 1. Bronze 스키마
CREATE SCHEMA IF NOT EXISTS hurcy.bronze COMMENT 'Raw data layer';

-- 2. Silver 스키마
CREATE SCHEMA IF NOT EXISTS hurcy.silver COMMENT 'Cleaned and enriched data';

-- 3. Gold 스키마
CREATE SCHEMA IF NOT EXISTS hurcy.gold COMMENT 'Business-level aggregates';

-- 4. 레거시 스키마 (중복 테이블 포함)
CREATE SCHEMA IF NOT EXISTS hurcy.legacy COMMENT 'Legacy tables - candidates for dedup';

-- 5. 마트 스키마
CREATE SCHEMA IF NOT EXISTS hurcy.mart COMMENT 'Data mart tables';

-- ============================================================================
-- BRONZE 레이어 - 소스 테이블 (10개)
-- ============================================================================

-- 6. 사용자 이벤트 로그
CREATE TABLE IF NOT EXISTS hurcy.bronze.user_events (
    event_id STRING COMMENT '이벤트 고유 ID',
    user_id STRING COMMENT '사용자 ID',
    event_type STRING COMMENT '이벤트 유형',
    event_timestamp TIMESTAMP COMMENT '이벤트 발생 시간',
    page_url STRING COMMENT '페이지 URL',
    session_id STRING COMMENT '세션 ID',
    device_type STRING COMMENT '디바이스 유형',
    browser STRING COMMENT '브라우저',
    ip_address STRING COMMENT 'IP 주소',
    raw_payload STRING COMMENT '원본 JSON 페이로드',
    ingested_at TIMESTAMP COMMENT '수집 시간'
) COMMENT 'Raw user event logs from web/app';

-- 7. 주문 원본 데이터
CREATE TABLE IF NOT EXISTS hurcy.bronze.orders_raw (
    order_id STRING COMMENT '주문 ID',
    customer_id STRING COMMENT '고객 ID',
    order_date DATE COMMENT '주문 일자',
    total_amount DECIMAL(18,2) COMMENT '총 금액',
    currency STRING COMMENT '통화',
    status STRING COMMENT '주문 상태',
    shipping_address STRING COMMENT '배송 주소',
    billing_address STRING COMMENT '청구 주소',
    created_at TIMESTAMP COMMENT '생성 시간',
    updated_at TIMESTAMP COMMENT '수정 시간'
) COMMENT 'Raw order data from e-commerce system';

-- 8. 상품 마스터
CREATE TABLE IF NOT EXISTS hurcy.bronze.products_master (
    product_id STRING COMMENT '상품 ID',
    product_name STRING COMMENT '상품명',
    category_id STRING COMMENT '카테고리 ID',
    subcategory_id STRING COMMENT '서브카테고리 ID',
    brand STRING COMMENT '브랜드',
    price DECIMAL(18,2) COMMENT '가격',
    cost DECIMAL(18,2) COMMENT '원가',
    sku STRING COMMENT 'SKU',
    status STRING COMMENT '상품 상태',
    created_date DATE COMMENT '등록일'
) COMMENT 'Product master data';

-- 9. 고객 정보
CREATE TABLE IF NOT EXISTS hurcy.bronze.customers (
    customer_id STRING COMMENT '고객 ID',
    email STRING COMMENT '이메일',
    phone STRING COMMENT '전화번호',
    first_name STRING COMMENT '이름',
    last_name STRING COMMENT '성',
    birth_date DATE COMMENT '생년월일',
    gender STRING COMMENT '성별',
    registration_date DATE COMMENT '가입일',
    country STRING COMMENT '국가',
    city STRING COMMENT '도시'
) COMMENT 'Customer master data';

-- 10. 결제 정보
CREATE TABLE IF NOT EXISTS hurcy.bronze.payments (
    payment_id STRING COMMENT '결제 ID',
    order_id STRING COMMENT '주문 ID',
    payment_method STRING COMMENT '결제 수단',
    amount DECIMAL(18,2) COMMENT '결제 금액',
    currency STRING COMMENT '통화',
    status STRING COMMENT '결제 상태',
    transaction_id STRING COMMENT '거래 ID',
    processed_at TIMESTAMP COMMENT '처리 시간'
) COMMENT 'Payment transaction data';

-- 11. 리뷰 데이터
CREATE TABLE IF NOT EXISTS hurcy.bronze.reviews (
    review_id STRING COMMENT '리뷰 ID',
    product_id STRING COMMENT '상품 ID',
    customer_id STRING COMMENT '고객 ID',
    rating INT COMMENT '평점 (1-5)',
    review_text STRING COMMENT '리뷰 내용',
    review_date DATE COMMENT '리뷰 작성일',
    helpful_votes INT COMMENT '도움됨 투표 수',
    verified_purchase BOOLEAN COMMENT '구매 확인 여부'
) COMMENT 'Product review data';

-- 12. 재고 데이터
CREATE TABLE IF NOT EXISTS hurcy.bronze.inventory (
    inventory_id STRING COMMENT '재고 ID',
    product_id STRING COMMENT '상품 ID',
    warehouse_id STRING COMMENT '창고 ID',
    quantity INT COMMENT '수량',
    reserved_quantity INT COMMENT '예약 수량',
    last_updated TIMESTAMP COMMENT '최종 업데이트'
) COMMENT 'Inventory levels by warehouse';

-- 13. 배송 정보
CREATE TABLE IF NOT EXISTS hurcy.bronze.shipments (
    shipment_id STRING COMMENT '배송 ID',
    order_id STRING COMMENT '주문 ID',
    carrier STRING COMMENT '배송사',
    tracking_number STRING COMMENT '운송장 번호',
    status STRING COMMENT '배송 상태',
    shipped_date DATE COMMENT '발송일',
    estimated_delivery DATE COMMENT '예상 도착일',
    actual_delivery DATE COMMENT '실제 도착일'
) COMMENT 'Shipment tracking data';

-- 14. 세션 로그
CREATE TABLE IF NOT EXISTS hurcy.bronze.sessions (
    session_id STRING COMMENT '세션 ID',
    user_id STRING COMMENT '사용자 ID',
    start_time TIMESTAMP COMMENT '세션 시작',
    end_time TIMESTAMP COMMENT '세션 종료',
    device_type STRING COMMENT '디바이스',
    os STRING COMMENT '운영체제',
    browser STRING COMMENT '브라우저',
    country STRING COMMENT '국가',
    city STRING COMMENT '도시'
) COMMENT 'User session data';

-- 15. 캠페인 데이터
CREATE TABLE IF NOT EXISTS hurcy.bronze.campaigns (
    campaign_id STRING COMMENT '캠페인 ID',
    campaign_name STRING COMMENT '캠페인명',
    campaign_type STRING COMMENT '캠페인 유형',
    start_date DATE COMMENT '시작일',
    end_date DATE COMMENT '종료일',
    budget DECIMAL(18,2) COMMENT '예산',
    channel STRING COMMENT '채널',
    target_audience STRING COMMENT '타겟 오디언스'
) COMMENT 'Marketing campaign data';

-- ============================================================================
-- SILVER 레이어 - 정제된 테이블 (10개) - bronze에서 파생
-- ============================================================================

-- 16. 정제된 사용자 이벤트 (bronze.user_events에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.user_events_cleaned (
    event_id STRING COMMENT '이벤트 고유 ID',
    user_id STRING COMMENT '사용자 ID',
    event_type STRING COMMENT '이벤트 유형',
    event_timestamp TIMESTAMP COMMENT '이벤트 발생 시간',
    event_date DATE COMMENT '이벤트 날짜',
    page_url STRING COMMENT '페이지 URL',
    page_category STRING COMMENT '페이지 카테고리',
    session_id STRING COMMENT '세션 ID',
    device_type STRING COMMENT '디바이스 유형',
    is_mobile BOOLEAN COMMENT '모바일 여부'
) COMMENT 'Cleaned user events from bronze.user_events';

-- 17. 정제된 주문 (bronze.orders_raw에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.orders_cleaned (
    order_id STRING COMMENT '주문 ID',
    customer_id STRING COMMENT '고객 ID',
    order_date DATE COMMENT '주문 일자',
    order_year INT COMMENT '주문 연도',
    order_month INT COMMENT '주문 월',
    total_amount DECIMAL(18,2) COMMENT '총 금액',
    total_amount_usd DECIMAL(18,2) COMMENT 'USD 환산 금액',
    status STRING COMMENT '주문 상태',
    is_completed BOOLEAN COMMENT '완료 여부'
) COMMENT 'Cleaned orders from bronze.orders_raw';

-- 18. 상품 정보 (bronze.products_master에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.products_enriched (
    product_id STRING COMMENT '상품 ID',
    product_name STRING COMMENT '상품명',
    category_id STRING COMMENT '카테고리 ID',
    category_name STRING COMMENT '카테고리명',
    subcategory_id STRING COMMENT '서브카테고리 ID',
    brand STRING COMMENT '브랜드',
    price DECIMAL(18,2) COMMENT '가격',
    cost DECIMAL(18,2) COMMENT '원가',
    margin_pct DECIMAL(5,2) COMMENT '마진율',
    is_active BOOLEAN COMMENT '활성 여부'
) COMMENT 'Enriched product data from bronze.products_master';

-- 19. 고객 프로필 (bronze.customers에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.customer_profiles (
    customer_id STRING COMMENT '고객 ID',
    email STRING COMMENT '이메일',
    full_name STRING COMMENT '전체 이름',
    age INT COMMENT '나이',
    age_group STRING COMMENT '연령대',
    gender STRING COMMENT '성별',
    registration_date DATE COMMENT '가입일',
    tenure_days INT COMMENT '가입 후 경과일',
    country STRING COMMENT '국가',
    region STRING COMMENT '지역'
) COMMENT 'Customer profiles from bronze.customers';

-- 20. 결제 상세 (bronze.payments, bronze.orders_raw에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.payment_details (
    payment_id STRING COMMENT '결제 ID',
    order_id STRING COMMENT '주문 ID',
    customer_id STRING COMMENT '고객 ID',
    payment_method STRING COMMENT '결제 수단',
    payment_category STRING COMMENT '결제 수단 분류',
    amount DECIMAL(18,2) COMMENT '결제 금액',
    amount_usd DECIMAL(18,2) COMMENT 'USD 환산',
    status STRING COMMENT '결제 상태',
    is_successful BOOLEAN COMMENT '성공 여부',
    processed_date DATE COMMENT '처리일'
) COMMENT 'Payment details from bronze.payments + bronze.orders_raw';

-- 21. 주문 아이템 (bronze.orders_raw, bronze.products_master에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.order_items (
    order_item_id STRING COMMENT '주문 아이템 ID',
    order_id STRING COMMENT '주문 ID',
    product_id STRING COMMENT '상품 ID',
    product_name STRING COMMENT '상품명',
    quantity INT COMMENT '수량',
    unit_price DECIMAL(18,2) COMMENT '단가',
    total_price DECIMAL(18,2) COMMENT '총액',
    discount_amount DECIMAL(18,2) COMMENT '할인 금액'
) COMMENT 'Order line items';

-- 22. 리뷰 분석 (bronze.reviews에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.review_analysis (
    review_id STRING COMMENT '리뷰 ID',
    product_id STRING COMMENT '상품 ID',
    customer_id STRING COMMENT '고객 ID',
    rating INT COMMENT '평점',
    sentiment_score DECIMAL(5,2) COMMENT '감성 점수',
    sentiment_label STRING COMMENT '감성 라벨',
    review_length INT COMMENT '리뷰 길이',
    review_date DATE COMMENT '리뷰 날짜',
    is_verified BOOLEAN COMMENT '인증 구매'
) COMMENT 'Review analysis from bronze.reviews';

-- 23. 재고 현황 (bronze.inventory에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.inventory_status (
    product_id STRING COMMENT '상품 ID',
    warehouse_id STRING COMMENT '창고 ID',
    available_quantity INT COMMENT '가용 수량',
    reserved_quantity INT COMMENT '예약 수량',
    total_quantity INT COMMENT '총 수량',
    stock_status STRING COMMENT '재고 상태',
    last_updated DATE COMMENT '최종 업데이트일'
) COMMENT 'Inventory status from bronze.inventory';

-- 24. 배송 추적 (bronze.shipments, bronze.orders_raw에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.shipment_tracking (
    shipment_id STRING COMMENT '배송 ID',
    order_id STRING COMMENT '주문 ID',
    customer_id STRING COMMENT '고객 ID',
    carrier STRING COMMENT '배송사',
    status STRING COMMENT '배송 상태',
    shipped_date DATE COMMENT '발송일',
    delivery_date DATE COMMENT '도착일',
    delivery_days INT COMMENT '배송 소요일',
    is_on_time BOOLEAN COMMENT '정시 배송 여부'
) COMMENT 'Shipment tracking from bronze.shipments + bronze.orders_raw';

-- 25. 세션 분석 (bronze.sessions, bronze.user_events에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.silver.session_analysis (
    session_id STRING COMMENT '세션 ID',
    user_id STRING COMMENT '사용자 ID',
    session_date DATE COMMENT '세션 날짜',
    session_duration_sec INT COMMENT '세션 시간(초)',
    page_views INT COMMENT '페이지뷰 수',
    events_count INT COMMENT '이벤트 수',
    device_type STRING COMMENT '디바이스',
    country STRING COMMENT '국가',
    is_bounce BOOLEAN COMMENT '이탈 여부'
) COMMENT 'Session analysis from bronze.sessions + bronze.user_events';

-- ============================================================================
-- GOLD 레이어 - 집계 테이블 (10개) - silver에서 파생
-- ============================================================================

-- 26. 일별 매출 요약 (silver.orders_cleaned, silver.payment_details에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.gold.daily_sales_summary (
    sales_date DATE COMMENT '매출 일자',
    total_orders INT COMMENT '총 주문 수',
    total_revenue DECIMAL(18,2) COMMENT '총 매출',
    avg_order_value DECIMAL(18,2) COMMENT '평균 주문 금액',
    unique_customers INT COMMENT '고유 고객 수',
    completed_orders INT COMMENT '완료된 주문',
    cancelled_orders INT COMMENT '취소된 주문'
) COMMENT 'Daily sales summary';

-- 27. 고객 세그먼트 (silver.customer_profiles, silver.orders_cleaned에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.gold.customer_segments (
    customer_id STRING COMMENT '고객 ID',
    segment STRING COMMENT '세그먼트',
    rfm_score STRING COMMENT 'RFM 점수',
    total_orders INT COMMENT '총 주문 수',
    total_spend DECIMAL(18,2) COMMENT '총 지출',
    avg_order_value DECIMAL(18,2) COMMENT '평균 주문 금액',
    last_order_date DATE COMMENT '마지막 주문일',
    customer_lifetime_days INT COMMENT '고객 생애 일수'
) COMMENT 'Customer segmentation';

-- 28. 상품 성과 (silver.order_items, silver.products_enriched에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.gold.product_performance (
    product_id STRING COMMENT '상품 ID',
    product_name STRING COMMENT '상품명',
    category_name STRING COMMENT '카테고리',
    total_quantity_sold INT COMMENT '총 판매 수량',
    total_revenue DECIMAL(18,2) COMMENT '총 매출',
    avg_rating DECIMAL(3,2) COMMENT '평균 평점',
    review_count INT COMMENT '리뷰 수',
    return_rate DECIMAL(5,2) COMMENT '반품률'
) COMMENT 'Product performance metrics';

-- 29. 월별 KPI (silver 여러 테이블에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.gold.monthly_kpi (
    year_month STRING COMMENT '연월',
    total_revenue DECIMAL(18,2) COMMENT '총 매출',
    revenue_growth_pct DECIMAL(5,2) COMMENT '매출 성장률',
    new_customers INT COMMENT '신규 고객',
    returning_customers INT COMMENT '재구매 고객',
    avg_order_value DECIMAL(18,2) COMMENT '평균 주문 금액',
    conversion_rate DECIMAL(5,2) COMMENT '전환율'
) COMMENT 'Monthly KPI dashboard';

-- 30. 채널별 성과 (silver.session_analysis, silver.orders_cleaned에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.gold.channel_performance (
    channel STRING COMMENT '채널',
    report_date DATE COMMENT '보고 일자',
    sessions INT COMMENT '세션 수',
    unique_users INT COMMENT '고유 사용자',
    orders INT COMMENT '주문 수',
    revenue DECIMAL(18,2) COMMENT '매출',
    conversion_rate DECIMAL(5,2) COMMENT '전환율',
    avg_session_duration INT COMMENT '평균 세션 시간'
) COMMENT 'Channel performance metrics';

-- ============================================================================
-- LEGACY 스키마 - 중복 테이블 (15개) - 다양한 중복 시나리오
-- ============================================================================

-- 31. 중복 시나리오 1: silver.user_events_cleaned와 유사 (99% 동일)
CREATE TABLE IF NOT EXISTS hurcy.legacy.tb_user_activity_log (
    event_id STRING COMMENT 'Event unique identifier',
    user_id STRING COMMENT 'User identifier',
    event_type STRING COMMENT 'Type of event',
    event_timestamp TIMESTAMP COMMENT 'When event occurred',
    event_date DATE COMMENT 'Event date',
    page_url STRING COMMENT 'Page URL',
    page_category STRING COMMENT 'Page category',
    session_id STRING COMMENT 'Session identifier',
    device_type STRING COMMENT 'Device type',
    is_mobile BOOLEAN COMMENT 'Mobile flag'
) COMMENT 'User activity log - LEGACY duplicate of silver.user_events_cleaned';

-- 32. 중복 시나리오 2: silver.orders_cleaned와 유사 (95% 동일, 컬럼명 다름)
CREATE TABLE IF NOT EXISTS hurcy.legacy.order_transaction_history (
    ord_id STRING COMMENT 'Order identifier',
    cust_id STRING COMMENT 'Customer identifier',
    ord_dt DATE COMMENT 'Order date',
    ord_year INT COMMENT 'Order year',
    ord_month INT COMMENT 'Order month',
    tot_amt DECIMAL(18,2) COMMENT 'Total amount',
    tot_amt_usd DECIMAL(18,2) COMMENT 'Amount in USD',
    ord_status STRING COMMENT 'Order status',
    is_complete BOOLEAN COMMENT 'Completion flag'
) COMMENT 'Order transaction history - LEGACY duplicate of silver.orders_cleaned';

-- 33. 중복 시나리오 3: silver.customer_profiles와 유사 (90% 동일)
CREATE TABLE IF NOT EXISTS hurcy.legacy.customer_master_old (
    customer_id STRING COMMENT 'Customer ID',
    email STRING COMMENT 'Email address',
    full_name STRING COMMENT 'Full name',
    age INT COMMENT 'Age',
    age_group STRING COMMENT 'Age group',
    gender STRING COMMENT 'Gender',
    registration_date DATE COMMENT 'Registration date',
    tenure_days INT COMMENT 'Tenure in days',
    country STRING COMMENT 'Country'
) COMMENT 'Old customer master - LEGACY duplicate of silver.customer_profiles';

-- 34. 중복 시나리오 4: gold.daily_sales_summary와 유사 (동일 소스에서 파생)
CREATE TABLE IF NOT EXISTS hurcy.legacy.daily_revenue_report (
    report_date DATE COMMENT 'Report date',
    order_count INT COMMENT 'Number of orders',
    gross_revenue DECIMAL(18,2) COMMENT 'Gross revenue',
    average_order_amount DECIMAL(18,2) COMMENT 'Average order amount',
    customer_count INT COMMENT 'Customer count',
    successful_orders INT COMMENT 'Successful orders',
    failed_orders INT COMMENT 'Failed orders'
) COMMENT 'Daily revenue report - LEGACY duplicate of gold.daily_sales_summary';

-- 35. 중복 시나리오 5: silver.products_enriched와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.product_catalog_v1 (
    prod_id STRING COMMENT 'Product ID',
    prod_name STRING COMMENT 'Product name',
    cat_id STRING COMMENT 'Category ID',
    cat_name STRING COMMENT 'Category name',
    subcat_id STRING COMMENT 'Subcategory ID',
    brand_name STRING COMMENT 'Brand',
    retail_price DECIMAL(18,2) COMMENT 'Retail price',
    wholesale_cost DECIMAL(18,2) COMMENT 'Wholesale cost',
    profit_margin DECIMAL(5,2) COMMENT 'Profit margin',
    active_flag BOOLEAN COMMENT 'Is active'
) COMMENT 'Product catalog v1 - LEGACY duplicate of silver.products_enriched';

-- 36. 중복 시나리오 6: silver.payment_details와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.payment_transaction_log (
    pay_id STRING COMMENT 'Payment ID',
    ord_id STRING COMMENT 'Order ID',
    cust_id STRING COMMENT 'Customer ID',
    pay_method STRING COMMENT 'Payment method',
    pay_type STRING COMMENT 'Payment type',
    pay_amount DECIMAL(18,2) COMMENT 'Payment amount',
    pay_amount_usd DECIMAL(18,2) COMMENT 'Amount in USD',
    pay_status STRING COMMENT 'Payment status',
    success_flag BOOLEAN COMMENT 'Success flag',
    process_date DATE COMMENT 'Process date'
) COMMENT 'Payment transaction log - LEGACY duplicate of silver.payment_details';

-- 37. 중복 시나리오 7: gold.customer_segments와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.customer_segmentation_old (
    cust_id STRING COMMENT 'Customer ID',
    segment_name STRING COMMENT 'Segment name',
    rfm STRING COMMENT 'RFM score',
    order_count INT COMMENT 'Order count',
    lifetime_value DECIMAL(18,2) COMMENT 'Lifetime value',
    aov DECIMAL(18,2) COMMENT 'Average order value',
    last_purchase_date DATE COMMENT 'Last purchase',
    days_as_customer INT COMMENT 'Days as customer'
) COMMENT 'Customer segmentation old - LEGACY duplicate of gold.customer_segments';

-- 38. 중복 시나리오 8: silver.review_analysis와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.product_review_stats (
    rev_id STRING COMMENT 'Review ID',
    prod_id STRING COMMENT 'Product ID',
    cust_id STRING COMMENT 'Customer ID',
    star_rating INT COMMENT 'Star rating',
    sentiment DECIMAL(5,2) COMMENT 'Sentiment score',
    sentiment_category STRING COMMENT 'Sentiment category',
    text_length INT COMMENT 'Text length',
    review_dt DATE COMMENT 'Review date',
    verified_flag BOOLEAN COMMENT 'Verified flag'
) COMMENT 'Product review stats - LEGACY duplicate of silver.review_analysis';

-- 39. 중복 시나리오 9: gold.product_performance와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.item_sales_analytics (
    item_id STRING COMMENT 'Item ID',
    item_name STRING COMMENT 'Item name',
    category STRING COMMENT 'Category',
    units_sold INT COMMENT 'Units sold',
    sales_amount DECIMAL(18,2) COMMENT 'Sales amount',
    avg_customer_rating DECIMAL(3,2) COMMENT 'Average rating',
    num_reviews INT COMMENT 'Number of reviews',
    return_percentage DECIMAL(5,2) COMMENT 'Return percentage'
) COMMENT 'Item sales analytics - LEGACY duplicate of gold.product_performance';

-- 40. 중복 시나리오 10: silver.shipment_tracking와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.delivery_tracking_old (
    ship_id STRING COMMENT 'Shipment ID',
    order_id STRING COMMENT 'Order ID',
    buyer_id STRING COMMENT 'Buyer ID',
    shipping_company STRING COMMENT 'Shipping company',
    delivery_status STRING COMMENT 'Delivery status',
    ship_date DATE COMMENT 'Ship date',
    arrive_date DATE COMMENT 'Arrival date',
    transit_days INT COMMENT 'Transit days',
    on_time_flag BOOLEAN COMMENT 'On time flag'
) COMMENT 'Delivery tracking old - LEGACY duplicate of silver.shipment_tracking';

-- 41. 중복 시나리오 11: silver.session_analysis와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.web_session_metrics (
    sess_id STRING COMMENT 'Session ID',
    visitor_id STRING COMMENT 'Visitor ID',
    visit_date DATE COMMENT 'Visit date',
    duration_seconds INT COMMENT 'Duration in seconds',
    pages_viewed INT COMMENT 'Pages viewed',
    total_events INT COMMENT 'Total events',
    device STRING COMMENT 'Device',
    geo_country STRING COMMENT 'Country',
    bounced BOOLEAN COMMENT 'Bounced'
) COMMENT 'Web session metrics - LEGACY duplicate of silver.session_analysis';

-- 42. 중복 시나리오 12: silver.inventory_status와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.stock_levels (
    sku_id STRING COMMENT 'SKU ID',
    location_id STRING COMMENT 'Location ID',
    qty_available INT COMMENT 'Available quantity',
    qty_reserved INT COMMENT 'Reserved quantity',
    qty_total INT COMMENT 'Total quantity',
    inventory_status STRING COMMENT 'Inventory status',
    updated_date DATE COMMENT 'Updated date'
) COMMENT 'Stock levels - LEGACY duplicate of silver.inventory_status';

-- 43. 중복 시나리오 13: gold.monthly_kpi와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.monthly_business_metrics (
    period STRING COMMENT 'Period (YYYY-MM)',
    revenue DECIMAL(18,2) COMMENT 'Revenue',
    growth_rate DECIMAL(5,2) COMMENT 'Growth rate',
    new_customer_count INT COMMENT 'New customers',
    repeat_customer_count INT COMMENT 'Repeat customers',
    basket_size DECIMAL(18,2) COMMENT 'Basket size',
    cvr DECIMAL(5,2) COMMENT 'Conversion rate'
) COMMENT 'Monthly business metrics - LEGACY duplicate of gold.monthly_kpi';

-- 44. 중복 시나리오 14: gold.channel_performance와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.marketing_channel_stats (
    channel_name STRING COMMENT 'Channel name',
    stat_date DATE COMMENT 'Stat date',
    session_count INT COMMENT 'Session count',
    visitor_count INT COMMENT 'Visitor count',
    order_count INT COMMENT 'Order count',
    channel_revenue DECIMAL(18,2) COMMENT 'Channel revenue',
    cvr_pct DECIMAL(5,2) COMMENT 'CVR percentage',
    avg_duration INT COMMENT 'Average duration'
) COMMENT 'Marketing channel stats - LEGACY duplicate of gold.channel_performance';

-- 45. 중복 시나리오 15: silver.order_items와 유사
CREATE TABLE IF NOT EXISTS hurcy.legacy.order_line_details (
    line_id STRING COMMENT 'Line ID',
    order_num STRING COMMENT 'Order number',
    item_id STRING COMMENT 'Item ID',
    item_name STRING COMMENT 'Item name',
    qty INT COMMENT 'Quantity',
    price_each DECIMAL(18,2) COMMENT 'Price each',
    line_total DECIMAL(18,2) COMMENT 'Line total',
    discount DECIMAL(18,2) COMMENT 'Discount'
) COMMENT 'Order line details - LEGACY duplicate of silver.order_items';

-- ============================================================================
-- MART 스키마 - 추가 테이블 (5개) - 더 많은 중복 시나리오
-- ============================================================================

-- 46. 중복 시나리오 16: gold.daily_sales_summary + legacy.daily_revenue_report와 모두 유사
CREATE TABLE IF NOT EXISTS hurcy.mart.sales_dashboard_daily (
    date_key DATE COMMENT 'Date key',
    num_orders INT COMMENT 'Number of orders',
    total_sales DECIMAL(18,2) COMMENT 'Total sales',
    mean_order_value DECIMAL(18,2) COMMENT 'Mean order value',
    distinct_customers INT COMMENT 'Distinct customers',
    completed_count INT COMMENT 'Completed count',
    cancelled_count INT COMMENT 'Cancelled count'
) COMMENT 'Sales dashboard daily - Similar to gold.daily_sales_summary';

-- 47. 중복 시나리오 17: gold.customer_segments와 유사하지만 일부 다름
CREATE TABLE IF NOT EXISTS hurcy.mart.customer_value_tiers (
    customer_key STRING COMMENT 'Customer key',
    value_tier STRING COMMENT 'Value tier',
    rfm_combined STRING COMMENT 'RFM combined',
    order_frequency INT COMMENT 'Order frequency',
    monetary_value DECIMAL(18,2) COMMENT 'Monetary value',
    avg_basket DECIMAL(18,2) COMMENT 'Average basket',
    recency_date DATE COMMENT 'Recency date',
    customer_age_days INT COMMENT 'Customer age in days',
    churn_risk STRING COMMENT 'Churn risk'
) COMMENT 'Customer value tiers - Similar to gold.customer_segments';

-- 48. 신규 테이블 (중복 아님): 상품 추천
CREATE TABLE IF NOT EXISTS hurcy.mart.product_recommendations (
    user_id STRING COMMENT 'User ID',
    recommended_product_id STRING COMMENT 'Recommended product ID',
    recommendation_score DECIMAL(5,4) COMMENT 'Score',
    recommendation_type STRING COMMENT 'Type',
    generated_at TIMESTAMP COMMENT 'Generated at'
) COMMENT 'Product recommendations';

-- 49. 신규 테이블 (중복 아님): AB 테스트 결과
CREATE TABLE IF NOT EXISTS hurcy.mart.ab_test_results (
    test_id STRING COMMENT 'Test ID',
    test_name STRING COMMENT 'Test name',
    variant STRING COMMENT 'Variant',
    metric_name STRING COMMENT 'Metric name',
    metric_value DECIMAL(18,4) COMMENT 'Metric value',
    sample_size INT COMMENT 'Sample size',
    p_value DECIMAL(5,4) COMMENT 'P-value',
    is_significant BOOLEAN COMMENT 'Is significant'
) COMMENT 'AB test results';

-- 50. 신규 테이블 (중복 아님): 예측 결과
CREATE TABLE IF NOT EXISTS hurcy.mart.demand_forecast (
    product_id STRING COMMENT 'Product ID',
    forecast_date DATE COMMENT 'Forecast date',
    predicted_demand INT COMMENT 'Predicted demand',
    lower_bound INT COMMENT 'Lower bound',
    upper_bound INT COMMENT 'Upper bound',
    model_version STRING COMMENT 'Model version',
    created_at TIMESTAMP COMMENT 'Created at'
) COMMENT 'Demand forecast';

