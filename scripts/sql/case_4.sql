/*
================================================================================
  New Case 4 & 4-1 통합 수익 분석 쿼리 (상세 주석 버전)
================================================================================
  작성 목적: 
    구매대행(WeBuy) 서비스의 묶음 패키지(CONSOLE/REPACK)와 
    그에 속한 개별 하위 패키지(INNER)의 수익을 정확하게 계산합니다.

  핵심 로직 개요:
    1. 중복 계산 방지: 상위 패키지와 하위 패키지가 1:N 관계이므로, 
       단순 조인 시 상위 패키지의 배송비/수수료가 N배로 부풀려지는 문제를 차단합니다.
    2. 데이터 소스 분리:
       - 상위 행(Case 4): 하위 상품 실적 합계 + 상위 전용 수수료(fee_new) + 배송 마크업
       - 하위 행(Case 4-1): 각 상품의 개별 실적 + 각 상품의 개별 수수료(fee_item_new)
    3. 수익 공식:
       - 상품 이익 = (상품가 + 핸들링피 + 부가세환급 + DK수수료) - (할인액 + 쿠폰액 + 업체수수료)
       - 총 이익 = 상품 이익 - PG수수료 + 창고 수수료 + 배송 마크업
================================================================================
*/

WITH verified_parent AS (
    -- ============================================================================
    -- 1. 유효한 상위 패키지 식별
    -- ============================================================================
    SELECT
        spn.package_id,
        spn.package_type,
        spn.trans_at_utc,      -- 패키지 생성/결제 시간
        spn.ship_at_kst,       -- 실제 출고일 (KST 기준)
        spn.package_merchant,
        spn.order_type,
        spn.customer_id,
        spn.country_code,
        spn.carrier,
        spn.carrier_service,
        spn.package_weight,
        spn.dimension_weight,
        spn.package_count,     -- 하위 패키지 총 개수
        spn.reference_type,
        spn.track_number,
        spn.shipping_fee,
        sc_u.customer_cost,    -- 고객이 지불한 배송비
        sc_u.original_cost,    -- 가입/운송 원가
        sc_u.fuel_surcharge_cost,
        sc_u.marked_up_cost    -- 우리가 남긴 배송 마진 (Mark-up)
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    LEFT JOIN (
        -- 배송비 테이블 중복 제거
        SELECT *, ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
        FROM `da-project-472406.data_warehouse.shipping_cost`
    ) sc_u
        ON TRIM(spn.track_number) = TRIM(sc_u.tracking_number) AND sc_u.rn = 1
    WHERE spn.package_type IN ('CONSOLE', 'REPACK')
      AND spn.order_type NOT LIKE '%WE_SHIP%'
      -- [필터링] 하위 패키지 중 하나라도 상품구매 타입이 아닌 것이 섞여있으면 Case 4에서 제외
      AND NOT EXISTS (
          SELECT 1 
          FROM `da-project-472406.data_warehouse.shipment_package_new` child
          WHERE child.package_id = spn.package_id 
            AND child.package_type = 'INNER'
            AND (child.reference_type IS NULL OR child.reference_type NOT IN ('BUY_REQUEST', 'BUYFORME'))
      )
      -- 날짜 필터: 2025-01-01 이후 데이터 조회
      AND DATE(spn.trans_at_utc) > '2025-01-01'
),

child_goods_aggregated AS (
    -- ============================================================================
    -- 2. 하위 패키지 상품 실적 사전 합산
    -- ============================================================================
    SELECT 
        spn.package_id,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) END) AS total_goods_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) / COALESCE(c_u_t.usd_krw, 1450), 2) END) AS total_goods_usd,
        
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END) AS total_handling_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(wam.fee_handling_fee_usd, 0) END) AS total_handling_usd,
        
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) END) AS total_surtax_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END) AS total_surtax_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) END) AS total_business_fee_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END) AS total_business_fee_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) END) AS total_dk_fee_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END / COALESCE(c_u_t.usd_krw, 1450), 2) END) AS total_dk_fee_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END) AS total_disc_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(t.discount_value, 0) END) AS total_disc_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END) AS total_coupon_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(t.coupon_discount_value, 0) END) AS total_coupon_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END) AS total_domestic_shipping_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(wam.fee_domestic_shipping_price_usd, 0) END) AS total_domestic_shipping_usd,

        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0) END) AS total_pg_fee_krw,
        SUM(CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END) AS total_pg_fee_usd
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    LEFT JOIN `da-project-472406.data_warehouse.webuy_all_market` wam ON spn.reference_id = wam.request_id
    LEFT JOIN `da-project-472406.data_warehouse.transaction` t ON wam.request_id = t.reference_id AND t.pay_service IN ('NewWeBuy', 'BUY_REQUEST') AND t.transaction_status = 'CNF'
    LEFT JOIN (SELECT currency_date, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_t ON DATE(t.trans_at_utc) = c_u_t.currency_date
    WHERE spn.package_type = 'INNER'
    GROUP BY spn.package_id
)

-- ================================================================================
-- Case 4: 상위 패키지 합산 레벨 (묶음 전체의 이익을 한 행에 표시)
-- ================================================================================
SELECT 
    cust.suite_number,
    0 AS market_id,
    DATE(vp.ship_at_kst) AS ship_date_kst,
    NULL AS trans_at_utc_webuy,
    DATE(vp.trans_at_utc) AS trans_at_utc_package,
    vp.package_id,
    0 AS inner_package_id,
    '4' AS profit_case,
    CAST(NULL AS STRING) AS package_merchant,
    CAST(NULL AS STRING) AS package_merchant_clean,
    CAST(NULL AS STRING) AS product_name,
    CAST(NULL AS STRING) AS product_category,
    vp.order_type,
    vp.package_type,
    vp.country_code,
    b2b.name_en AS country_name,
    b2b.name_kr AS country_name_kr,
    vp.carrier,
    vp.carrier_service,
    vp.package_weight,
    vp.dimension_weight,
    vp.package_count,
    vp.reference_type,

    -- ============================================================================
    -- 매출 상세 (Revenue Details)
    -- ============================================================================

    -- 1. 상품 구매 매출
    -- goods_revenue_krw = total_goods_price_krw + domestic_shipping_price_krw + fee_handling_fee_krw + pg_fee_krw + surtax_krw + dk_fee_krw - discount_value_krw - coupon_discount_value_krw
    -- goods_revenue_usd = total_goods_price_usd + domestic_shipping_price_usd + fee_handling_fee_usd + pg_fee_usd + surtax_usd + dk_fee_usd - discount_value_usd - coupon_discount_value_usd
    ROUND(
        COALESCE(cga.total_goods_krw, 0) + COALESCE(cga.total_domestic_shipping_krw, 0) + COALESCE(cga.total_handling_krw, 0) + COALESCE(cga.total_pg_fee_krw, 0) + COALESCE(cga.total_surtax_krw, 0) + COALESCE(cga.total_dk_fee_krw, 0) - (COALESCE(cga.total_disc_krw, 0) + COALESCE(cga.total_coupon_krw, 0))
    , 0) AS goods_revenue_krw,
    ROUND(
        COALESCE(cga.total_goods_usd, 0) + COALESCE(cga.total_domestic_shipping_usd, 0) + COALESCE(cga.total_handling_usd, 0) + COALESCE(cga.total_pg_fee_usd, 0) + COALESCE(cga.total_surtax_usd, 0) + COALESCE(cga.total_dk_fee_usd, 0) - (COALESCE(cga.total_disc_usd, 0) + COALESCE(cga.total_coupon_usd, 0))
    , 2) AS goods_revenue_usd,
    
    -- 2. 창고 옵션 매출
    -- warehouse_revenue_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
    -- warehouse_revenue_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
    ROUND((COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450), 0) AS warehouse_revenue_krw,
    ROUND((COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0)), 2) AS warehouse_revenue_usd,
    
    -- 3. 배송비 매출
    -- shipping_revenue_krw = shipping_fee_usd × usd_krw_package
    -- shipping_revenue_usd = shipping_fee_usd
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_revenue_krw,
    COALESCE(spfn.shipping_fee, 0) AS shipping_revenue_usd,

    -- [핵심 지표] 총 수익
    -- profit_krw = goods_profit_krw + warehouse_profit_krw + shipping_profit_krw
    -- profit_usd = goods_profit_usd + warehouse_profit_usd + shipping_profit_usd
    ROUND(
        (COALESCE(cga.total_goods_krw, 0) + COALESCE(cga.total_domestic_shipping_krw, 0) + COALESCE(cga.total_handling_krw, 0) + COALESCE(cga.total_surtax_krw, 0) + COALESCE(cga.total_dk_fee_krw, 0) - (COALESCE(cga.total_disc_krw, 0) + COALESCE(cga.total_coupon_krw, 0)))
        - COALESCE(cga.total_domestic_shipping_krw, 0)  -- 국내배송비 차감 (실제 비용)
        - COALESCE(cga.total_business_fee_krw, 0)
        + (COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450)
        + vp.marked_up_cost,  -- 배송비 마크업 (NULL이면 profit도 NULL → 6월 이전 데이터 제외)
    0) AS profit_krw,

    ROUND(
        (COALESCE(cga.total_goods_usd, 0) + COALESCE(cga.total_domestic_shipping_usd, 0) + COALESCE(cga.total_handling_usd, 0) + COALESCE(cga.total_surtax_usd, 0) + COALESCE(cga.total_dk_fee_usd, 0) - (COALESCE(cga.total_disc_usd, 0) + COALESCE(cga.total_coupon_usd, 0)))
        - COALESCE(cga.total_domestic_shipping_usd, 0)  -- 국내배송비 차감 (실제 비용)
        - COALESCE(cga.total_business_fee_usd, 0)
        + (COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0))
        + (vp.marked_up_cost / COALESCE(curr.usd_krw, 1450)),
    2) AS profit_usd,

    -- 구매대행 수익
    -- goods_profit_krw = total_goods_price_krw + fee_handling_fee_krw + surtax_krw + dk_fee_krw - discount_value_krw - coupon_discount_value_krw - business_fee_krw
    -- goods_profit_usd = total_goods_price_usd + fee_handling_fee_usd + surtax_usd + dk_fee_usd - discount_value_usd - coupon_discount_value_usd - business_fee_usd
    ROUND(
        COALESCE(cga.total_goods_krw, 0) + COALESCE(cga.total_handling_krw, 0) + COALESCE(cga.total_surtax_krw, 0) + COALESCE(cga.total_dk_fee_krw, 0) - (COALESCE(cga.total_disc_krw, 0) + COALESCE(cga.total_coupon_krw, 0))
        - COALESCE(cga.total_business_fee_krw, 0)
    , 0) AS goods_profit_krw,
    ROUND(
        COALESCE(cga.total_goods_usd, 0) + COALESCE(cga.total_handling_usd, 0) + COALESCE(cga.total_surtax_usd, 0) + COALESCE(cga.total_dk_fee_usd, 0) - (COALESCE(cga.total_disc_usd, 0) + COALESCE(cga.total_coupon_usd, 0))
        - COALESCE(cga.total_business_fee_usd, 0)
    , 2) AS goods_profit_usd,

    -- 창고 수익
    -- warehouse_profit_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
    -- warehouse_profit_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
    ROUND((COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450), 0) AS warehouse_profit_krw,
    ROUND((COALESCE(spfn.storage_fee,0)+COALESCE(spfn.repack_fee,0)+COALESCE(spfn.request_photo_fee,0)+COALESCE(spfn.bubble_wrap_fee,0)+COALESCE(spfn.vacuum_repack_fee,0)+COALESCE(spfn.plasticbox_fee,0)+COALESCE(spfn.remove_papertube_fee,0)+COALESCE(spfn.inclusion_fee,0)+COALESCE(spfn.bfm_extra_fee,0)+COALESCE(spfn.receiving_fee,0)+COALESCE(spfn.package_extra_fee,0)), 2) AS warehouse_profit_usd,

    -- 배송 수익
    -- shipping_profit_krw = marked_up_cost_krw
    -- shipping_profit_usd = marked_up_cost_usd
    ROUND(vp.marked_up_cost, 0) AS shipping_profit_krw,
    ROUND(vp.marked_up_cost / COALESCE(curr.usd_krw, 1450), 2) AS shipping_profit_usd,

    0 AS fee_unit_price_krw,
    0 AS fee_unit_price_usd,
    0 AS quotation_quantity,
    COALESCE(cga.total_goods_krw, 0) AS total_goods_price_krw,
    COALESCE(cga.total_goods_usd, 0) AS total_goods_price_usd,
    COALESCE(cga.total_handling_krw, 0) AS fee_handling_fee_krw,
    COALESCE(cga.total_handling_usd, 0) AS fee_handling_fee_usd,
    COALESCE(cga.total_domestic_shipping_krw, 0) AS domestic_shipping_price_krw,
    COALESCE(cga.total_domestic_shipping_usd, 0) AS domestic_shipping_price_usd,
    COALESCE(cga.total_surtax_krw, 0) AS surtax_krw,
    COALESCE(cga.total_surtax_usd, 0) AS surtax_usd,
    COALESCE(cga.total_business_fee_krw, 0) AS business_fee_krw,
    COALESCE(cga.total_business_fee_usd, 0) AS business_fee_usd,
    COALESCE(cga.total_pg_fee_krw, 0) AS pg_fee_krw,
    COALESCE(cga.total_pg_fee_usd, 0) AS pg_fee_usd,
    COALESCE(cga.total_dk_fee_krw, 0) AS dk_fee_krw,
    COALESCE(cga.total_dk_fee_usd, 0) AS dk_fee_usd,
    COALESCE(cga.total_disc_krw, 0) AS discount_value_krw,
    COALESCE(cga.total_disc_usd, 0) AS discount_value_usd,
    COALESCE(cga.total_coupon_krw, 0) AS coupon_discount_value_krw,
    COALESCE(cga.total_coupon_usd, 0) AS coupon_discount_value_usd,

    -- 창고/서비스 수수료
    ROUND(COALESCE(spfn.storage_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS storage_fee_krw,
    COALESCE(spfn.storage_fee, 0) AS storage_fee_usd,
    ROUND(COALESCE(spfn.request_photo_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS request_photo_fee_krw,
    COALESCE(spfn.request_photo_fee, 0) AS request_photo_fee_usd,
    ROUND(COALESCE(spfn.repack_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS repack_fee_krw,
    COALESCE(spfn.repack_fee, 0) AS repack_fee_usd,
    ROUND(COALESCE(spfn.bubble_wrap_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
    COALESCE(spfn.bubble_wrap_fee, 0) AS bubble_wrap_fee_usd,
    ROUND(COALESCE(spfn.vacuum_repack_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
    COALESCE(spfn.vacuum_repack_fee, 0) AS vacuum_repack_fee_usd,
    ROUND(COALESCE(spfn.plasticbox_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS plasticbox_fee_krw,
    COALESCE(spfn.plasticbox_fee, 0) AS plasticbox_fee_usd,
    ROUND(COALESCE(spfn.remove_papertube_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
    COALESCE(spfn.remove_papertube_fee, 0) AS remove_papertube_fee_usd,
    ROUND(COALESCE(spfn.inclusion_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS inclusion_fee_krw,
    COALESCE(spfn.inclusion_fee, 0) AS inclusion_fee_usd,
    ROUND(COALESCE(spfn.bfm_extra_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS bfm_extra_fee_krw,
    COALESCE(spfn.bfm_extra_fee, 0) AS bfm_extra_fee_usd,
    
    -- 입고 수수료
    ROUND(COALESCE(spfn.receiving_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS receiving_fee_krw,
    COALESCE(spfn.receiving_fee, 0) AS receiving_fee_usd,
    
    -- 배송대행 추가 비용
    ROUND(COALESCE(spfn.package_extra_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS package_extra_fee_krw,
    COALESCE(spfn.package_extra_fee, 0) AS package_extra_fee_usd,

    -- 배송비
    spfn.shipping_fee AS shipping_fee_usd,
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_fee_krw,

    ROUND(vp.customer_cost, 0) AS customer_cost_krw,
    ROUND(vp.customer_cost / COALESCE(curr.usd_krw, 1450), 2) AS customer_cost_usd,
    ROUND(vp.original_cost, 0) AS original_cost_krw,
    ROUND(vp.original_cost / COALESCE(curr.usd_krw, 1450), 2) AS original_cost_usd,
    ROUND(vp.fuel_surcharge_cost, 0) AS fuel_surcharge_cost_krw,
    ROUND(vp.fuel_surcharge_cost / COALESCE(curr.usd_krw, 1450), 2) AS fuel_surcharge_cost_usd,
    ROUND(vp.marked_up_cost, 0) AS marked_up_cost_krw,
    ROUND(vp.marked_up_cost / COALESCE(curr.usd_krw, 1450), 2) AS marked_up_cost_usd,
    
    1450 AS origin_usd_krw_webuy,
    1450 AS usd_krw_webuy,
    COALESCE(curr.origin_usd_krw, 1450) AS origin_usd_krw_package,
    COALESCE(curr.usd_krw, 1450) AS usd_krw_package
FROM verified_parent vp
LEFT JOIN child_goods_aggregated cga ON vp.package_id = cga.package_id
LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_new` spfn ON vp.package_id = spfn.package_id
LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON vp.customer_id = cust.customer_id
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON vp.country_code = b2b.country_code
LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) curr ON DATE(vp.trans_at_utc) = curr.currency_date

UNION ALL

-- ================================================================================
-- Case 4-1: 개별 자식(INNER) 패키지 레벨 (상세 내역 표시)
-- ================================================================================
SELECT 
    cust.suite_number,
    wam.market_id,
    DATE(spn.ship_at_kst) AS ship_date_kst,
    DATE(t.trans_at_utc) AS trans_at_utc_webuy,
    DATE(spn.trans_at_utc) AS trans_at_utc_package,
    spn.package_id,
    spn.inner_package_id,
    '4_1' AS profit_case,
    spn.package_merchant,
    -- 패키지 판매자 정보 (정제)
    CASE 
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30) 
        THEN brud.parsed_domain 
      ELSE spn.package_merchant 
    END AS package_merchant_clean,
    -- 상품명 (customs_item_description 우선, 없으면 item_description)
    COALESCE(wam.customs_item_description, wam.item_description) AS product_name,
    -- 상품 카테고리
    wam.customs_category AS product_category,
    spn.order_type,
    spn.package_type,
    vp.country_code,
    b2b.name_en AS country_name,
    b2b.name_kr AS country_name_kr,
    vp.carrier,
    vp.carrier_service,
    spn.package_weight,
    spn.dimension_weight,
    spn.package_count,
    spn.reference_type,

    -- ============================================================================
    -- 매출 상세 (Revenue Details)
    -- ============================================================================
    
    -- 1. 상품 구매 매출
    -- goods_revenue_krw = total_goods_price_krw + (domestic_shipping_price_usd × usd_krw_webuy) + (fee_handling_fee_usd × usd_krw_webuy) + pg_fee_krw + surtax_krw + dk_fee_krw - (discount_value_usd × usd_krw_webuy) - (coupon_discount_value_usd × usd_krw_webuy)
    -- goods_revenue_usd = total_goods_price_usd + domestic_shipping_price_usd + fee_handling_fee_usd + pg_fee_usd + surtax_usd + dk_fee_usd - discount_value_usd - coupon_discount_value_usd
    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) + ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) + ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) - ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) - ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0))
        END, 0) AS goods_revenue_krw,
    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450)) + COALESCE(wam.fee_domestic_shipping_price_usd, 0) + COALESCE(wam.fee_handling_fee_usd, 0) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) - COALESCE(t.discount_value, 0) - COALESCE(t.coupon_discount_value, 0))
        END, 2) AS goods_revenue_usd,
    
    -- 2. 창고 옵션 매출
    -- warehouse_revenue_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
    -- warehouse_revenue_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
    ROUND((COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450), 0) AS warehouse_revenue_krw,
    ROUND((COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0)), 2) AS warehouse_revenue_usd,
    
    -- 3. 배송비 매출
    -- shipping_revenue_krw = shipping_fee_usd × usd_krw_package
    -- shipping_revenue_usd = shipping_fee_usd
    ROUND(COALESCE(spfin.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_revenue_krw,
    COALESCE(spfin.shipping_fee, 0) AS shipping_revenue_usd,

    -- [핵심 지표] 총 수익
    -- profit_krw = goods_profit_krw + warehouse_profit_krw
    -- profit_usd = goods_profit_usd + warehouse_profit_usd
    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE
            ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) + ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) + ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) - ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) - ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0))
            - ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0)  -- 국내배송비 차감 (실제 비용)
            - ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0)
            + (COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450)
        END, 0) AS profit_krw,

    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE
            ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450)) + COALESCE(wam.fee_domestic_shipping_price_usd, 0) + COALESCE(wam.fee_handling_fee_usd, 0) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) - COALESCE(t.discount_value, 0) - COALESCE(t.coupon_discount_value, 0))
            - COALESCE(wam.fee_domestic_shipping_price_usd, 0)  -- 국내배송비 차감 (실제 비용)
            - (CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END)
            + (COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0))
        END, 2) AS profit_usd,

    -- 구매대행 수익
    -- goods_profit_krw = total_goods_price_krw + fee_handling_fee_krw + surtax_krw + dk_fee_krw - discount_value_krw - coupon_discount_value_krw - business_fee_krw
    -- goods_profit_usd = total_goods_price_usd + fee_handling_fee_usd + surtax_usd + dk_fee_usd - discount_value_usd - coupon_discount_value_usd - business_fee_usd
    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE
            ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) + ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) - ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) - ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0))
            - ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0)
        END, 0) AS goods_profit_krw,
    ROUND(
        CASE WHEN wam.last_status = 'CANCELED' THEN 0
        ELSE
            ((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450)) + COALESCE(wam.fee_handling_fee_usd, 0) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END) - COALESCE(t.discount_value, 0) - COALESCE(t.coupon_discount_value, 0))
            - (CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END)
        END, 2) AS goods_profit_usd,

    -- 창고 수익
    -- warehouse_profit_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
    -- warehouse_profit_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
    ROUND((COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0)) * COALESCE(curr.usd_krw, 1450), 0) AS warehouse_profit_krw,
    ROUND((COALESCE(spfin.storage_fee,0)+COALESCE(spfin.repack_fee,0)+COALESCE(spfin.request_photo_fee,0)+COALESCE(spfin.bubble_wrap_fee,0)+COALESCE(spfin.vacuum_repack_fee,0)+COALESCE(spfin.plasticbox_fee,0)+COALESCE(spfin.remove_papertube_fee,0)+COALESCE(spfin.inclusion_fee,0)+COALESCE(spfin.bfm_extra_fee,0)+COALESCE(spfin.receiving_fee,0)+COALESCE(spfin.package_extra_fee,0)), 2) AS warehouse_profit_usd,

    -- 배송 수익 (하위 패키지는 배송 마크업 없음)
    -- shipping_profit_krw = NULL
    -- shipping_profit_usd = NULL
    CAST(NULL AS FLOAT64) AS shipping_profit_krw,
    CAST(NULL AS FLOAT64) AS shipping_profit_usd,

    COALESCE(wam.fee_unit_price_krw, 0) AS fee_unit_price_krw,
    COALESCE(wam.fee_unit_price_usd, 0) AS fee_unit_price_usd,
    COALESCE(wam.quotation_quantity, 0) AS quotation_quantity,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) END AS total_goods_price_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) / COALESCE(c_u_t.usd_krw, 1450), 2) END AS total_goods_price_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS fee_handling_fee_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(wam.fee_handling_fee_usd, 0) END AS fee_handling_fee_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS domestic_shipping_price_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(wam.fee_domestic_shipping_price_usd, 0) END AS domestic_shipping_price_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) END AS surtax_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END AS surtax_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) END AS business_fee_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id = 19 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-08' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END AS business_fee_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0) END AS pg_fee_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END AS pg_fee_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) END AS dk_fee_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) END AS dk_fee_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS discount_value_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(t.discount_value, 0) END AS discount_value_usd,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS coupon_discount_value_krw,
    CASE WHEN wam.last_status = 'CANCELED' THEN 0 ELSE COALESCE(t.coupon_discount_value, 0) END AS coupon_discount_value_usd,
    
    -- 서비스 수수료
    ROUND(COALESCE(spfin.storage_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS storage_fee_krw,
    COALESCE(spfin.storage_fee, 0) AS storage_fee_usd,
    ROUND(COALESCE(spfin.request_photo_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS request_photo_fee_krw,
    COALESCE(spfin.request_photo_fee, 0) AS request_photo_fee_usd,
    ROUND(COALESCE(spfin.repack_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS repack_fee_krw,
    COALESCE(spfin.repack_fee, 0) AS repack_fee_usd,
    ROUND(COALESCE(spfin.bubble_wrap_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
    COALESCE(spfin.bubble_wrap_fee, 0) AS bubble_wrap_fee_usd,
    ROUND(COALESCE(spfin.vacuum_repack_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
    COALESCE(spfin.vacuum_repack_fee, 0) AS vacuum_repack_fee_usd,
    ROUND(COALESCE(spfin.plasticbox_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS plasticbox_fee_krw,
    COALESCE(spfin.plasticbox_fee, 0) AS plasticbox_fee_usd,
    ROUND(COALESCE(spfin.remove_papertube_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
    COALESCE(spfin.remove_papertube_fee, 0) AS remove_papertube_fee_usd,
    ROUND(COALESCE(spfin.inclusion_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS inclusion_fee_krw,
    COALESCE(spfin.inclusion_fee, 0) AS inclusion_fee_usd,
    ROUND(COALESCE(spfin.bfm_extra_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS bfm_extra_fee_krw,
    COALESCE(spfin.bfm_extra_fee, 0) AS bfm_extra_fee_usd,
    
    -- 입고 수수료
    ROUND(COALESCE(spfin.receiving_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS receiving_fee_krw,
    COALESCE(spfin.receiving_fee, 0) AS receiving_fee_usd,
    
    -- 배송대행 추가 비용
    ROUND(COALESCE(spfin.package_extra_fee,0) * COALESCE(curr.usd_krw, 1450), 0) AS package_extra_fee_krw,
    COALESCE(spfin.package_extra_fee, 0) AS package_extra_fee_usd,

    -- 배송비
    spfin.shipping_fee AS shipping_fee_usd,
    ROUND(COALESCE(spfin.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_fee_krw,

    CAST(NULL AS FLOAT64) AS customer_cost_krw,
    CAST(NULL AS FLOAT64) AS customer_cost_usd,
    CAST(NULL AS FLOAT64) AS original_cost_krw,
    CAST(NULL AS FLOAT64) AS original_cost_usd,
    CAST(NULL AS FLOAT64) AS fuel_surcharge_cost_krw,
    CAST(NULL AS FLOAT64) AS fuel_surcharge_cost_usd,
    CAST(NULL AS FLOAT64) AS marked_up_cost_krw,
    CAST(NULL AS FLOAT64) AS marked_up_cost_usd,

    COALESCE(c_u_t.origin_usd_krw, 1450) AS origin_usd_krw_webuy,
    COALESCE(c_u_t.usd_krw, 1450) AS usd_krw_webuy,
    COALESCE(curr.origin_usd_krw, 1450) AS origin_usd_krw_package,
    COALESCE(curr.usd_krw, 1450) AS usd_krw_package
FROM `da-project-472406.data_warehouse.shipment_package_new` spn
INNER JOIN verified_parent vp ON spn.package_id = vp.package_id
LEFT JOIN `da-project-472406.data_warehouse.webuy_all_market` wam ON spn.reference_id = wam.request_id
LEFT JOIN `da-project-472406.data_warehouse.transaction` t ON wam.request_id = t.reference_id AND t.pay_service IN ('NewWeBuy', 'BUY_REQUEST') AND t.transaction_status = 'CNF'
LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_item_new` spfin ON spn.inner_package_id = spfin.inner_package_id
LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON spn.customer_id = cust.customer_id
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON vp.country_code = b2b.country_code
LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) curr ON DATE(spn.trans_at_utc) = curr.currency_date
LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_t ON DATE(t.trans_at_utc) = c_u_t.currency_date
LEFT JOIN `da-project-472406.data_warehouse.buy_request_url_domain` brud ON wam.request_id = brud.buy_request_id
WHERE spn.package_type = 'INNER'

ORDER BY package_id ASC, profit_case ASC;
