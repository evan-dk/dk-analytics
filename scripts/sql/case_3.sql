/*
================================================================================
  New Case 3 수익 분석 쿼리 (통합 버전)
================================================================================
  목적: 배송 대행 서비스의 CONSOLE/REPACK 패키지에 대한 통합 수익 계산
  
  구성:
  - Case 3: 상위 패키지의 수익 (shipment_package_fee_new 기준 + 배송 마크업)
  - Case 3-1: 하위 패키지(INNER)별 수익 (shipment_package_fee_item_new 기준)
  
  필터 조건:
  - 상위 패키지 package_type: CONSOLE 또는 REPACK
  - 하위 패키지(INNER)의 reference_type: 전부 ASN
================================================================================
*/

WITH verified_parent AS (
    -- 상위 패키지(REPACK, CONSOLE) 식별 및 공통 정보 추출
    SELECT DISTINCT
        spn.package_id,
        spn.package_type,
        spn.trans_at_utc,
        spn.ship_at_kst,
        spn.package_merchant,
        spn.order_type,
        spn.customer_id,
        spn.country_code,
        spn.carrier,
        spn.carrier_service,
        spn.package_count,
        spn.package_weight,
        spn.dimension_weight,
        spn.reference_type,
        spn.track_number,
        spn.shipping_fee
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    WHERE spn.package_type IN ('REPACK', 'CONSOLE')
      -- [조건] 모든 하위 패키지가 ASN이어야 함
      AND NOT EXISTS (
          SELECT 1
          FROM `da-project-472406.data_warehouse.shipment_package_new` spn_inner
          WHERE spn_inner.package_id = spn.package_id
            AND spn_inner.package_type = 'INNER'
            AND spn_inner.reference_type != 'ASN'
      )
      -- 날짜 필터: 2025-01-01 이후 데이터 조회
      AND DATE(spn.trans_at_utc) > '2025-01-01'
),

child_packages AS (
    -- 하위 패키지(INNER) 개별 정보
    SELECT 
        spn.inner_package_id,
        spn.package_id,
        spn.package_type,
        spn.reference_type,
        spn.package_weight,
        spn.dimension_weight,
        spn.package_count,
        vp.trans_at_utc,
        vp.ship_at_kst,
        vp.package_merchant,
        spn.order_type,
        vp.customer_id,
        vp.country_code,
        vp.carrier,
        vp.carrier_service
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    JOIN verified_parent vp ON spn.package_id = vp.package_id
    WHERE spn.package_type = 'INNER'
      AND spn.inner_package_id != 0
      AND spn.reference_type = 'ASN'
)

-- ================================================================================
-- Case 3: 상위 패키지 수익
-- ================================================================================
SELECT 
    cust.suite_number,
    DATE(vp.ship_at_kst) AS ship_date_kst, 
    DATE(vp.trans_at_utc) AS trans_at_utc_package, 
    vp.package_id, 
    0 AS inner_package_id,
    '3' AS profit_case,
    vp.package_merchant,
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

    -- 1. 상품 구매 매출 (Case 3은 ASN 배송대행이므로 상품 매출 0)
    0 AS goods_revenue_krw,
    0 AS goods_revenue_usd,
    
    -- 2. 창고 옵션 매출 (11가지 상위 창고 서비스 수수료 합계)
    ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u.usd_krw, 1450), 0) AS warehouse_revenue_krw,
    ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)), 2) AS warehouse_revenue_usd,
    
    -- 3. 배송비 매출 (shipment_package_fee_new.shipping_fee 컬럼 기준)
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS shipping_revenue_krw,
    COALESCE(spfn.shipping_fee, 0) AS shipping_revenue_usd,

    -- [핵심 지표] 총 수익 (수수료 수익 + 배송 마진)
    ROUND(
      (
        COALESCE(spfn.storage_fee, 0) + 
        COALESCE(spfn.request_photo_fee, 0) + 
        COALESCE(spfn.repack_fee, 0) + 
        COALESCE(spfn.bubble_wrap_fee, 0) + 
        COALESCE(spfn.vacuum_repack_fee, 0) + 
        COALESCE(spfn.plasticbox_fee, 0) + 
        COALESCE(spfn.remove_papertube_fee, 0) + 
        COALESCE(spfn.inclusion_fee, 0) +
        COALESCE(spfn.bfm_extra_fee, 0) +
        COALESCE(spfn.receiving_fee, 0) +
        COALESCE(spfn.package_extra_fee, 0)
      ) * COALESCE(c_u.usd_krw, 1450)
      + sc_u.marked_up_cost,  -- 배송비 마크업 (NULL이면 profit도 NULL → 6월 이전 데이터 제외)
    0) AS profit_krw,

    ROUND(
      (
        COALESCE(spfn.storage_fee, 0) + 
        COALESCE(spfn.request_photo_fee, 0) + 
        COALESCE(spfn.repack_fee, 0) + 
        COALESCE(spfn.bubble_wrap_fee, 0) + 
        COALESCE(spfn.vacuum_repack_fee, 0) + 
        COALESCE(spfn.plasticbox_fee, 0) + 
        COALESCE(spfn.remove_papertube_fee, 0) + 
        COALESCE(spfn.inclusion_fee, 0) +
        COALESCE(spfn.bfm_extra_fee, 0) +
        COALESCE(spfn.receiving_fee, 0) +
        COALESCE(spfn.package_extra_fee, 0)
      )
      + (sc_u.marked_up_cost / COALESCE(c_u.usd_krw, 1450)),
    2) AS profit_usd,

    -- 구매 수익 (Case 3은 ASN 배송대행이므로 0)
    0 AS goods_profit_krw,
    0 AS goods_profit_usd,

    -- 창고 수익 (11가지 창고 수수료 합계 = 수익)
    ROUND(
      (COALESCE(spfn.storage_fee, 0) +
       COALESCE(spfn.request_photo_fee, 0) +
       COALESCE(spfn.repack_fee, 0) +
       COALESCE(spfn.bubble_wrap_fee, 0) +
       COALESCE(spfn.vacuum_repack_fee, 0) +
       COALESCE(spfn.plasticbox_fee, 0) +
       COALESCE(spfn.remove_papertube_fee, 0) +
       COALESCE(spfn.inclusion_fee, 0) +
       COALESCE(spfn.bfm_extra_fee, 0) +
       COALESCE(spfn.receiving_fee, 0) +
       COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u.usd_krw, 1450),
    0) AS warehouse_profit_krw,
    ROUND(
      (COALESCE(spfn.storage_fee, 0) +
       COALESCE(spfn.request_photo_fee, 0) +
       COALESCE(spfn.repack_fee, 0) +
       COALESCE(spfn.bubble_wrap_fee, 0) +
       COALESCE(spfn.vacuum_repack_fee, 0) +
       COALESCE(spfn.plasticbox_fee, 0) +
       COALESCE(spfn.remove_papertube_fee, 0) +
       COALESCE(spfn.inclusion_fee, 0) +
       COALESCE(spfn.bfm_extra_fee, 0) +
       COALESCE(spfn.receiving_fee, 0) +
       COALESCE(spfn.package_extra_fee, 0)),
    2) AS warehouse_profit_usd,

    -- 배송 수익 (고객 배송비 - 배송 원가 = 마크업)
    ROUND(sc_u.marked_up_cost, 0) AS shipping_profit_krw,
    ROUND(sc_u.marked_up_cost / COALESCE(c_u.usd_krw, 1450), 2) AS shipping_profit_usd,

    spfn.storage_fee AS storage_fee_usd,
    ROUND(COALESCE(spfn.storage_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS storage_fee_krw, 
    spfn.request_photo_fee AS request_photo_fee_usd,
    ROUND(COALESCE(spfn.request_photo_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS request_photo_fee_krw,
    spfn.repack_fee AS repack_fee_usd,
    ROUND(COALESCE(spfn.repack_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS repack_fee_krw,
    spfn.bubble_wrap_fee AS bubble_wrap_fee_usd,
    ROUND(COALESCE(spfn.bubble_wrap_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
    spfn.vacuum_repack_fee AS vacuum_repack_fee_usd,
    ROUND(COALESCE(spfn.vacuum_repack_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
    spfn.plasticbox_fee AS plasticbox_fee_usd,
    ROUND(COALESCE(spfn.plasticbox_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS plasticbox_fee_krw,
    spfn.remove_papertube_fee AS remove_papertube_fee_usd,
    ROUND(COALESCE(spfn.remove_papertube_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
    spfn.inclusion_fee AS inclusion_fee_usd,
    ROUND(COALESCE(spfn.inclusion_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS inclusion_fee_krw,
    spfn.bfm_extra_fee AS bfm_extra_fee_usd,
    ROUND(COALESCE(spfn.bfm_extra_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS bfm_extra_fee_krw,
    spfn.receiving_fee AS receiving_fee_usd,
    ROUND(COALESCE(spfn.receiving_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS receiving_fee_krw,
    spfn.package_extra_fee AS package_extra_fee_usd,
    ROUND(COALESCE(spfn.package_extra_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS package_extra_fee_krw,
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS shipping_fee_krw,
    spfn.shipping_fee AS shipping_fee_usd,

    ROUND(sc_u.customer_cost, 0) AS customer_cost_krw,
    ROUND(sc_u.customer_cost / COALESCE(c_u.usd_krw, 1450), 2) AS customer_cost_usd,
    ROUND(sc_u.original_cost, 0) AS original_cost_krw,
    ROUND(sc_u.original_cost / COALESCE(c_u.usd_krw, 1450), 2) AS original_cost_usd,
    ROUND(sc_u.fuel_surcharge_cost, 0) AS fuel_surcharge_cost_krw,
    ROUND(sc_u.fuel_surcharge_cost / COALESCE(c_u.usd_krw, 1450), 2) AS fuel_surcharge_cost_usd,
    ROUND(sc_u.marked_up_cost, 0) AS marked_up_cost_krw,
    ROUND(sc_u.marked_up_cost / COALESCE(c_u.usd_krw, 1450), 2) AS marked_up_cost_usd,
    
    COALESCE(c_u.origin_usd_krw, 1450) AS origin_usd_krw_package,
    COALESCE(c_u.usd_krw, 1450) AS usd_krw_package

FROM verified_parent vp
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
    FROM `da-project-472406.data_warehouse.shipping_cost`
) sc_u ON vp.track_number = sc_u.tracking_number AND sc_u.rn = 1
-- 상위 패키지 테이블 직접 참조
LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_new` spfn ON vp.package_id = spfn.package_id
LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON vp.customer_id = cust.customer_id
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON vp.country_code = b2b.country_code
LEFT JOIN (
    SELECT currency_date, origin_usd_krw, usd_krw, ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) as rn
    FROM `da-project-472406.data_warehouse.currency`
) c_u ON DATE(vp.trans_at_utc) = c_u.currency_date AND c_u.rn = 1

UNION ALL

-- ================================================================================
-- Case 3-1: 하위 패키지(INNER) 개별 수익
-- ================================================================================
SELECT 
    cust.suite_number,
    DATE(cp.ship_at_kst) AS ship_date_kst,
    DATE(cp.trans_at_utc) AS trans_at_utc_package,
    cp.package_id,
    cp.inner_package_id,
    '3_1' AS profit_case,
    cp.package_merchant,
    cp.order_type,
    cp.package_type,
    cp.country_code,
    b2b.name_en AS country_name,
    b2b.name_kr AS country_name_kr,
    cp.carrier,
    cp.carrier_service,
    cp.package_weight,
    cp.dimension_weight,
    cp.package_count,
    cp.reference_type,

    -- ============================================================================
    -- 매출 상세 (Revenue Details)
    -- ============================================================================
    
    -- 1. 상품 구매 매출 (Case 3-1은 ASN 배송대행이므로 상품 매출 0)
    0 AS goods_revenue_krw,
    0 AS goods_revenue_usd,
    
    -- 2. 창고 옵션 매출 (11가지 하위 개별 창고 서비스 수수료 합계)
    ROUND((COALESCE(spfin.storage_fee, 0) + COALESCE(spfin.request_photo_fee, 0) + COALESCE(spfin.repack_fee, 0) + COALESCE(spfin.bubble_wrap_fee, 0) + COALESCE(spfin.vacuum_repack_fee, 0) + COALESCE(spfin.plasticbox_fee, 0) + COALESCE(spfin.remove_papertube_fee, 0) + COALESCE(spfin.inclusion_fee, 0) + COALESCE(spfin.bfm_extra_fee, 0) + COALESCE(spfin.receiving_fee, 0) + COALESCE(spfin.package_extra_fee, 0)) * COALESCE(curr.usd_krw, 1450), 0) AS warehouse_revenue_krw,
    ROUND((COALESCE(spfin.storage_fee, 0) + COALESCE(spfin.request_photo_fee, 0) + COALESCE(spfin.repack_fee, 0) + COALESCE(spfin.bubble_wrap_fee, 0) + COALESCE(spfin.vacuum_repack_fee, 0) + COALESCE(spfin.plasticbox_fee, 0) + COALESCE(spfin.remove_papertube_fee, 0) + COALESCE(spfin.inclusion_fee, 0) + COALESCE(spfin.bfm_extra_fee, 0) + COALESCE(spfin.receiving_fee, 0) + COALESCE(spfin.package_extra_fee, 0)), 2) AS warehouse_revenue_usd,
    
    -- 3. 배송비 매출 (shipment_package_fee_item_new.shipping_fee 컬럼 기준)
    ROUND(COALESCE(spfin.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_revenue_krw,
    COALESCE(spfin.shipping_fee, 0) AS shipping_revenue_usd,

    -- [수익 계산] 하위 패키지 테이블(fee_item_new) 수수료 사용 (시스템 환율 적용)
    ROUND(
        (
            COALESCE(spfin.storage_fee, 0) +
            COALESCE(spfin.request_photo_fee, 0) +
            COALESCE(spfin.repack_fee, 0) +
            COALESCE(spfin.bubble_wrap_fee, 0) +
            COALESCE(spfin.vacuum_repack_fee, 0) +
            COALESCE(spfin.plasticbox_fee, 0) +
            COALESCE(spfin.remove_papertube_fee, 0) +
            COALESCE(spfin.inclusion_fee, 0) +
            COALESCE(spfin.bfm_extra_fee, 0) +
            COALESCE(spfin.receiving_fee, 0) +
            COALESCE(spfin.package_extra_fee, 0)
        ) * COALESCE(curr.usd_krw, 1450),
    0) AS profit_krw,

    ROUND(
        (
            COALESCE(spfin.storage_fee, 0) +
            COALESCE(spfin.request_photo_fee, 0) +
            COALESCE(spfin.repack_fee, 0) +
            COALESCE(spfin.bubble_wrap_fee, 0) +
            COALESCE(spfin.vacuum_repack_fee, 0) +
            COALESCE(spfin.plasticbox_fee, 0) +
            COALESCE(spfin.remove_papertube_fee, 0) +
            COALESCE(spfin.inclusion_fee, 0) +
            COALESCE(spfin.bfm_extra_fee, 0) +
            COALESCE(spfin.receiving_fee, 0) +
            COALESCE(spfin.package_extra_fee, 0)
        ),
    2) AS profit_usd,

    -- 구매 수익 (Case 3-1은 ASN 배송대행이므로 0)
    0 AS goods_profit_krw,
    0 AS goods_profit_usd,

    -- 창고 수익 (11가지 창고 수수료 합계 = 수익)
    ROUND(
      (COALESCE(spfin.storage_fee, 0) +
       COALESCE(spfin.request_photo_fee, 0) +
       COALESCE(spfin.repack_fee, 0) +
       COALESCE(spfin.bubble_wrap_fee, 0) +
       COALESCE(spfin.vacuum_repack_fee, 0) +
       COALESCE(spfin.plasticbox_fee, 0) +
       COALESCE(spfin.remove_papertube_fee, 0) +
       COALESCE(spfin.inclusion_fee, 0) +
       COALESCE(spfin.bfm_extra_fee, 0) +
       COALESCE(spfin.receiving_fee, 0) +
       COALESCE(spfin.package_extra_fee, 0)) * COALESCE(curr.usd_krw, 1450),
    0) AS warehouse_profit_krw,
    ROUND(
      (COALESCE(spfin.storage_fee, 0) +
       COALESCE(spfin.request_photo_fee, 0) +
       COALESCE(spfin.repack_fee, 0) +
       COALESCE(spfin.bubble_wrap_fee, 0) +
       COALESCE(spfin.vacuum_repack_fee, 0) +
       COALESCE(spfin.plasticbox_fee, 0) +
       COALESCE(spfin.remove_papertube_fee, 0) +
       COALESCE(spfin.inclusion_fee, 0) +
       COALESCE(spfin.bfm_extra_fee, 0) +
       COALESCE(spfin.receiving_fee, 0) +
       COALESCE(spfin.package_extra_fee, 0)),
    2) AS warehouse_profit_usd,

    -- 배송 수익 (Case 3-1 하위 패키지는 개별 배송비 없음)
    CAST(NULL AS FLOAT64) AS shipping_profit_krw,
    CAST(NULL AS FLOAT64) AS shipping_profit_usd,

    spfin.storage_fee AS storage_fee_usd,
    ROUND(COALESCE(spfin.storage_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS storage_fee_krw,
    spfin.request_photo_fee AS request_photo_fee_usd,
    ROUND(COALESCE(spfin.request_photo_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS request_photo_fee_krw,
    spfin.repack_fee AS repack_fee_usd,
    ROUND(COALESCE(spfin.repack_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS repack_fee_krw,
    spfin.bubble_wrap_fee AS bubble_wrap_fee_usd,
    ROUND(COALESCE(spfin.bubble_wrap_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
    spfin.vacuum_repack_fee AS vacuum_repack_fee_usd,
    ROUND(COALESCE(spfin.vacuum_repack_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
    spfin.plasticbox_fee AS plasticbox_fee_usd,
    ROUND(COALESCE(spfin.plasticbox_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS plasticbox_fee_krw,
    spfin.remove_papertube_fee AS remove_papertube_fee_usd,
    ROUND(COALESCE(spfin.remove_papertube_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
    spfin.inclusion_fee AS inclusion_fee_usd,
    ROUND(COALESCE(spfin.inclusion_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS inclusion_fee_krw,
    spfin.bfm_extra_fee AS bfm_extra_fee_usd,
    ROUND(COALESCE(spfin.bfm_extra_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS bfm_extra_fee_krw,
    spfin.receiving_fee AS receiving_fee_usd,
    ROUND(COALESCE(spfin.receiving_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS receiving_fee_krw,
    spfin.package_extra_fee AS package_extra_fee_usd,
    ROUND(COALESCE(spfin.package_extra_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS package_extra_fee_krw,
    ROUND(COALESCE(spfin.shipping_fee, 0) * COALESCE(curr.usd_krw, 1450), 0) AS shipping_fee_krw,
    spfin.shipping_fee AS shipping_fee_usd,

    CAST(NULL AS FLOAT64) AS customer_cost_krw,
    CAST(NULL AS FLOAT64) AS customer_cost_usd,
    CAST(NULL AS FLOAT64) AS original_cost_krw,
    CAST(NULL AS FLOAT64) AS original_cost_usd,
    CAST(NULL AS FLOAT64) AS fuel_surcharge_cost_krw,
    CAST(NULL AS FLOAT64) AS fuel_surcharge_cost_usd,
    CAST(NULL AS FLOAT64) AS marked_up_cost_krw,
    CAST(NULL AS FLOAT64) AS marked_up_cost_usd,
    
    COALESCE(curr.origin_usd_krw, 1450) AS origin_usd_krw_package,
    COALESCE(curr.usd_krw, 1450) AS usd_krw_package

FROM `da-project-472406.data_warehouse.shipment_package_fee_item_new` spfin
JOIN child_packages cp ON spfin.inner_package_id = cp.inner_package_id
LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON cp.customer_id = cust.customer_id
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON cp.country_code = b2b.country_code
LEFT JOIN (
    SELECT currency_date, origin_usd_krw, usd_krw, ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) as rn
    FROM `da-project-472406.data_warehouse.currency`
) curr ON DATE(cp.trans_at_utc) = curr.currency_date AND curr.rn = 1
WHERE spfin.inner_package_id != 0

ORDER BY package_id ASC, profit_case ASC;
