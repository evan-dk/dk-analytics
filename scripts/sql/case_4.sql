/*
================================================================================
  Case 4 수익 분석 쿼리
================================================================================
  목적: 구매대행 서비스(WeBuy)의 묶음 패키지(CONSOLE/REPACK)에 대한 통합 수익 계산
  
  주요 계산:
  - 상위(부모) 패키지의 배송 마크업과 모든 하위(자식) 패키지의 서비스 수수료를 합산
  - 개별 하위 패키지의 수익 = (상품가 + 핸들링피 + 수수료 - PG수수료 - 할인 등)
  - 부모 패키지 레벨에서 모든 자식 패키지의 수익을 합산하여 최종 profit_krw 산출
  
  필터 조건:
  - 상위 패키지 타입: CONSOLE 또는 REPACK
  - 주문 유형: WE_SHIP 제외 (구매대행 유형 위주)
  - 하위 패키지 조건: 모든 하위 패키지의 reference_type이 'BUY_REQUEST' 또는 'BUYFORME'일 것
  
  데이터 구조:
  1. verified_parent: 배송 비용 정보가 있는 유효한 상위 패키지 식별
  2. raw_data: 상위 패키지와 그에 속한 모든 하위 패키지의 상세 데이터 결합
  3. aggregated_data: 하위 패키지들의 수익 요소를 부모 패키지 ID 기준으로 합산
================================================================================
*/

WITH verified_parent AS (
    -- ============================================================================
    -- 1. 상위 패키지(REPACK, CONSOLE) 및 배송 비용 정보 추출
    -- ============================================================================
    SELECT 
        spn.package_id AS parent_id,
        spn.trans_at_utc AS parent_trans_at_utc,
        spn.package_type AS parent_type,
        spn.package_merchant AS parent_merchant,
        spn.order_type AS parent_order_type,
        spn.customer_id AS parent_customer_id,
        spn.reference_id AS parent_reference_id,
        spn.country_code,
        spn.carrier,
        spn.carrier_service,
        spn.package_weight,
        spn.dimension_weight,
        spn.package_count,    -- 패키지 개수
        spn.reference_type,   -- 참조 유형
        spn.shipping_fee,     -- 패키지 테이블 배송비
        sc_u.customer_cost,
        sc_u.original_cost,
        sc_u.fuel_surcharge_cost,
        sc_u.marked_up_cost
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    -- 배송 비용 테이블 (중복 제거 후 조인)
    LEFT JOIN (
        -- 배송비 중복 제거 (송장번호당 가장 높은 원가 기준)
        SELECT *, ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
        FROM `da-project-472406.data_warehouse.shipping_cost`
    ) sc_u
        ON TRIM(spn.track_number) = TRIM(sc_u.tracking_number) AND sc_u.rn = 1
    WHERE spn.package_type IN ('CONSOLE', 'REPACK')
      AND spn.order_type NOT LIKE '%WE_SHIP%'
      -- 날짜 필터: 2025년 데이터만 조회
      AND DATE(spn.trans_at_utc) BETWEEN '2025-01-01' AND CURRENT_DATE()
      -- [조건 1] 모든 하위 패키지의 reference_type이 'BUY_REQUEST' 또는 'BUYFORME'여야 함
      AND NOT EXISTS (
          SELECT 1 
          FROM `da-project-472406.data_warehouse.shipment_package_new` child
          WHERE child.package_id = spn.package_id 
            AND child.package_type = 'INNER'
            AND (child.reference_type IS NULL OR child.reference_type NOT IN ('BUY_REQUEST', 'BUYFORME'))
      )
),

raw_data AS (
    -- ============================================================================
    -- 2. 상위 및 하위 패키지별 상세 데이터 결합
    -- ============================================================================
    SELECT 
        vp.parent_id,
        spn.package_id,
        spn.inner_package_id,
        spn.package_type,
        spn.package_merchant,
        spn.order_type,
        spn.ship_at_kst,
        vp.country_code,
        vp.carrier,
        vp.carrier_service,
        vp.package_weight,
        vp.dimension_weight,
        vp.package_count,
        vp.reference_type,
        vp.shipping_fee,
        wam.market_id,
        cust.suite_number,
        COALESCE(spn.trans_at_utc, vp.parent_trans_at_utc) AS trans_at_utc_pkg,
        t.trans_at_utc AS trans_at_utc_webuy,
        COALESCE(c_u_t.origin_usd_krw, 1450) AS ex_webuy_official,
        COALESCE(c_u_t.usd_krw, 1450) AS ex_webuy_system,
        COALESCE(c_u_spn.origin_usd_krw, 1450) AS ex_pkg_official,
        COALESCE(c_u_spn.usd_krw, 1450) AS ex_pkg_system,
        
        -- 상품 금액
        (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) AS goods_krw,
        ROUND((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) / COALESCE(c_u_t.usd_krw, 1450), 2) AS goods_usd,
        COALESCE(wam.fee_unit_price_usd, 0) AS fee_unit_price_usd, -- 원본 단가 (USD)
        COALESCE(wam.quotation_quantity, 0) AS qty_raw,
        
        -- 기타 수수료 (시스템 환율 적용)
        ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) AS handling_krw,
        COALESCE(wam.fee_handling_fee_usd, 0) AS handling_usd,
        
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) AS surtax_krw,
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) AS surtax_usd,
        
        ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) AS dom_ship_krw,
        COALESCE(wam.fee_domestic_shipping_price_usd, 0) AS dom_ship_usd,
        
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0) AS pg_fee_krw,
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END / COALESCE(c_u_t.usd_krw, 1450), 2) AS pg_fee_usd,
        
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id = 20 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) AS dk_fee_krw,
        ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id = 20 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END / COALESCE(c_u_t.usd_krw, 1450), 2) AS dk_fee_usd,
        
        ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) AS disc_krw,
        COALESCE(t.discount_value, 0) AS disc_usd,
        ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) AS coupon_krw,
        COALESCE(t.coupon_discount_value, 0) AS coupon_usd,
        
        -- 서비스 수수료 (storage, repack 등) (시스템 환율 적용)
        ROUND(COALESCE(f.storage_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS s_fee_krw,
        COALESCE(f.storage_fee, 0) AS s_fee_usd,
        ROUND(COALESCE(f.request_photo_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_fee_krw,
        COALESCE(f.request_photo_fee, 0) AS p_fee_usd,
        ROUND(COALESCE(f.repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS r_fee_krw,
        COALESCE(f.repack_fee, 0) AS r_fee_usd,
        ROUND(COALESCE(f.bubble_wrap_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS b_fee_krw,
        COALESCE(f.bubble_wrap_fee, 0) AS b_fee_usd,
        ROUND(COALESCE(f.vacuum_repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS v_fee_krw,
        COALESCE(f.vacuum_repack_fee, 0) AS v_fee_usd,
        ROUND(COALESCE(f.plasticbox_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS pl_fee_krw,
        COALESCE(f.plasticbox_fee, 0) AS pl_fee_usd,
        ROUND(COALESCE(f.remove_papertube_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS pt_fee_krw,
        COALESCE(f.remove_papertube_fee, 0) AS pt_fee_usd,
        ROUND(COALESCE(f.inclusion_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS i_fee_krw,
        COALESCE(f.inclusion_fee, 0) AS i_fee_usd,
        ROUND(COALESCE(f.bfm_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS bfm_fee_krw,
        COALESCE(f.bfm_extra_fee, 0) AS bfm_fee_usd,
        
        -- 배송 비용 정보 (부모 패키지에만 적용)
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN COALESCE(vp.marked_up_cost, 0) ELSE 0 END AS markup_raw,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN ROUND(COALESCE(vp.marked_up_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) ELSE 0 END AS markup_usd,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN COALESCE(vp.customer_cost, 0) ELSE 0 END AS c_cost_krw,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN ROUND(COALESCE(vp.customer_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) ELSE 0 END AS c_cost_usd,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN COALESCE(vp.original_cost, 0) ELSE 0 END AS o_cost_krw,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN ROUND(COALESCE(vp.original_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) ELSE 0 END AS o_cost_usd,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN COALESCE(vp.fuel_surcharge_cost, 0) ELSE 0 END AS f_cost_krw,
        CASE WHEN spn.package_type IN ('CONSOLE', 'REPACK') THEN ROUND(COALESCE(vp.fuel_surcharge_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) ELSE 0 END AS f_cost_usd
    FROM verified_parent vp
    INNER JOIN `da-project-472406.data_warehouse.shipment_package_new` spn ON vp.parent_id = spn.package_id
    LEFT JOIN (
        -- 상위패키지 수수료와 하위패키지 수수료 결합
        SELECT 'PARENT' AS type, package_id AS join_key, storage_fee, request_photo_fee, repack_fee, bubble_wrap_fee, vacuum_repack_fee, plasticbox_fee, remove_papertube_fee, inclusion_fee, bfm_extra_fee FROM `da-project-472406.data_warehouse.shipment_package_fee_new`
        UNION ALL
        SELECT 'CHILD' AS type, inner_package_id AS join_key, storage_fee, request_photo_fee, repack_fee, bubble_wrap_fee, vacuum_repack_fee, plasticbox_fee, remove_papertube_fee, inclusion_fee, bfm_extra_fee FROM `da-project-472406.data_warehouse.shipment_package_fee_item_new`
    ) f ON (CASE WHEN spn.package_type = 'INNER' THEN 'CHILD' ELSE 'PARENT' END) = f.type AND (CASE WHEN spn.package_type = 'INNER' THEN spn.inner_package_id ELSE spn.package_id END) = f.join_key
    LEFT JOIN `da-project-472406.data_warehouse.webuy_all_market` wam ON spn.reference_id = wam.request_id 
    LEFT JOIN `da-project-472406.data_warehouse.transaction` t ON wam.request_id = t.reference_id AND t.pay_service IN ('NewWeBuy', 'BUY_REQUEST') AND t.transaction_status = 'CNF'
    LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON spn.customer_id = cust.customer_id
    LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_t ON DATE(t.trans_at_utc) = c_u_t.currency_date
    LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_spn ON DATE(COALESCE(spn.trans_at_utc, vp.parent_trans_at_utc)) = c_u_spn.currency_date
    WHERE (wam.last_status IS NULL OR wam.last_status != 'CANCELED')
),

aggregated_data AS (
    -- ============================================================================
    -- 3. 하위 패키지 항목들을 부모 패키지 기준으로 합산
    -- ============================================================================
    SELECT
        *,
        -- 프로핏 계산 공식: sum(개별 하위 패키지 수익) + 부모 배송 마크업
        SUM((goods_krw + dom_ship_krw + handling_krw) + surtax_krw + dk_fee_krw - dom_ship_krw - pg_fee_krw - (disc_krw + coupon_krw) + (s_fee_krw + p_fee_krw + r_fee_krw + b_fee_krw + v_fee_krw + pl_fee_krw + pt_fee_krw + i_fee_krw + bfm_fee_krw) + markup_raw) OVER(PARTITION BY parent_id) AS total_profit_krw,
        SUM((goods_usd + dom_ship_usd + handling_usd) + surtax_usd + dk_fee_usd - dom_ship_usd - pg_fee_usd - (disc_usd + coupon_usd) + (s_fee_usd + p_fee_usd + r_fee_usd + b_fee_usd + v_fee_usd + pl_fee_usd + pt_fee_usd + i_fee_usd + bfm_fee_usd) + markup_usd) OVER(PARTITION BY parent_id) AS total_profit_usd,
        
        SUM(handling_krw) OVER(PARTITION BY parent_id) AS total_handling_krw,
        SUM(handling_usd) OVER(PARTITION BY parent_id) AS total_handling_usd,
        SUM(surtax_krw) OVER(PARTITION BY parent_id) AS total_surtax_krw,
        SUM(surtax_usd) OVER(PARTITION BY parent_id) AS total_surtax_usd,
        SUM(goods_krw) OVER(PARTITION BY parent_id) AS total_goods_krw,
        SUM(goods_usd) OVER(PARTITION BY parent_id) AS total_goods_usd,
        SUM(dom_ship_krw) OVER(PARTITION BY parent_id) AS total_dom_ship_krw,
        SUM(dom_ship_usd) OVER(PARTITION BY parent_id) AS total_dom_ship_usd,
        SUM(pg_fee_krw) OVER(PARTITION BY parent_id) AS total_pg_fee_krw,
        SUM(pg_fee_usd) OVER(PARTITION BY parent_id) AS total_pg_fee_usd,
        SUM(dk_fee_krw) OVER(PARTITION BY parent_id) AS total_dk_fee_krw,
        SUM(dk_fee_usd) OVER(PARTITION BY parent_id) AS total_dk_fee_usd,
        SUM(disc_krw) OVER(PARTITION BY parent_id) AS total_disc_krw,
        SUM(disc_usd) OVER(PARTITION BY parent_id) AS total_disc_usd,
        SUM(coupon_krw) OVER(PARTITION BY parent_id) AS total_coupon_krw,
        SUM(coupon_usd) OVER(PARTITION BY parent_id) AS total_coupon_usd,
        
        SUM(s_fee_krw) OVER(PARTITION BY parent_id) AS total_s_fee_krw,
        SUM(s_fee_usd) OVER(PARTITION BY parent_id) AS total_s_fee_usd,
        SUM(p_fee_krw) OVER(PARTITION BY parent_id) AS total_p_fee_krw,
        SUM(p_fee_usd) OVER(PARTITION BY parent_id) AS total_p_fee_usd,
        SUM(r_fee_krw) OVER(PARTITION BY parent_id) AS total_r_fee_krw,
        SUM(r_fee_usd) OVER(PARTITION BY parent_id) AS total_r_fee_usd,
        SUM(b_fee_krw) OVER(PARTITION BY parent_id) AS total_b_fee_krw,
        SUM(b_fee_usd) OVER(PARTITION BY parent_id) AS total_b_fee_usd,
        SUM(v_fee_krw) OVER(PARTITION BY parent_id) AS total_v_fee_krw,
        SUM(v_fee_usd) OVER(PARTITION BY parent_id) AS total_v_fee_usd,
        SUM(pl_fee_krw) OVER(PARTITION BY parent_id) AS total_pl_fee_krw,
        SUM(pl_fee_usd) OVER(PARTITION BY parent_id) AS total_pl_fee_usd,
        SUM(pt_fee_krw) OVER(PARTITION BY parent_id) AS total_pt_fee_krw,
        SUM(pt_fee_usd) OVER(PARTITION BY parent_id) AS total_pt_fee_usd,
        SUM(i_fee_krw) OVER(PARTITION BY parent_id) AS total_i_fee_krw,
        SUM(i_fee_usd) OVER(PARTITION BY parent_id) AS total_i_fee_usd,
        SUM(bfm_fee_krw) OVER(PARTITION BY parent_id) AS total_bfm_fee_krw,
        SUM(bfm_fee_usd) OVER(PARTITION BY parent_id) AS total_bfm_fee_usd,
        
        SUM(unit_price_raw) OVER(PARTITION BY parent_id) AS total_unit_price,
        SUM(qty_raw) OVER(PARTITION BY parent_id) AS total_qty
    FROM raw_data
)

-- ================================================================================
-- 최종 출력: 상위 패키지 레벨로 결과 조회
-- ================================================================================
SELECT
    ad.suite_number,
    ad.market_id,
    DATE(ad.ship_at_kst) AS ship_date_kst,
    DATE(ad.trans_at_utc_webuy) AS trans_at_utc_webuy, 
    DATE(ad.trans_at_utc_pkg) AS trans_at_utc_package, 
    ad.package_id, 
    ad.inner_package_id,
    '4' AS profit_case,
    ad.package_merchant, 
    ad.order_type, 
    ad.package_type,
    ad.country_code,
    b2b.name_en AS country_name,
    ad.carrier,
    ad.carrier_service,
    ad.package_weight,
    ad.dimension_weight,
    ad.package_count,
    ad.reference_type,
    ad.shipping_fee,
    ROUND(COALESCE(ad.shipping_fee, 0) * COALESCE(ad.ex_pkg_system, 1450), 0) AS shipping_fee_krw,
    
    ROUND(ad.total_profit_krw, 0) AS profit_krw,
    ROUND(ad.total_profit_usd, 2) AS profit_usd,
    
    ad.total_unit_price AS fee_unit_price_krw,
    ad.total_qty AS quotation_quantity,
    ad.total_goods_krw AS total_goods_price_krw,
    ad.total_goods_usd AS total_goods_price_usd,
    ad.total_handling_krw AS fee_handling_fee_krw,
    ad.total_handling_usd AS fee_handling_fee_usd,
    ad.total_dom_ship_krw AS domestic_shipping_price_krw,
    ad.total_dom_ship_usd AS domestic_shipping_price_usd,
    ad.total_surtax_krw AS surtax_krw,
    ad.total_surtax_usd AS surtax_usd,
    ad.total_pg_fee_krw AS pg_fee_krw,
    ad.total_pg_fee_usd AS pg_fee_usd,
    ad.total_dk_fee_krw AS dk_fee_krw,
    ad.total_dk_fee_usd AS dk_fee_usd,
    ad.total_disc_krw AS discount_value_krw,
    ad.total_disc_usd AS discount_value_usd,
    ad.total_coupon_krw AS coupon_discount_value_krw,
    ad.total_coupon_usd AS coupon_discount_value_usd,
    
    ad.total_s_fee_krw AS storage_fee_krw,
    ad.total_s_fee_usd AS storage_fee_usd,
    ad.total_p_fee_krw AS request_photo_fee_krw,
    ad.total_p_fee_usd AS request_photo_fee_usd,
    ad.total_r_fee_krw AS repack_fee_krw,
    ad.total_r_fee_usd AS repack_fee_usd,
    ad.total_b_fee_krw AS bubble_wrap_fee_krw,
    ad.total_b_fee_usd AS bubble_wrap_fee_usd,
    ad.total_v_fee_krw AS vacuum_repack_fee_krw,
    ad.total_v_fee_usd AS vacuum_repack_fee_usd,
    ad.total_pl_fee_krw AS plasticbox_fee_krw,
    ad.total_pl_fee_usd AS plasticbox_fee_usd,
    ad.total_pt_fee_krw AS remove_papertube_fee_krw,
    ad.total_pt_fee_usd AS remove_papertube_fee_usd,
    ad.total_i_fee_krw AS inclusion_fee_krw,
    ad.total_i_fee_usd AS inclusion_fee_usd,
    ad.total_bfm_fee_krw AS bfm_extra_fee_krw,
    ad.total_bfm_fee_usd AS bfm_extra_fee_usd,
    
    ad.c_cost_krw AS customer_cost_krw,
    ad.c_cost_usd AS customer_cost_usd,
    ad.o_cost_krw AS original_cost_krw,
    ad.o_cost_usd AS original_cost_usd,
    ad.f_cost_krw AS fuel_surcharge_cost_krw,
    ad.f_cost_usd AS fuel_surcharge_cost_usd,
    ad.markup_raw AS marked_up_cost_krw,
    ad.markup_usd AS marked_up_cost_usd,
    
    ad.ex_webuy_official AS origin_usd_krw_webuy,
    ad.ex_webuy_system AS usd_krw_webuy,
    ad.ex_pkg_official AS origin_usd_krw_package,
    ad.ex_pkg_system AS usd_krw_package
FROM aggregated_data ad
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON ad.country_code = b2b.country_code
WHERE ad.package_type != 'INNER'
ORDER BY ad.parent_id ASC;