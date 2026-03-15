/*
================================================================================
  Case 5 수익 분석 쿼리
================================================================================
  목적: DK_SHOP 서비스 또는 WeShip 변형(WE_SHIP_...)의 묶음 패키지에 대한 수익 계산
  
  주요 계산:
  - DK_SHOP인 경우 하위 패키지 상품가를 수익에서 제외 (DK_SHOP 수수료 구조 반영)
  - 상위(부모) 패키지의 배송 마크업과 모든 하위(자식) 패키지의 서비스 수수료를 합산
  
  필터 조건:
  - 상위 패키지 타입: CONSOLE 또는 REPACK
  - 주문 유형: (WE_SHIP 파생형 AND WE_SHIP 아님) OR (DK_SHOP)
================================================================================
*/

WITH verified_parent AS (
    -- 1. 상위 패키지 정보 추출 (WE_SHIP 파생형 및 정확히 DK_SHOP 포함)
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
        spn.shipping_fee,     -- 패키지 테이블 배송비
        sc_u.customer_cost AS p_customer_cost,
        sc_u.original_cost AS p_original_cost,
        sc_u.fuel_surcharge_cost AS p_fuel_surcharge_cost,
        sc_u.marked_up_cost AS p_marked_up_cost
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    INNER JOIN (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
        FROM `da-project-472406.data_warehouse.shipping_cost`
    ) sc_u ON TRIM(spn.track_number) = TRIM(sc_u.tracking_number) AND sc_u.rn = 1
    WHERE spn.package_type IN ('CONSOLE', 'REPACK')
      AND (
          (spn.order_type LIKE '%WE_SHIP%' AND spn.order_type != 'WE_SHIP') 
          OR spn.order_type = 'DK_SHOP'
      )
      -- 날짜 필터: 2025년 데이터만 조회
      AND DATE(spn.trans_at_utc) BETWEEN '2025-01-01' AND CURRENT_DATE()
),

raw_combined AS (
    -- 2. 하위 데이터 결합 (DK_SHOP/WE_SHIP 격리)
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
        vp.shipping_fee,
        ROUND(COALESCE(vp.shipping_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS shipping_fee_krw,
        wam.market_id,
        cust.suite_number,
        COALESCE(spn.trans_at_utc, vp.parent_trans_at_utc) AS trans_at_utc_pkg,
        t.trans_at_utc AS trans_at_utc_webuy,
        COALESCE(c_u_t.origin_usd_krw, 1450) AS ex_webuy_official,
        COALESCE(c_u_t.usd_krw, 1450) AS ex_webuy_system,
        COALESCE(c_u_spn.origin_usd_krw, 1450) AS ex_pkg_official,
        COALESCE(c_u_spn.usd_krw, 1450) AS ex_pkg_system,
        
        -- 상품 실적 (DK_SHOP/WE_SHIP 변형에 따른 격리)
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) END AS goods_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE ROUND((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) / COALESCE(c_u_t.usd_krw, 1450), 2) END AS goods_usd,
        
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(wam.fee_unit_price_usd, 0) END AS fee_unit_price_usd, -- 원본 단가 (USD)
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(wam.quotation_quantity, 0) END AS qty_raw,
        
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS handling_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(wam.fee_handling_fee_usd, 0) END AS handling_usd,
             
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END, 0) 
        END AS surtax_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END, 2) 
        END AS surtax_usd,

        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS dom_ship_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(wam.fee_domestic_shipping_price_usd, 0) END AS dom_ship_usd,

        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0) 
        END AS pg_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END / COALESCE(c_u_t.usd_krw, 1450), 2) 
        END AS pg_usd,

        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id = 20 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0) 
        END AS dk_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' THEN 0 
             ELSE ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id = 20 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END / COALESCE(c_u_t.usd_krw, 1450), 2) 
        END AS dk_usd,

        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS disc_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(t.discount_value, 0) END AS disc_usd,

        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450), 0) END AS coup_krw,
        CASE WHEN spn.order_type IN ('WE_SHIP', 'DK_SHOP') OR spn.order_type LIKE 'WE_SHIP%' 
             THEN 0 ELSE COALESCE(t.coupon_discount_value, 0) END AS coup_usd,
        
        -- 부모 패키지 수수료 (시스템 환율 적용)
        ROUND(COALESCE(spfn.storage_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_s_f_krw,
        COALESCE(spfn.storage_fee, 0) AS p_s_f_usd,
        ROUND(COALESCE(spfn.request_photo_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_p_f_krw,
        COALESCE(spfn.request_photo_fee, 0) AS p_p_f_usd,
        ROUND(COALESCE(spfn.repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_r_f_krw,
        COALESCE(spfn.repack_fee, 0) AS p_r_f_usd,
        ROUND(COALESCE(spfn.bubble_wrap_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_b_f_krw,
        COALESCE(spfn.bubble_wrap_fee, 0) AS p_b_f_usd,
        ROUND(COALESCE(spfn.vacuum_repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_v_f_krw,
        COALESCE(spfn.vacuum_repack_fee, 0) AS p_v_f_usd,
        ROUND(COALESCE(spfn.plasticbox_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_pl_f_krw,
        COALESCE(spfn.plasticbox_fee, 0) AS p_pl_f_usd,
        ROUND(COALESCE(spfn.remove_papertube_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_pt_f_krw,
        COALESCE(spfn.remove_papertube_fee, 0) AS p_pt_f_usd,
        ROUND(COALESCE(spfn.inclusion_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_i_f_krw,
        COALESCE(spfn.inclusion_fee, 0) AS p_i_f_usd,
        ROUND(COALESCE(spfn.bfm_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS p_bfm_f_krw,
        COALESCE(spfn.bfm_extra_fee, 0) AS p_bfm_f_usd,

        vp.p_customer_cost AS p_c_krw, 
        ROUND(vp.p_customer_cost / COALESCE(c_u_spn.usd_krw, 1450), 2) AS p_c_usd,
        vp.p_original_cost AS p_o_krw, 
        ROUND(vp.p_original_cost / COALESCE(c_u_spn.usd_krw, 1450), 2) AS p_o_usd,
        vp.p_fuel_surcharge_cost AS p_f_krw, 
        ROUND(vp.p_fuel_surcharge_cost / COALESCE(c_u_spn.usd_krw, 1450), 2) AS p_f_usd,
        vp.p_marked_up_cost AS p_m_krw,
        ROUND(vp.p_marked_up_cost / COALESCE(c_u_spn.usd_krw, 1450), 2) AS p_m_usd
    FROM `da-project-472406.data_warehouse.shipment_package_new` spn
    INNER JOIN verified_parent vp ON spn.package_id = vp.parent_id
    LEFT JOIN `da-project-472406.data_warehouse.webuy_all_market` wam ON spn.reference_id = wam.request_id AND spn.order_type NOT IN ('WE_SHIP', 'DK_SHOP')
    LEFT JOIN `da-project-472406.data_warehouse.transaction` t ON wam.request_id = t.reference_id AND t.pay_service IN ('NewWeBuy', 'BUY_REQUEST') AND t.transaction_status = 'CNF'
    LEFT JOIN `da-project-472406.data_warehouse.customer` cust ON spn.customer_id = cust.customer_id
    LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_new` spfn ON vp.parent_id = spfn.package_id
    LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_t ON DATE(t.trans_at_utc) = c_u_t.currency_date
    LEFT JOIN (SELECT currency_date, origin_usd_krw, usd_krw FROM `da-project-472406.data_warehouse.currency` QUALIFY ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) = 1) c_u_spn ON DATE(COALESCE(spn.trans_at_utc, vp.parent_trans_at_utc)) = c_u_spn.currency_date
    WHERE (wam.last_status IS NULL OR wam.last_status != 'CANCELED')
),
final_aggregated AS (
    -- 3. 집계 수행 (Profit 계산 시 DK_SHOP 예외 처리)
    SELECT
        *,
        SUM(CASE WHEN package_type = 'INNER' THEN 
                -- DK_SHOP인 경우 상품가(goods_krw)를 이익 계산에서 제외
                (CASE WHEN order_type = 'DK_SHOP' THEN 0 ELSE goods_krw END + handling_krw + surtax_krw + dk_krw - pg_krw - disc_krw - coup_krw) 
            ELSE 0 END) OVER(PARTITION BY parent_id) 
        + p_s_f_krw + p_p_f_krw + p_r_f_krw + p_b_f_krw + p_v_f_krw + p_pl_f_krw + p_pt_f_krw + p_i_f_krw + p_bfm_f_krw + p_m_krw AS total_profit_krw,

        SUM(CASE WHEN package_type = 'INNER' THEN 
                (CASE WHEN order_type = 'DK_SHOP' THEN 0 ELSE goods_usd END + handling_usd + surtax_usd + dk_usd - pg_usd - disc_usd - coup_usd) 
            ELSE 0 END) OVER(PARTITION BY parent_id) 
        + p_s_f_usd + p_p_f_usd + p_r_f_usd + p_b_f_usd + p_v_f_usd + p_pl_f_usd + p_pt_f_usd + p_i_f_usd + p_bfm_f_usd + p_m_usd AS total_profit_usd,
        
        SUM(CASE WHEN package_type = 'INNER' THEN unit_price_raw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_unit_price,
        SUM(CASE WHEN package_type = 'INNER' THEN qty_raw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_qty,
        SUM(CASE WHEN package_type = 'INNER' THEN goods_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_goods_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN goods_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_goods_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN handling_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_handling_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN handling_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_handling_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN dom_ship_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_dom_ship_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN dom_ship_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_dom_ship_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN surtax_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_surtax_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN surtax_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_surtax_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN pg_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_pg_fee_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN pg_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_pg_fee_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN dk_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_dk_fee_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN dk_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_dk_fee_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN disc_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_disc_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN disc_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_disc_usd,
        SUM(CASE WHEN package_type = 'INNER' THEN coup_krw ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_coupon_krw,
        SUM(CASE WHEN package_type = 'INNER' THEN coup_usd ELSE 0 END) OVER(PARTITION BY parent_id) AS sum_coupon_usd
    FROM raw_combined
)
-- 4. 최종 출력
SELECT
    fa.suite_number, fa.market_id, fa.ship_at_kst AS ship_date_kst, 
    fa.trans_at_utc_webuy, fa.trans_at_utc_pkg AS trans_at_utc_package, 
    fa.package_id, 5 AS profit_case,
    fa.inner_package_id, fa.package_merchant, fa.order_type, fa.package_type,
    fa.country_code, b2b.name_en AS country_name, fa.carrier, fa.carrier_service,
    fa.package_weight, fa.dimension_weight,
    fa.shipping_fee,
    
    ROUND(fa.total_profit_krw, 0) AS profit_krw,
    ROUND(fa.total_profit_usd, 2) AS profit_usd,
    
    fa.sum_unit_price AS fee_unit_price_krw,
    fa.sum_qty AS quotation_quantity,
    fa.sum_goods_krw AS total_goods_price_krw,
    fa.sum_goods_usd AS total_goods_price_usd,
    fa.sum_handling_krw AS fee_handling_fee_krw,
    fa.sum_handling_usd AS fee_handling_fee_usd,
    fa.sum_dom_ship_krw AS domestic_shipping_price_krw,
    fa.sum_dom_ship_usd AS domestic_shipping_price_usd,
    fa.sum_surtax_krw AS surtax_krw,
    fa.sum_surtax_usd AS surtax_usd,
    fa.sum_pg_fee_krw AS pg_fee_krw,
    fa.sum_pg_fee_usd AS pg_fee_usd,
    fa.sum_dk_fee_krw AS dk_fee_krw,
    fa.sum_dk_fee_usd AS dk_fee_usd,
    fa.sum_disc_krw AS discount_value_krw,
    fa.sum_disc_usd AS discount_value_usd,
    fa.sum_coupon_krw AS coupon_discount_value_krw,
    fa.sum_coupon_usd AS coupon_discount_value_usd,
    
    fa.p_s_f_krw AS storage_fee_krw,
    fa.p_s_f_usd AS storage_fee_usd,
    fa.p_p_f_krw AS request_photo_fee_krw,
    fa.p_p_f_usd AS request_photo_fee_usd,
    fa.p_r_f_krw AS repack_fee_krw,
    fa.p_r_f_usd AS repack_fee_usd,
    fa.p_b_f_krw AS bubble_wrap_fee_krw,
    fa.p_b_f_usd AS bubble_wrap_fee_usd,
    fa.p_v_f_krw AS vacuum_repack_fee_krw,
    fa.p_v_f_usd AS vacuum_repack_fee_usd,
    fa.p_pl_f_krw AS plasticbox_fee_krw,
    fa.p_pl_f_usd AS plasticbox_fee_usd,
    fa.p_pt_f_krw AS remove_papertube_fee_krw,
    fa.p_pt_f_usd AS remove_papertube_fee_usd,
    fa.p_i_f_krw AS inclusion_fee_krw,
    fa.p_i_f_usd AS inclusion_fee_usd,
    fa.p_bfm_f_krw AS bfm_extra_fee_krw,
    fa.p_bfm_f_usd AS bfm_extra_fee_usd,
    
    fa.p_c_krw AS customer_cost_krw,
    fa.p_c_usd AS customer_cost_usd,
    fa.p_o_krw AS original_cost_krw,
    fa.p_o_usd AS original_cost_usd,
    fa.p_f_krw AS fuel_surcharge_cost_krw,
    fa.p_f_usd AS fuel_surcharge_cost_usd,
    fa.p_m_krw AS marked_up_cost_krw,
    fa.p_m_usd AS marked_up_cost_usd,
    
    fa.ex_webuy_official AS origin_usd_krw_webuy, 
    fa.ex_webuy_system AS usd_krw_webuy,
    fa.ex_pkg_official AS origin_usd_krw_package,
    fa.ex_pkg_system AS usd_krw_package
FROM final_aggregated fa
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b ON fa.country_code = b2b.country_code
WHERE fa.package_type IN ('CONSOLE', 'REPACK') 
ORDER BY fa.parent_id ASC;