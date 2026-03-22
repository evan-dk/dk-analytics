/*
================================================================================
  Case 2 수익 분석 쿼리
================================================================================
  목적: 구매대행 서비스(WeBuy)의 SINGLE 패키지에 대한 수익 계산 및 상세 정보 조회
  
  주요 계산:
  - 상품 가격, 국내배송비, 핸들링피, PG 수수료를 합산
  - DK 수수료, 부가세 환급 추가
  - 국내배송비, PG 수수료, 업체 수수료, 할인액, 쿠폰할인 차감
  - 각종 창고 서비스 수수료(storage, photo, repack 등)를 USD → KRW 환율로 변환
  - 배송 비용의 마크업을 포함한 총 수익 계산
  
  필터 조건:
  - package_type: SINGLE (단일 패키지)
  - reference_type: BUY_REQUEST 또는 BUYFORME (구매대행 요청)
  - last_status: CANCELED 제외
  
  데이터 출처:
  - wam: webuy_all_market (구매대행 마켓 정보)
  - spn: shipment_package_new (배송 패키지 정보)
  - t: transaction (거래 정보)
  - spfn: shipment_package_fee_new (패키지 수수료 정보)
  - sc: shipping_cost (배송 비용)
  - cust: customer (고객 정보)
  - b2b: country_code_b2b (국가 코드)
  - brud: buy_request_url_domain (구매 요청 URL 도메인)
  - c: currency (환율 정보)
================================================================================
*/

SELECT
  -- ============================================================================
  -- 기본 식별 정보
  -- ============================================================================
  
  -- 고객의 Suite 번호 (첫 번째 컬럼으로 배치)
  cust.suite_number,
  
  -- 마켓 ID (구매대행 서비스 스토어의 아이디)
  -- 1: OTHER, 2: BUNJANG, 3: MERCARI 등
  wam.market_id,
  
  -- 출고일 (KST 기준)
  DATE(spn.ship_at_kst) AS ship_date_kst,
  
  -- 구매대행 상품 결제일 (UTC 기준)
  DATE(t.trans_at_utc) AS trans_at_utc_webuy,
  
  -- 배송비 결제일 (UTC 기준, 패키지 단위)
  DATE(spn.trans_at_utc) AS trans_at_utc_package,
  
  -- 패키지 고유 ID
  -- 패키지 타입이 CONSOLE, REPACK인 경우 상위 패키지 ID가 된다
  wam.package_id,
  
  -- 하위 패키지 ID
  -- SINGLE 패키지의 경우 NULL, CONSOLE/REPACK의 경우 하위 패키지 ID
  spn.inner_package_id,
  
  -- 수익 케이스 구분자 (Case 2 = 2)
  2 AS profit_case,
  
  -- ============================================================================
  -- 상품 및 판매자 정보
  -- ============================================================================
  
  -- 패키지 판매자 정보 (원본)
  -- 정형화되어 있지 않고 값 입력이 선택 사항이어서 값이 없을 수도 있다
  spn.package_merchant,
  
  -- 패키지 판매자 정보 (정제)
  -- 특정 market_id의 경우 도메인 파싱 결과 사용, 그 외는 원본 사용
  CASE 
    WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30) 
      THEN brud.parsed_domain 
    ELSE spn.package_merchant 
  END AS package_merchant_clean,

  -- 상품명 (customs_item_description 우선, 없으면 item_description)
  COALESCE(wam.customs_item_description, wam.item_description) AS product_name,
  
  -- 상품 카테고리
  wam.customs_category AS product_category,

  -- ============================================================================
  -- 패키지 기본 정보
  -- ============================================================================
  
  -- 주문 유형
  -- WE_SHIP: 배송 대행, WE_BUY: 구매 대행 등
  spn.order_type,
  
  -- 패키지 타입
  -- SINGLE: 단건, CONSOLE: 콘솔됨, REPACK: 리팩됨, INNER: 하위 패키지
  spn.package_type,

  -- ============================================================================
  -- 배송지 및 배송사 정보
  -- ============================================================================
  
  -- 국가 코드 (ISO 3166-1 alpha-2)
  spn.country_code,
  
  -- 국가명 (영문, B2B 테이블 조인)
  b2b.name_en AS country_name,

  -- 국가명 (한글, B2B 테이블 조인)
  b2b.name_kr AS country_name_kr,

  -- 배송사 (carrier)
  spn.carrier,
  
  -- 배송 서비스 유형
  spn.carrier_service,
  
  -- ============================================================================
  -- 무게 정보
  -- ============================================================================
  
  -- 실제 무게 (kg)
  spn.package_weight,
  
  -- 부피 무게 (kg, 과금 기준)
  spn.dimension_weight,
  
  -- 패키지 개수
  -- 하위 패키지가 몇 개 포함되어 있는지 표시
  spn.package_count,
  
  -- 참조 유형
  -- BUY_REQUEST, BUYFORME 등 패키지가 어떤 방식으로 등록되었는지 확인
  spn.reference_type,

  -- ============================================================================
  -- 매출 상세 (Revenue Details)
  -- ============================================================================
  
  -- 1. 상품 구매 매출
  -- goods_revenue_krw = total_goods_price_krw + (domestic_shipping_price_usd × usd_krw_webuy) + (fee_handling_fee_usd × usd_krw_webuy) + pg_fee_krw + surtax_krw + dk_fee_krw - (discount_value_usd × usd_krw_webuy) - (coupon_discount_value_usd × usd_krw_webuy)
  -- goods_revenue_usd = total_goods_price_usd + domestic_shipping_price_usd + fee_handling_fee_usd + pg_fee_usd + surtax_usd + dk_fee_usd - discount_value_usd - coupon_discount_value_usd
  ROUND(
    (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0))
    + (COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450)) -- 국내배송비 포함 (매출 기준)
    + (COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450))
    + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 ELSE 0 END, 0)
    + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 ELSE 0 END)
    + ROUND(CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 ELSE 0 END, 0)
    - (COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))
    - (COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))
  , 0) AS goods_revenue_krw,
  
  ROUND(
    (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450))
    + COALESCE(wam.fee_domestic_shipping_price_usd, 0) -- 국내배송비 포함 (매출 기준)
    + COALESCE(wam.fee_handling_fee_usd, 0)
    + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21) AND DATE(t.trans_at_utc) >= '2025-12-09' THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (2,21,22) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END)
    + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,20,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END)
    + (CASE WHEN wam.market_id IN (1,4,5,6,7,8,9,10,11,12,13,14,18,19,26,27,28,29,30) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (3,24,33) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id IN (20,34) THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450) WHEN wam.market_id = 22 THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450) ELSE 0 END)
    - (COALESCE(t.discount_value, 0))
    - (COALESCE(t.coupon_discount_value, 0))
  , 2) AS goods_revenue_usd,

  -- 2. 창고 옵션 매출
  -- warehouse_revenue_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
  -- warehouse_revenue_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
  ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS warehouse_revenue_krw,
  ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)), 2) AS warehouse_revenue_usd,

  -- 3. 배송비 매출
  -- shipping_revenue_krw = shipping_fee_usd × usd_krw_package
  -- shipping_revenue_usd = shipping_fee_usd
  ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS shipping_revenue_krw,
  COALESCE(spfn.shipping_fee, 0) AS shipping_revenue_usd,

  -- [핵심 지표] 총 수익
  -- profit_krw = goods_profit_krw + warehouse_profit_krw + shipping_profit_krw
  ROUND(
    (
      -- --------------------------------------------------------------------
      -- 1. [상품 가격 × 수량 + 국내배송비 + 핸들링피 + PG 수수료]
      -- --------------------------------------------------------------------
      -- 고객이 지불한 총 상품 금액
      (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0))

      -- 상품 구매 원가 차감 (DK 실제 지출 비용)
      - (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0))

      -- 국내 배송비 (시스템 환율 적용)
      + (COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450))
      
      -- 핸들링 수수료 (시스템 환율 적용)
      + (COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450))
      
      -- --------------------------------------------------------------------
      -- 2. + DK 수수료 (market_id별 차등 적용)
      -- --------------------------------------------------------------------
      -- market_id 1,4,5... : 5%
      -- market_id 3,24,33 : 8%
      -- market_id 20,34 : 7%
      -- market_id 22 : 7.18%
      + ROUND(
          CASE 
            WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30) 
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
            WHEN wam.market_id IN (3, 24, 33)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
            WHEN wam.market_id IN (20, 34)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07
            WHEN wam.market_id = 22 
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
            ELSE 0 
          END, 0)
      -- --------------------------------------------------------------------
      -- 3. - 국내배송비 차감
      -- --------------------------------------------------------------------
      -- 위에서 더한 국내배송비를 다시 차감 (실제 비용이므로)
      - (COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.usd_krw, 1450))

      -- --------------------------------------------------------------------
      -- 4. - 업체 수수료 차감 (실제 비용이므로)
      -- --------------------------------------------------------------------
      - ROUND(
          CASE
            WHEN wam.market_id = 19
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
            WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218
            WHEN wam.market_id IN (2, 21)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
            ELSE 0
          END, 0)

      -- --------------------------------------------------------------------
      -- 6. - DK 할인 (상품 할인 + 쿠폰 할인)
      -- --------------------------------------------------------------------
      -- 상품 할인 금액 (시스템 환율 적용)
      - (COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))

      -- 쿠폰 할인 금액 (시스템 환율 적용)
      - (COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))

      -- --------------------------------------------------------------------
      -- 7. + 창고 서비스 수수료 (시스템 환율 적용)
      -- --------------------------------------------------------------------
      -- 창고 보관비
      + (COALESCE(spfn.storage_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 사진 서비스 비용
      + (COALESCE(spfn.request_photo_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 콘솔&리팩 비용
      + (COALESCE(spfn.repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 에어 쿠션 비용
      + (COALESCE(spfn.bubble_wrap_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 진공 포장 비용
      + (COALESCE(spfn.vacuum_repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 플라스틱 박스 비용
      + (COALESCE(spfn.plasticbox_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 지관통 제거 비용
      + (COALESCE(spfn.remove_papertube_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 인클루전 비용
      + (COALESCE(spfn.inclusion_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 구매대행 추가 비용
      + (COALESCE(spfn.bfm_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 입고 수수료
      + (COALESCE(spfn.receiving_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- 배송대행 추가 비용
      + (COALESCE(spfn.package_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450))
      
      -- --------------------------------------------------------------------
      -- 8. + 배송 마크업 (고객 지불 - 원가)
      -- --------------------------------------------------------------------
      + sc_u.marked_up_cost  -- 배송비 마크업 (NULL이면 profit도 NULL → 6월 이전 데이터 제외)
    )

    -- ------------------------------------------------------------------------
    -- 9. + 부가세 환급 (특정 market_id만 해당)
    -- ------------------------------------------------------------------------
    -- market_id 1,4,5... : 10% 환급
    + (CASE 
        WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30) 
          THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1
        ELSE 0 
      END)
  , 0) AS profit_krw,

  -- 총 수익 (USD)
  -- profit_usd = goods_profit_usd + warehouse_profit_usd + shipping_profit_usd
  ROUND(
    (
      (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450))

      -- 상품 구매 원가 차감 (DK 실제 지출 비용)
      - (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450))

      + COALESCE(wam.fee_domestic_shipping_price_usd, 0)  -- 국내배송비 포함 (매출 기준)
      + COALESCE(wam.fee_handling_fee_usd, 0)
      + (
          CASE
            WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
            WHEN wam.market_id IN (3, 24, 33)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
            WHEN wam.market_id IN (20, 34)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07
            WHEN wam.market_id = 22
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
            ELSE 0
          END / COALESCE(c_u_t.usd_krw, 1450)
        )
      - (COALESCE(wam.fee_domestic_shipping_price_usd, 0))  -- 국내배송비 차감
      - (
          CASE
            WHEN wam.market_id = 19
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
            WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218
            WHEN wam.market_id IN (2, 21)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
            ELSE 0
          END / COALESCE(c_u_t.usd_krw, 1450)
        )  -- 업체 수수료 차감
      - COALESCE(t.discount_value, 0)
      - COALESCE(t.coupon_discount_value, 0)
      + COALESCE(spfn.storage_fee, 0)
      + COALESCE(spfn.request_photo_fee, 0)
      + COALESCE(spfn.repack_fee, 0)
      + COALESCE(spfn.bubble_wrap_fee, 0)
      + COALESCE(spfn.vacuum_repack_fee, 0)
      + COALESCE(spfn.plasticbox_fee, 0)
      + COALESCE(spfn.remove_papertube_fee, 0)
      + COALESCE(spfn.inclusion_fee, 0)
      + COALESCE(spfn.bfm_extra_fee, 0)
      + COALESCE(spfn.receiving_fee, 0)
      + COALESCE(spfn.package_extra_fee, 0)
      + (sc_u.marked_up_cost / COALESCE(c_u_spn.usd_krw, 1450))  -- 배송비 마크업 (NULL이면 profit도 NULL → 6월 이전 데이터 제외)
      + (
          CASE
            WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30)
              THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1
            ELSE 0
          END / COALESCE(c_u_t.usd_krw, 1450)
        )
    ), 2) AS profit_usd,

  -- 구매대행 수익
  -- goods_profit_krw = total_goods_price_krw + (fee_handling_fee_usd × usd_krw_webuy) + dk_fee_krw - business_fee_krw - (discount_value_usd × usd_krw_webuy) - (coupon_discount_value_usd × usd_krw_webuy) + surtax_krw
  -- goods_profit_usd = total_goods_price_usd + fee_handling_fee_usd + dk_fee_usd - business_fee_usd - discount_value_usd - coupon_discount_value_usd + surtax_usd
  ROUND(
    (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0))

    -- 상품 구매 원가 차감 (DK 실제 지출 비용)
    - (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0))

    + (COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.usd_krw, 1450))
    + ROUND(
        CASE
          WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
          WHEN wam.market_id IN (3, 24, 33)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
          WHEN wam.market_id IN (20, 34)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07
          WHEN wam.market_id = 22
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
          ELSE 0
        END, 0)
    - ROUND(
        CASE
          WHEN wam.market_id = 19
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
          WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218
          WHEN wam.market_id IN (2, 21)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
          ELSE 0
        END, 0)
    - (COALESCE(t.discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))
    - (COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.usd_krw, 1450))
    + (CASE
        WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30)
          THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1
        ELSE 0
      END)
  , 0) AS goods_profit_krw,
  ROUND(
    (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450))

    -- 상품 구매 원가 차감 (DK 실제 지출 비용)
    - (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0) / COALESCE(c_u_t.usd_krw, 1450))

    + COALESCE(wam.fee_handling_fee_usd, 0)
    + (
        CASE
          WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
          WHEN wam.market_id IN (3, 24, 33)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
          WHEN wam.market_id IN (20, 34)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07
          WHEN wam.market_id = 22
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
          ELSE 0
        END / COALESCE(c_u_t.usd_krw, 1450))
    - (
        CASE
          WHEN wam.market_id = 19
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
          WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218
          WHEN wam.market_id IN (2, 21)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
          ELSE 0
        END / COALESCE(c_u_t.usd_krw, 1450))
    - COALESCE(t.discount_value, 0)
    - COALESCE(t.coupon_discount_value, 0)
    + (
        CASE
          WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30)
            THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1
          ELSE 0
        END / COALESCE(c_u_t.usd_krw, 1450))
  , 2) AS goods_profit_usd,

  -- 창고 수익
  -- warehouse_profit_krw = (storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd) × usd_krw_package
  -- warehouse_profit_usd = storage_fee_usd + request_photo_fee_usd + repack_fee_usd + bubble_wrap_fee_usd + vacuum_repack_fee_usd + plasticbox_fee_usd + remove_papertube_fee_usd + inclusion_fee_usd + bfm_extra_fee_usd + receiving_fee_usd + package_extra_fee_usd
  ROUND(
    (COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u_spn.usd_krw, 1450),
  0) AS warehouse_profit_krw,
  ROUND(
    (COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)),
  2) AS warehouse_profit_usd,

  -- 배송 수익
  -- shipping_profit_krw = marked_up_cost_krw
  -- shipping_profit_usd = marked_up_cost_usd
  ROUND(sc_u.marked_up_cost, 0) AS shipping_profit_krw,
  ROUND(sc_u.marked_up_cost / COALESCE(c_u_spn.usd_krw, 1450), 2) AS shipping_profit_usd,

  -- ============================================================================
  -- 상품 및 수수료 상세 정보
  -- ============================================================================
  
  -- 상품 단가 (KRW/USD)
  COALESCE(wam.fee_unit_price_krw, 0) AS fee_unit_price_krw,
  COALESCE(wam.fee_unit_price_usd, 0) AS fee_unit_price_usd, -- 원본 값 (USD)
  
  -- 주문 수량
  COALESCE(wam.quotation_quantity, 0) AS quotation_quantity,
  
  -- 총 상품 금액 (단가 × 수량)
  (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) AS total_goods_price_krw,
  ROUND((COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) / COALESCE(c_u_t.usd_krw, 1450), 2) AS total_goods_price_usd, -- 원본 환산 (USD)
  
  -- 핸들링 수수료 (KRW)
  ROUND(COALESCE(wam.fee_handling_fee_usd, 0) * COALESCE(c_u_t.origin_usd_krw, 1450), 0) AS fee_handling_fee_krw,
  COALESCE(wam.fee_handling_fee_usd, 0) AS fee_handling_fee_usd, -- 원본 값 (USD)
  
  -- 국내 배송비 (KRW)
  ROUND(COALESCE(wam.fee_domestic_shipping_price_usd, 0) * COALESCE(c_u_t.origin_usd_krw, 1450), 0) AS domestic_shipping_price_krw,
  COALESCE(wam.fee_domestic_shipping_price_usd, 0) AS domestic_shipping_price_usd, -- 원본 값 (USD)
  
  -- ============================================================================
  -- 세금 및 수수료 상세
  -- ============================================================================
  
  -- 부가세 환급액 (KRW)
  -- 특정 market_id에 대해 상품 금액의 10%
  ROUND(
    CASE 
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30) 
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1
      ELSE 0 
    END, 0
  ) AS surtax_krw,
  ROUND(
    CASE 
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30) 
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1 / COALESCE(c_u_t.usd_krw, 1450)
      ELSE 0 
    END, 2
  ) AS surtax_usd,

  -- 업체 수수료 (KRW)
  -- market_id 19: 5%, market_id 2,21: 7.18% (2025-12-08 이후 2.18%)
  ROUND(
    CASE
      WHEN wam.market_id = 19
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
      WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218
      WHEN wam.market_id IN (2, 21)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
      ELSE 0
    END, 0
  ) AS business_fee_krw,
  ROUND(
    CASE
      WHEN wam.market_id = 19
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-08'
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0218 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (2, 21)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450)
      ELSE 0
    END, 2
  ) AS business_fee_usd,

  -- PG 수수료 (KRW)
  -- market_id별 차등 적용 (8%, 7.78%, 11.64%)
  ROUND(
    CASE
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
      WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-09'
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164
      WHEN wam.market_id IN (2, 21, 22)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778
      ELSE 0
    END, 0
  ) AS pg_fee_krw,
  ROUND(
    CASE
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20, 26, 27, 28, 29, 30)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (2, 21) AND DATE(t.trans_at_utc) >= '2025-12-09'
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.1164 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (2, 21, 22)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0778 / COALESCE(c_u_t.usd_krw, 1450)
      ELSE 0
    END, 2
  ) AS pg_fee_usd,

  -- DK 수수료 (KRW)
  -- market_id별 차등 적용 (5%, 7%, 7.18%, 8%)
  ROUND(
    CASE 
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05
      WHEN wam.market_id IN (3, 24, 33)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08
      WHEN wam.market_id IN (20, 34)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07
      WHEN wam.market_id = 22
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718
      ELSE 0
    END, 0
  ) AS dk_fee_krw,
  ROUND(
    CASE
      WHEN wam.market_id IN (1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19, 26, 27, 28, 29, 30)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.05 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (3, 24, 33)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.08 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id IN (20, 34)
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.07 / COALESCE(c_u_t.usd_krw, 1450)
      WHEN wam.market_id = 22
        THEN (COALESCE(wam.fee_unit_price_krw, 0) * COALESCE(wam.quotation_quantity, 0)) * 0.0718 / COALESCE(c_u_t.usd_krw, 1450)
      ELSE 0
    END, 2
  ) AS dk_fee_usd,

  -- ============================================================================
  -- 할인 정보
  -- ============================================================================
  
  -- 상품 할인 금액 (KRW)
  ROUND(COALESCE(t.discount_value, 0) * COALESCE(c_u_t.origin_usd_krw, 1450), 0) AS discount_value_krw,
  COALESCE(t.discount_value, 0) AS discount_value_usd, -- 원본 값 (USD)
  
  -- 쿠폰 할인 금액 (KRW)
  ROUND(COALESCE(t.coupon_discount_value, 0) * COALESCE(c_u_t.origin_usd_krw, 1450), 0) AS coupon_discount_value_krw,
  COALESCE(t.coupon_discount_value, 0) AS coupon_discount_value_usd, -- 원본 값 (USD)

  -- ============================================================================
  -- 창고 서비스 수수료 상세 (USD → KRW 변환)
  -- ============================================================================
  
  -- 창고 보관비 (USD/KRW)
  spfn.storage_fee AS storage_fee_usd,
  ROUND(COALESCE(spfn.storage_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS storage_fee_krw,
  
  -- 사진 촬영 요청료 (USD/KRW)
  spfn.request_photo_fee AS request_photo_fee_usd,
  ROUND(COALESCE(spfn.request_photo_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS request_photo_fee_krw,
  
  -- 콘솔&리팩 비용 (USD/KRW)
  spfn.repack_fee AS repack_fee_usd,
  ROUND(COALESCE(spfn.repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS repack_fee_krw,
  
  -- 에어 쿠션 비용 (USD/KRW)
  spfn.bubble_wrap_fee AS bubble_wrap_fee_usd,
  ROUND(COALESCE(spfn.bubble_wrap_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
  
  -- 진공 포장 비용 (USD/KRW)
  spfn.vacuum_repack_fee AS vacuum_repack_fee_usd,
  ROUND(COALESCE(spfn.vacuum_repack_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
  
  -- 플라스틱 박스 사용료 (USD/KRW)
  spfn.plasticbox_fee AS plasticbox_fee_usd,
  ROUND(COALESCE(spfn.plasticbox_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS plasticbox_fee_krw,
  
  -- 지관통 제거 비용 (USD/KRW)
  spfn.remove_papertube_fee AS remove_papertube_fee_usd,
  ROUND(COALESCE(spfn.remove_papertube_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
  
  -- 인클루전 비용 (USD/KRW)
  spfn.inclusion_fee AS inclusion_fee_usd,
  ROUND(COALESCE(spfn.inclusion_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS inclusion_fee_krw,
  
  -- 구매대행 추가 비용 (USD/KRW)
  spfn.bfm_extra_fee AS bfm_extra_fee_usd,
  ROUND(COALESCE(spfn.bfm_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS bfm_extra_fee_krw,

  -- 입고 수수료 (USD/KRW)
  spfn.receiving_fee AS receiving_fee_usd,
  ROUND(COALESCE(spfn.receiving_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS receiving_fee_krw,

  -- 배송대행 추가 비용 (USD/KRW)
  spfn.package_extra_fee AS package_extra_fee_usd,
  ROUND(COALESCE(spfn.package_extra_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS package_extra_fee_krw,

  -- ============================================================================
  -- 배송 비용 정보
  -- ============================================================================
  -- 패키지 수수료 테이블에 기록된 배송비 (USD)
  spfn.shipping_fee AS shipping_fee_usd,
  ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u_spn.usd_krw, 1450), 0) AS shipping_fee_krw,

  -- 고객이 결제해야 할 국제배송비 (KRW/USD)
  ROUND(COALESCE(sc_u.customer_cost, 0), 0) AS customer_cost_krw,
  ROUND(COALESCE(sc_u.customer_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) AS customer_cost_usd,
  
  -- 배송원가 (KRW/USD)
  ROUND(COALESCE(sc_u.original_cost, 0), 0) AS original_cost_krw,
  ROUND(COALESCE(sc_u.original_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) AS original_cost_usd,
  
  -- 서차지 비용 (유류할증료, KRW/USD)
  ROUND(COALESCE(sc_u.fuel_surcharge_cost, 0), 0) AS fuel_surcharge_cost_krw,
  ROUND(COALESCE(sc_u.fuel_surcharge_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) AS fuel_surcharge_cost_usd,
  
  -- 마크업 비용 (고객 지불 - 원가, KRW/USD)
  ROUND(COALESCE(sc_u.marked_up_cost, 0), 0) AS marked_up_cost_krw,
  ROUND(COALESCE(sc_u.marked_up_cost, 0) / COALESCE(c_u_spn.usd_krw, 1450), 2) AS marked_up_cost_usd,

  -- ============================================================================
  -- 환율 정보
  -- ============================================================================
  
  -- 적용된 USD → KRW 환율 (구매대행 거래일 기준)
  COALESCE(c_u_t.origin_usd_krw, 1450) AS origin_usd_krw_webuy,
  COALESCE(c_u_t.usd_krw, 1450) AS usd_krw_webuy,
  
  -- 적용된 USD → KRW 환율 (배송비 거래일 기준)
  COALESCE(c_u_spn.origin_usd_krw, 1450) AS origin_usd_krw_package,
  COALESCE(c_u_spn.usd_krw, 1450) AS usd_krw_package

-- ================================================================================
-- FROM 절: 메인 테이블 및 조인
-- ================================================================================

FROM `da-project-472406.data_warehouse.shipment_package_new` spn

-- 배송 비용 테이블 (중복 제거 후 조인)
LEFT JOIN (
    -- ----------------------------------------------------------------------------
    -- 배송 비용 테이블 (중복 제거)
    -- ----------------------------------------------------------------------------
    -- ROW_NUMBER를 사용하여 tracking_number별로 original_cost가 가장 높은 행만 선택
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
    FROM `da-project-472406.data_warehouse.shipping_cost`
) sc_u
    ON TRIM(spn.track_number) = TRIM(sc_u.tracking_number)  -- 송장번호 매칭 (공백 제거)
    AND sc_u.rn = 1  -- 중복 제거된 행만 사용

-- 구매대행 전체 마켓 정보 테이블
INNER JOIN `da-project-472406.data_warehouse.webuy_all_market` wam
    ON spn.reference_id = wam.request_id  -- 요청 ID로 매칭
    AND spn.package_id = wam.package_id  -- 패키지 ID로 매칭

-- 거래 정보 테이블
INNER JOIN `da-project-472406.data_warehouse.transaction` t
    ON wam.request_id = t.reference_id  -- 요청 ID로 매칭
    AND t.pay_service IN ('NewWeBuy', 'BUY_REQUEST')  -- 구매대행 서비스만
    AND t.transaction_status = 'CNF'  -- 확정된 거래만

-- 구매 요청 URL 도메인 테이블 (판매자 정보 정제용)
LEFT JOIN `da-project-472406.data_warehouse.buy_request_url_domain` brud
    ON wam.request_id = brud.buy_request_id

-- 고객 정보 테이블 (suite_number 조회용)
LEFT JOIN `da-project-472406.data_warehouse.customer` cust
    ON wam.customer_id = cust.customer_id

-- 국가 코드 B2B 테이블 (국가명 조회용)
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b
    ON spn.country_code = b2b.country_code

-- 패키지 수수료 정보 테이블
LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_new` spfn
    ON spn.package_id = spfn.package_id

-- 환율 정보 테이블 (구매대행 거래일 기준, 중복 제거)
LEFT JOIN (
    -- 날짜별로 가장 높은 환율 선택
    SELECT currency_date, 
           origin_usd_krw,
           usd_krw,
           ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) as rn
    FROM `da-project-472406.data_warehouse.currency`
) c_u_t 
    ON DATE(t.trans_at_utc) = c_u_t.currency_date  -- 구매대행 거래일 기준
    AND c_u_t.rn = 1  -- 중복 제거된 행만 사용

-- 환율 정보 테이블 (배송비 거래일 기준, 중복 제거)
LEFT JOIN (
    -- 날짜별로 가장 높은 환율 선택
    SELECT currency_date, 
           origin_usd_krw,
           usd_krw,
           ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) as rn
    FROM `da-project-472406.data_warehouse.currency`
) c_u_spn 
    ON DATE(spn.trans_at_utc) = c_u_spn.currency_date  -- 배송비 거래일 기준
    AND c_u_spn.rn = 1  -- 중복 제거된 행만 사용

-- ================================================================================
-- WHERE 절: 필터 조건
-- ================================================================================

WHERE 
    -- 취소되지 않은 주문만 포함
    wam.last_status != 'CANCELED'
    
    -- 패키지 타입: SINGLE (단일 패키지만, 묶음 배송 제외)
    -- SINGLE: 단건, CONSOLE: 콘솔됨, REPACK: 리팩됨, INNER: 하위 패키지
    AND spn.package_type = 'SINGLE'
    
    -- 참조 유형: BUY_REQUEST 또는 BUYFORME
    -- 구매대행 요청으로 등록된 패키지가 포함
    AND spn.reference_type IN ('BUY_REQUEST', 'BUYFORME')
    
    -- 날짜 필터: 2025-01-01 이후 데이터 조회 (UTC 기준)
    AND DATE(spn.trans_at_utc) >= '2025-01-01';
