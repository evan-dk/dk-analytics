/*
================================================================================
  Case 1 수익 분석 쿼리
================================================================================
  목적: WE_SHIP 주문의 SINGLE 패키지에 대한 수익 계산 및 상세 정보 조회
  
  주요 계산:
  - 각종 수수료(storage, photo, repack 등)를 USD → KRW 환율로 변환
  - 배송 비용의 마크업을 포함한 총 수익 계산
  
  필터 조건:
  - package_type: SINGLE (단일 패키지)
  - reference_type: ASN (배송 대행)
  
  데이터 출처:
  - spn: shipment_package_new
  - spfn: shipment_package_fee_new
  - sc: shipping_cost
  - cust: customer
  - b2b: country_code_b2b
  - c: currency
================================================================================
*/

SELECT 
    -- ============================================================================
    -- 기본 식별 정보
    -- ============================================================================
    
    -- 고객의 Suite 번호 (첫 번째 컬럼으로 배치)
    cust.suite_number,
    
    -- 배송 날짜 (KST 기준)
    DATE(spn_u.ship_at_kst) AS ship_date_kst, 
    
    -- 거래 날짜 (UTC 기준, 패키지 단위)
    DATE(spn_u.trans_at_utc) AS trans_date_utc_package, 
    
    -- 패키지 고유 ID
    -- 패키지 타입이 CONSOLE, REPACK인 경우 상위 패키지 ID가 된다
    spn_u.package_id, 
    
    -- 하위 패키지 ID
    -- SINGLE 패키지의 경우 NULL, CONSOLE/REPACK의 경우 하위 패키지 ID
    spn_u.inner_package_id,
    
    -- 수익 케이스 구분자 (Case 1 = 1)
    1 AS profit_case,
    
    -- ============================================================================
    -- 패키지 기본 정보
    -- ============================================================================
    
    -- 패키지 판매자 정보
    -- 정형화되어 있지 않고 값 입력이 선택 사항이어서 값이 없을 수도 있다
    spn_u.package_merchant,
    
    -- 주문 유형 (WE_SHIP: 배송 대행)
    -- 패키지가 어느 서비스를 통해 들어왔는지 확인할 수 있다
    spn_u.order_type, 
    
    -- 패키지 타입
    -- SINGLE: 단건, CONSOLE: 콘솔됨, REPACK: 리팩됨, INNER: 하위 패키지
    spn_u.package_type, 
    
    -- ============================================================================
    -- 배송지 및 배송사 정보
    -- ============================================================================
    
    -- 국가 코드 (ISO 3166-1 alpha-2)
    spn_u.country_code,
    
    -- 국가명 (영문, B2B 테이블 조인)
    b2b.name_en AS country_name,

    -- 국가명 (한글, B2B 테이블 조인)
    b2b.name_kr AS country_name_kr,
    
    -- 배송사 (carrier)
    spn_u.carrier,
    
    -- 배송 서비스 유형
    spn_u.carrier_service,
    
    -- ============================================================================
    -- 무게 정보
    -- ============================================================================
    
    -- 실제 무게 (kg)
    spn_u.package_weight,
    
    -- 부피 무게 (kg, 과금 기준)
    spn_u.dimension_weight,
    
    -- 패키지 개수
    -- 하위 패키지가 몇 개 포함되어 있는지 표시
    spn_u.package_count,
    
    -- 참조 유형
    -- ASN (Advanced Shipping Notice): 사전 배송 통지
    -- 패키지가 어떤 방식으로 등록되었는지 확인
    spn_u.reference_type,

    -- ============================================================================
    -- 매출 상세 (Revenue Details)
    -- ============================================================================
    
    -- 1. 상품 구매 매출 (Case 1은 ASN 배송대행이므로 상품 매출 0)
    0 AS goods_revenue_krw,
    0 AS goods_revenue_usd,
    
    -- 2. 창고 옵션 매출 (11가지 창고 서비스 수수료 합계)
    ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u.usd_krw, 1450), 0) AS warehouse_revenue_krw,
    ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0)), 2) AS warehouse_revenue_usd,
    
    -- 3. 배송비 매출 (shipment_package_fee_new.shipping_fee 컬럼 기준)
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS shipping_revenue_krw,
    COALESCE(spfn.shipping_fee, 0) AS shipping_revenue_usd,
    -- 총 매출 합계 (revenue_krw = warehouse_revenue_krw + shipping_revenue_krw)
    ROUND((COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0) + COALESCE(spfn.shipping_fee, 0)) * COALESCE(c_u.usd_krw, 1450), 0) AS revenue_krw,
    ROUND(COALESCE(spfn.storage_fee, 0) + COALESCE(spfn.request_photo_fee, 0) + COALESCE(spfn.repack_fee, 0) + COALESCE(spfn.bubble_wrap_fee, 0) + COALESCE(spfn.vacuum_repack_fee, 0) + COALESCE(spfn.plasticbox_fee, 0) + COALESCE(spfn.remove_papertube_fee, 0) + COALESCE(spfn.inclusion_fee, 0) + COALESCE(spfn.bfm_extra_fee, 0) + COALESCE(spfn.receiving_fee, 0) + COALESCE(spfn.package_extra_fee, 0) + COALESCE(spfn.shipping_fee, 0), 2) AS revenue_usd,


    -- [핵심 지표] 총 수익 (수수료 수익 + 배송 마진)
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
       COALESCE(spfn.package_extra_fee, 0)) * COALESCE(c_u.usd_krw, 1450)  -- 시스템 환율 적용 (실질 수익 기준)
      + sc_u.marked_up_cost,  -- 배송비 마크업 (NULL이면 profit도 NULL → 6월 이전 데이터 제외)
    0) AS profit_krw,
    
    -- 총 수익 (USD)
    -- 공식: 각종 수수료(USD) + 배송비 마크업(KRW) / 시스템 환율
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
       COALESCE(spfn.package_extra_fee, 0))
      + (sc_u.marked_up_cost / COALESCE(c_u.usd_krw, 1450)),
    2) AS profit_usd,

    -- 구매 수익 (Case 1은 ASN 배송대행이므로 0)
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

    -- ============================================================================
    -- 각종 수수료 상세 (USD → KRW 변환)
    -- ============================================================================
    
    -- 창고 보관비 (USD/KRW)
    -- 입고일로부터 45일이 지날 때마다 1달러씩 부과
    spfn.storage_fee AS storage_fee_usd,
    ROUND(COALESCE(spfn.storage_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS storage_fee_krw,
    
    -- 사진 촬영 요청료 (USD/KRW)
    spfn.request_photo_fee AS request_photo_fee_usd,
    ROUND(COALESCE(spfn.request_photo_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS request_photo_fee_krw,
    
    -- 콘솔&리팩 비용 (USD/KRW)
    -- 하위 패키지 개수 × 리팩 비용
    spfn.repack_fee AS repack_fee_usd,
    ROUND(COALESCE(spfn.repack_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS repack_fee_krw,
    
    -- 에어 쿠션 비용 (USD/KRW)
    spfn.bubble_wrap_fee AS bubble_wrap_fee_usd,
    ROUND(COALESCE(spfn.bubble_wrap_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS bubble_wrap_fee_krw,
    
    -- 진공 포장 비용 (USD/KRW)
    spfn.vacuum_repack_fee AS vacuum_repack_fee_usd,
    ROUND(COALESCE(spfn.vacuum_repack_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS vacuum_repack_fee_krw,
    
    -- 플라스틱 박스 사용료 (USD/KRW)
    spfn.plasticbox_fee AS plasticbox_fee_usd,
    ROUND(COALESCE(spfn.plasticbox_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS plasticbox_fee_krw,
    
    -- 지관통 제거 비용 (USD/KRW)
    spfn.remove_papertube_fee AS remove_papertube_fee_usd,
    ROUND(COALESCE(spfn.remove_papertube_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS remove_papertube_fee_krw,
    
    -- 인클루전 비용 (USD/KRW)
    spfn.inclusion_fee AS inclusion_fee_usd,
    ROUND(COALESCE(spfn.inclusion_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS inclusion_fee_krw,
    
    -- 구매대행 추가 비용 (USD/KRW)
    spfn.bfm_extra_fee AS bfm_extra_fee_usd,
    ROUND(COALESCE(spfn.bfm_extra_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS bfm_extra_fee_krw,
    
    -- 입고 수수료 (USD/KRW)
    spfn.receiving_fee AS receiving_fee_usd,
    ROUND(COALESCE(spfn.receiving_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS receiving_fee_krw,
    
    -- 배송대행 추가 비용 (USD/KRW)
    spfn.package_extra_fee AS package_extra_fee_usd,
    ROUND(COALESCE(spfn.package_extra_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS package_extra_fee_krw,

    -- ============================================================================
    -- 배송 비용 정보
    -- ============================================================================
    
    -- 패키지 수수료 테이블에 기록된 배송비 (주로 USD)
    spfn.shipping_fee AS shipping_fee_usd,
    ROUND(COALESCE(spfn.shipping_fee, 0) * COALESCE(c_u.usd_krw, 1450), 0) AS shipping_fee_krw,

    -- 고객이 결제해야 할 국제배송비 (KRW/USD)
    ROUND(sc_u.customer_cost, 0) AS customer_cost_krw,
    ROUND(sc_u.customer_cost / COALESCE(c_u.usd_krw, 1450), 2) AS customer_cost_usd,
    
    -- 배송원가 (KRW/USD)
    ROUND(sc_u.original_cost, 0) AS original_cost_krw,
    ROUND(sc_u.original_cost / COALESCE(c_u.usd_krw, 1450), 2) AS original_cost_usd,
    
    -- 서차지 비용 (유류할증료, KRW/USD)
    ROUND(sc_u.fuel_surcharge_cost, 0) AS fuel_surcharge_cost_krw,
    ROUND(sc_u.fuel_surcharge_cost / COALESCE(c_u.usd_krw, 1450), 2) AS fuel_surcharge_cost_usd,
    
    -- 마크업 비용 (고객 지불 - 원가, KRW/USD)
    ROUND(sc_u.marked_up_cost, 0) AS marked_up_cost_krw,
    ROUND(sc_u.marked_up_cost / COALESCE(c_u.usd_krw, 1450), 2) AS marked_up_cost_usd,
    
    -- 적용된 USD → KRW 환율 (패키지 거래일 기준)
    -- 배송비 서비스에 대한 고시환율
    COALESCE(c_u.origin_usd_krw, 1450) AS origin_usd_krw_package,
    -- 시스템 환율 (실질 수익 계산용)
    COALESCE(c_u.usd_krw, 1450) AS usd_krw_package

-- ================================================================================
-- FROM 절: 메인 테이블 및 조인
-- ================================================================================

FROM `da-project-472406.data_warehouse.shipment_package_new` spn_u

-- 배송 비용 테이블 (중복 제거 후 조인)
LEFT JOIN (
    -- ----------------------------------------------------------------------------
    -- 배송 비용 테이블 (중복 제거)
    -- ----------------------------------------------------------------------------
    -- ROW_NUMBER를 사용하여 tracking_number별로 original_cost가 가장 높은 행만 선택
    -- (동일 송장번호에 여러 비용 레코드가 있을 경우 최고 비용 기준)
    -- ----------------------------------------------------------------------------
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY tracking_number ORDER BY original_cost DESC) as rn
    FROM `da-project-472406.data_warehouse.shipping_cost`
) sc_u
    ON TRIM(spn_u.track_number) = TRIM(sc_u.tracking_number)  -- 송장번호 매칭 (공백 제거)
    AND sc_u.rn = 1  -- 중복 제거된 행만 사용

-- 패키지 수수료 정보 테이블
LEFT JOIN `da-project-472406.data_warehouse.shipment_package_fee_new` spfn
    ON spn_u.package_id = spfn.package_id  -- 패키지 ID로 매칭

-- 고객 정보 테이블 (suite_number 조회용)
LEFT JOIN `da-project-472406.data_warehouse.customer` cust
    ON spn_u.customer_id = cust.customer_id  -- 고객 ID로 매칭

-- 국가 코드 B2B 테이블 (국가명 조회용)
LEFT JOIN `da-project-472406.data_warehouse.country_code_b2b` b2b
    ON spn_u.country_code = b2b.country_code  -- 국가 코드로 매칭

-- 환율 정보 테이블 (중복 제거)
LEFT JOIN (
    -- 날짜별로 가장 높은 환율 선택
    SELECT currency_date, 
           origin_usd_krw, 
           usd_krw,
           ROW_NUMBER() OVER(PARTITION BY currency_date ORDER BY origin_usd_krw DESC) as rn
    FROM `da-project-472406.data_warehouse.currency`
) c_u 
    ON DATE(spn_u.trans_at_utc) = c_u.currency_date  -- 거래일 기준 환율 매칭
    AND c_u.rn = 1  -- 중복 제거된 행만 사용

-- ================================================================================
-- WHERE 절: 필터 조건
-- ================================================================================

WHERE 
    -- 패키지 타입: SINGLE (단일 패키지만, 묶음 배송 제외)
    -- SINGLE: 단건, CONSOLE: 콘솔됨, REPACK: 리팩됨, INNER: 하위 패키지
    spn_u.package_type = 'SINGLE'
    
    -- 참조 유형: ASN (Advanced Shipping Notice, 사전 배송 통지)
    -- 배송 전 미리 통지된 패키지만 포함
    AND spn_u.reference_type = 'ASN'
    
    -- 날짜 필터: 2025-01-01 이후 데이터 조회 (UTC 기준)
    AND DATE(spn_u.trans_at_utc) >= '2025-01-01';
