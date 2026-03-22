"""
BigQuery -> dashboard_data.json 자동 갱신 스크립트

GitHub Actions에서 실행되어 BigQuery에서 Case 1~5 쿼리를 실행하고
profit/dashboard_data.json을 업데이트합니다.

Usage:
    python scripts/update_dashboard_data.py
"""

import os
import json
import decimal
import pandas as pd
import numpy as np
from google.cloud import bigquery


# ============================================================================
# 설정
# ============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SQL_DIR = os.path.join(SCRIPT_DIR, "sql")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "profit", "dashboard_data.json")
COMP_JSON_PATH = os.path.join(PROJECT_ROOT, "profit", "competitor_rate_data.json")

# 분석 기간: 2025-01-01 이후 (상한 없음)

# 관리자 Suite 번호
ADMIN_SUITES = ["Z9996", "Z9997", "Z9998", "Z9999"]

# 창고 수수료 컬럼 (USD/KRW)
WAREHOUSE_FEE_COLS_KRW = [
    "storage_fee_krw", "request_photo_fee_krw", "repack_fee_krw",
    "bubble_wrap_fee_krw", "vacuum_repack_fee_krw", "plasticbox_fee_krw",
    "remove_papertube_fee_krw", "inclusion_fee_krw", "bfm_extra_fee_krw",
]
WAREHOUSE_FEE_COLS_USD = [c.replace("_krw", "_usd") for c in WAREHOUSE_FEE_COLS_KRW]


# ============================================================================
# BigQuery 쿼리 실행
# ============================================================================

def run_queries(client: bigquery.Client) -> dict[int, pd.DataFrame]:
    """Case 1~5 SQL 쿼리를 실행하고 DataFrame dict로 반환"""
    results = {}
    for case_num in range(1, 6):
        sql_path = os.path.join(SQL_DIR, f"case_{case_num}.sql")
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()

        print(f"Running Case {case_num} query...")
        df = client.query(sql).to_dataframe()
        print(f"  -> {len(df)} rows returned")

        # BigQuery는 NUMERIC/BIGNUMERIC을 decimal.Decimal로 반환
        # pandas sum()에서 float와 혼합 시 TypeError 발생하므로 변환
        for col in df.columns:
            if df[col].dtype == object and len(df) > 0:
                sample = df[col].dropna().head(1)
                if len(sample) > 0 and isinstance(sample.iloc[0], decimal.Decimal):
                    df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

        results[case_num] = df
    return results


# ============================================================================
# 컬럼 표준화
# ============================================================================

def _sum_cols(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """DataFrame에서 여러 컬럼의 합계 Series 반환 (없는 컬럼은 0)"""
    total = pd.Series(0, index=df.index, dtype=float)
    for c in cols:
        if c in df.columns:
            total += df[c].fillna(0)
    return total


def standardize_columns(df: pd.DataFrame, case_num: int) -> pd.DataFrame:
    """SQL 출력 컬럼을 prepare_dashboard_data.py가 기대하는 표준 컬럼명으로 변환"""
    df = df.copy()

    # --- warehouse_revenue ---
    if "warehouse_revenue_krw" not in df.columns:
        if "revenue_sto_krw" in df.columns:
            df["warehouse_revenue_krw"] = df["revenue_sto_krw"].fillna(0)
            df["warehouse_revenue_usd"] = df["revenue_sto_usd"].fillna(0)
        else:
            df["warehouse_revenue_krw"] = _sum_cols(df, WAREHOUSE_FEE_COLS_KRW)
            df["warehouse_revenue_usd"] = _sum_cols(df, WAREHOUSE_FEE_COLS_USD)

    # --- shipping_revenue ---
    if "shipping_revenue_krw" not in df.columns:
        if "revenue_shp_krw" in df.columns:
            df["shipping_revenue_krw"] = df["revenue_shp_krw"].fillna(0)
            df["shipping_revenue_usd"] = df["revenue_shp_usd"].fillna(0)
        elif "shipping_fee_krw" in df.columns:
            df["shipping_revenue_krw"] = df["shipping_fee_krw"].fillna(0)
            df["shipping_revenue_usd"] = df.get("shipping_fee", pd.Series(0, index=df.index)).fillna(0)
        else:
            # shipping_fee (USD) * exchange_rate
            rate = df.get("usd_krw_package", pd.Series(1450, index=df.index)).fillna(1450)
            fee = df.get("shipping_fee", pd.Series(0, index=df.index)).fillna(0)
            df["shipping_revenue_krw"] = (fee * rate).round(0)
            df["shipping_revenue_usd"] = fee

    # --- goods_revenue ---
    if "goods_revenue_krw" not in df.columns:
        if "revenue_buy_krw" in df.columns:
            df["goods_revenue_krw"] = df["revenue_buy_krw"].fillna(0)
            df["goods_revenue_usd"] = df["revenue_buy_usd"].fillna(0)
        elif case_num in [1, 3]:
            # WE_SHIP: 구매대행 매출 없음
            df["goods_revenue_krw"] = 0
            df["goods_revenue_usd"] = 0
        else:
            # Case 4, 5 (WeBuy CONSOLE/REPACK)
            buy_cols_krw = [
                "total_goods_price_krw", "domestic_shipping_price_krw",
                "fee_handling_fee_krw", "pg_fee_krw", "dk_fee_krw", "surtax_krw",
            ]
            buy_cols_usd = [c.replace("_krw", "_usd") for c in buy_cols_krw]
            df["goods_revenue_krw"] = _sum_cols(df, buy_cols_krw)
            df["goods_revenue_usd"] = _sum_cols(df, buy_cols_usd)

    # --- ship_date_kst 통일 ---
    if "ship_date_kst" not in df.columns:
        for alt in ["ship_date_kst", "ship_at_kst"]:
            if alt in df.columns:
                df["ship_date_kst"] = df[alt]
                break

    # --- marked_up_cost_krw 보장 ---
    if "marked_up_cost_krw" not in df.columns:
        df["marked_up_cost_krw"] = 0

    # --- origin_usd_krw_package 보장 ---
    if "origin_usd_krw_package" not in df.columns:
        if "usd_krw_package" in df.columns:
            df["origin_usd_krw_package"] = df["usd_krw_package"]
        else:
            df["origin_usd_krw_package"] = 1450

    return df


# ============================================================================
# Dashboard 데이터 가공 (prepare_dashboard_data.py 로직)
# ============================================================================

def build_dashboard_data(case_dfs: dict[int, pd.DataFrame]) -> dict:
    """Case 1~5 DataFrame을 합쳐서 dashboard_data.json 구조로 변환"""

    all_dfs = []
    for case_num, df in case_dfs.items():
        case_name = f"Case {case_num}"
        df = standardize_columns(df, case_num)

        # 날짜 필터 (상한 없음 - 2025-01-01 이후 모두 포함)
        if "ship_date_kst" in df.columns:
            df["ship_date_kst"] = pd.to_datetime(df["ship_date_kst"], errors="coerce")

        # Case 3,4,5 필터
        if case_name in ["Case 3", "Case 4", "Case 5"] and "profit_case" in df.columns:
            df = df[df["profit_case"].astype(str).isin(["3", "4", "5"])].copy()

        # revenue 컬럼 생성
        df["revenue_buy_krw"] = df["goods_revenue_krw"].fillna(0)
        df["revenue_ship_krw"] = df["shipping_revenue_krw"].fillna(0)
        df["revenue_storage_krw"] = df["warehouse_revenue_krw"].fillna(0)
        df["revenue_krw"] = df["revenue_buy_krw"] + df["revenue_ship_krw"] + df["revenue_storage_krw"]

        # profit 컴포넌트 컬럼 보정 (SQL 원본 필드명 그대로 사용)
        for col in ["goods_profit_krw", "warehouse_profit_krw", "shipping_profit_krw",
                    "goods_profit_usd", "warehouse_profit_usd", "shipping_profit_usd"]:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = df[col].fillna(0)

        df["revenue_buy_usd"] = df["goods_revenue_usd"].fillna(0)
        df["revenue_ship_usd"] = df["shipping_revenue_usd"].fillna(0)
        df["revenue_storage_usd"] = df["warehouse_revenue_usd"].fillna(0)
        df["revenue_usd"] = df["revenue_buy_usd"] + df["revenue_ship_usd"] + df["revenue_storage_usd"]

        if "profit_usd" not in df.columns:
            df["profit_usd"] = 0

        df["source_case"] = case_name
        all_dfs.append(df)

    total_df = pd.concat(all_dfs, ignore_index=True)

    # --- 옵션 2: profit_krw가 NULL인 패키지는 컴포넌트도 NaN으로 통일 ---
    # (배송원가 누락 패키지를 Total Profit 기준과 동일하게 컴포넌트에서도 제외)
    # NaN 사용 이유: pandas sum()은 NaN을 자동으로 건너뛰므로 합계 정확성 유지 +
    #               describe().count도 profit_krw와 동일하게 46,954로 일치
    _profit_null = total_df["profit_krw"].isna()
    for _col in ["goods_profit_krw", "warehouse_profit_krw", "shipping_profit_krw",
                 "goods_profit_usd", "warehouse_profit_usd", "shipping_profit_usd"]:
        if _col in total_df.columns:
            total_df.loc[_profit_null, _col] = np.nan
    # -----------------------------------------------------------------------

    total_df["suite_number"] = total_df["suite_number"].fillna("Unknown").astype(str)

    # --- KPI ---
    kpis = {
        "total_profit": int(total_df["profit_krw"].sum()),
        "total_revenue": int(total_df["revenue_krw"].sum()),
        "total_buy_revenue": int(total_df["revenue_buy_krw"].sum()),
        "total_storage_revenue": int(total_df["revenue_storage_krw"].sum()),
        "total_ship_revenue": int(total_df["revenue_ship_krw"].sum()),
        "total_packages": int(total_df["package_id"].nunique()),
        "total_customers": int(total_df["suite_number"].nunique()),
        "total_profit_usd": round(float(total_df["profit_usd"].sum()), 2),
        "total_revenue_usd": round(float(total_df["revenue_usd"].sum()), 2),
        "total_buy_revenue_usd": round(float(total_df["revenue_buy_usd"].sum()), 2),
        "total_storage_revenue_usd": round(float(total_df["revenue_storage_usd"].sum()), 2),
        "total_ship_revenue_usd": round(float(total_df["revenue_ship_usd"].sum()), 2),
    }

    # 환율 중앙값
    if "origin_usd_krw_package" in total_df.columns:
        valid_rates = total_df["origin_usd_krw_package"].dropna()
        valid_rates = valid_rates[valid_rates > 0]
        kpis["exchange_rate"] = round(float(valid_rates.median()), 2) if len(valid_rates) > 0 else 1450.0
    else:
        kpis["exchange_rate"] = 1450.0

    # 프로핏 측정 기간 (profit_krw 유효 행 기준, ship_at_kst 기준)
    if "ship_date_kst" in total_df.columns:
        _profit_dates = pd.to_datetime(
            total_df.loc[total_df["profit_krw"].notna(), "ship_date_kst"], errors="coerce"
        ).dropna()
        if len(_profit_dates) > 0:
            kpis["profit_start_date"] = str(_profit_dates.min().date())
            kpis["profit_end_date"] = str(_profit_dates.max().date())
            kpis["profit_days"] = int(_profit_dates.dt.date.nunique())  # 실측 날짜 수
        else:
            kpis["profit_start_date"] = None
            kpis["profit_end_date"] = None
            kpis["profit_days"] = 0
    else:
        kpis["profit_start_date"] = None
        kpis["profit_end_date"] = None
        kpis["profit_days"] = 0

    # 마지막 업데이트 시각 (KST)
    import datetime as _dt
    _kst = _dt.timezone(_dt.timedelta(hours=9))
    kpis["last_updated"] = _dt.datetime.now(_kst).strftime("%Y-%m-%d %H:%M KST")

    # --- 패키지 통계 ---
    pkg_metrics = [
        "revenue_krw", "revenue_buy_krw", "revenue_storage_krw",
        "revenue_ship_krw", "profit_krw", "goods_profit_krw",
        "warehouse_profit_krw", "shipping_profit_krw", "marked_up_cost_krw",
    ]
    # 존재하지 않는 컬럼은 0으로 채움
    for col in pkg_metrics:
        if col not in total_df.columns:
            total_df[col] = 0

    def get_pkg_stats(df):
        stats = df[pkg_metrics].describe().T
        stats["sum"] = df[pkg_metrics].sum()
        return {k: {kk: _safe(vv) for kk, vv in v.items()} for k, v in stats.to_dict(orient="index").items()}

    def get_case_summary(df):
        summary = df.groupby("source_case").agg({
            "package_id": "nunique",
            "suite_number": "nunique",
            "revenue_krw": "sum",
            "revenue_buy_krw": "sum",
            "revenue_storage_krw": "sum",
            "revenue_ship_krw": "sum",
            "profit_krw": "sum",
            "goods_profit_krw": "sum",
            "warehouse_profit_krw": "sum",
            "shipping_profit_krw": "sum",
            "revenue_usd": "sum",
            "revenue_buy_usd": "sum",
            "revenue_storage_usd": "sum",
            "revenue_ship_usd": "sum",
            "profit_usd": "sum",
            "goods_profit_usd": "sum",
            "warehouse_profit_usd": "sum",
            "shipping_profit_usd": "sum",
        }).reset_index()
        summary.rename(columns={"suite_number": "suite_count"}, inplace=True)
        # profit_krw가 NOT NULL인 패키지 수 (프로핏 계산 가능 건수)
        profit_pkg = (
            df[df["profit_krw"].notna()]
            .groupby("source_case")["package_id"]
            .nunique()
            .rename("profit_pkg_count")
        )
        summary = summary.merge(profit_pkg, on="source_case", how="left")
        summary["profit_pkg_count"] = summary["profit_pkg_count"].fillna(0).astype(int)
        summary["profit_per_pkg"] = summary["profit_krw"] / summary["package_id"].replace(0, 1)
        summary["profit_per_suite"] = summary["profit_krw"] / summary["suite_count"].replace(0, 1)
        summary["profit_per_pkg_usd"] = summary["profit_usd"] / summary["package_id"].replace(0, 1)
        summary["profit_per_suite_usd"] = summary["profit_usd"] / summary["suite_count"].replace(0, 1)
        return _safe_records(summary)

    # 전체 / 관리자 제외
    admin_upper = [s.upper() for s in ADMIN_SUITES]
    total_df_no_admin = total_df[~total_df["suite_number"].str.upper().isin(admin_upper)]

    pkg_stats_all = get_pkg_stats(total_df)
    pkg_stats_no_admin = get_pkg_stats(total_df_no_admin)
    case_summary_all = get_case_summary(total_df)
    case_summary_no_admin = get_case_summary(total_df_no_admin)

    # --- Suite 통계 ---
    suite_df = total_df.groupby("suite_number").agg({
        "revenue_krw": "sum",
        "revenue_buy_krw": "sum",
        "revenue_storage_krw": "sum",
        "revenue_ship_krw": "sum",
        "profit_krw": "sum",
        "goods_profit_krw": "sum",
        "warehouse_profit_krw": "sum",
        "shipping_profit_krw": "sum",
        "marked_up_cost_krw": "sum",
        "package_id": "nunique",
        "revenue_usd": "sum",
        "revenue_buy_usd": "sum",
        "revenue_storage_usd": "sum",
        "revenue_ship_usd": "sum",
        "profit_usd": "sum",
        "goods_profit_usd": "sum",
        "warehouse_profit_usd": "sum",
        "shipping_profit_usd": "sum",
    })
    suite_df.columns = [
        "total_revenue", "total_rev_buy", "total_rev_storage", "total_rev_ship",
        "total_profit", "total_buy_profit", "total_storage_profit", "total_ship_profit",
        "total_markup", "total_packages",
        "total_revenue_usd", "total_rev_buy_usd", "total_rev_storage_usd",
        "total_rev_ship_usd", "total_profit_usd",
        "total_buy_profit_usd", "total_storage_profit_usd", "total_ship_profit_usd",
    ]
    suite_df["rev_percentile"] = np.ceil(
        (suite_df["total_revenue"].rank(ascending=False) / len(suite_df)) * 100
    ).astype(int)
    suite_df["profit_percentile"] = np.ceil(
        (suite_df["total_profit"].rank(ascending=False) / len(suite_df)) * 100
    ).astype(int)
    suite_df["percentile"] = suite_df["rev_percentile"]

    # Case별 상세 실적
    case_detail = total_df.groupby(["suite_number", "source_case"]).agg({
        "package_id": "nunique",
        "revenue_krw": "sum",
        "profit_krw": "sum",
        "revenue_buy_krw": "sum",
        "revenue_storage_krw": "sum",
        "revenue_ship_krw": "sum",
        "revenue_usd": "sum",
        "profit_usd": "sum",
        "revenue_buy_usd": "sum",
        "revenue_storage_usd": "sum",
        "revenue_ship_usd": "sum",
    }).rename(columns={
        "package_id": "count",
        "revenue_krw": "revenue",
        "profit_krw": "profit",
        "revenue_buy_krw": "buy",
        "revenue_storage_krw": "storage",
        "revenue_ship_krw": "ship",
        "revenue_usd": "revenue_usd",
        "profit_usd": "profit_usd",
        "revenue_buy_usd": "buy_usd",
        "revenue_storage_usd": "storage_usd",
        "revenue_ship_usd": "ship_usd",
    }).reset_index()

    case_stats_map = {}
    for _, row in case_detail.iterrows():
        s_num = str(row["suite_number"])
        c_name = str(row["source_case"])
        if s_num not in case_stats_map:
            case_stats_map[s_num] = {}
        case_stats_map[s_num][c_name] = {
            "count": int(row["count"]),
            "revenue": _safe(row["revenue"]),
            "profit": _safe(row["profit"]),
            "buy": _safe(row["buy"]),
            "storage": _safe(row["storage"]),
            "ship": _safe(row["ship"]),
            "revenue_usd": round(_safe(row["revenue_usd"]), 2),
            "profit_usd": round(_safe(row["profit_usd"]), 2),
            "buy_usd": round(_safe(row["buy_usd"]), 2),
            "storage_usd": round(_safe(row["storage_usd"]), 2),
            "ship_usd": round(_safe(row["ship_usd"]), 2),
        }

    # 콘솔/리팩 패키지 수
    console_agg = total_df[total_df["package_type"].isin(["CONSOLE", "REPACK"])].groupby("suite_number")["package_id"].nunique()

    # 평균 실측/부피 무게
    weight_agg = total_df.groupby("suite_number").agg(
        avg_package_weight=("package_weight", "mean"),
        avg_dim_weight=("dimension_weight", "mean"),
    )

    # 국가별 집계 (ISO 3166-1 alpha-2, 2자리 대문자만)
    valid_country_df = total_df[
        total_df["country_code"].notna() &
        (total_df["country_code"].astype(str).str.len() == 2) &
        (total_df["country_code"].astype(str).str.match(r"^[A-Z]{2}$"))
    ].copy()
    country_agg = valid_country_df.groupby(["suite_number", "country_code"])["package_id"].nunique().reset_index()
    country_agg.columns = ["suite_number", "country_code", "count"]
    country_counts_map = {}
    for _, crow in country_agg.iterrows():
        s = str(crow["suite_number"])
        if s not in country_counts_map:
            country_counts_map[s] = {}
        country_counts_map[s][str(crow["country_code"])] = int(crow["count"])

    # all_suites 리스트
    all_suites_list = []
    for suite_num, row in suite_df.iterrows():
        suite_num_str = str(suite_num)
        suite_data = {k: _safe(v) for k, v in row.to_dict().items()}
        suite_data["suite_number"] = suite_num_str
        suite_data["case_stats"] = case_stats_map.get(suite_num_str, {})
        suite_data["console_packages"] = int(console_agg.get(suite_num, 0))
        suite_data["avg_package_weight"] = round(float(weight_agg.loc[suite_num, "avg_package_weight"]), 1) if suite_num in weight_agg.index else 0.0
        suite_data["avg_dim_weight"] = round(float(weight_agg.loc[suite_num, "avg_dim_weight"]), 1) if suite_num in weight_agg.index else 0.0
        suite_data["country_counts"] = country_counts_map.get(suite_num_str, {})
        suite_data["shipping_countries"] = len(suite_data["country_counts"])
        all_suites_list.append(suite_data)

    # Suite 통계 요약
    suite_metrics = [
        "total_revenue", "total_rev_buy", "total_rev_storage",
        "total_rev_ship", "total_profit", "total_markup", "total_packages",
    ]
    suite_stats = suite_df[suite_metrics].describe().T
    suite_stats["sum"] = suite_df[suite_metrics].sum()
    suite_stats_dict = {k: {kk: _safe(vv) for kk, vv in v.items()} for k, v in suite_stats.to_dict(orient="index").items()}

    # 매출 비중
    def get_revenue_split(df):
        return {
            "buy": int(df["revenue_buy_krw"].sum()),
            "storage": int(df["revenue_storage_krw"].sum()),
            "ship": int(df["revenue_ship_krw"].sum()),
            "buy_usd": round(float(df["revenue_buy_usd"].sum()), 2),
            "storage_usd": round(float(df["revenue_storage_usd"].sum()), 2),
            "ship_usd": round(float(df["revenue_ship_usd"].sum()), 2),
        }

    revenue_split = get_revenue_split(total_df)

    case_revenue_splits = {}
    for case_name in total_df["source_case"].unique():
        case_revenue_splits[case_name] = get_revenue_split(
            total_df[total_df["source_case"] == case_name]
        )

    case_revenue_splits_no_admin = {}
    for case_name in total_df_no_admin["source_case"].unique():
        case_revenue_splits_no_admin[case_name] = get_revenue_split(
            total_df_no_admin[total_df_no_admin["source_case"] == case_name]
        )

    # VVIP Top 50
    vvip = suite_df.sort_values("total_revenue", ascending=False).head(50).reset_index()
    vvip_list = _safe_records(vvip)

    # 요약
    n_suites = len(suite_df)
    top_10_pct = suite_df.sort_values("total_revenue", ascending=False).head(
        max(1, int(n_suites * 0.1))
    )["total_revenue"].sum()
    summary = {
        "top_1_percent_count": int(len(suite_df[suite_df["percentile"] == 1])),
        "top_10_percent_contribution": float(
            (top_10_pct / max(kpis["total_revenue"], 1)) * 100
        ),
    }

    return {
        "kpis": kpis,
        "pkg_stats": pkg_stats_all,
        "pkg_stats_no_admin": pkg_stats_no_admin,
        "case_summary": case_summary_all,
        "case_summary_no_admin": case_summary_no_admin,
        "suite_stats": suite_stats_dict,
        "revenue_split": revenue_split,
        "case_revenue_splits": case_revenue_splits,
        "case_revenue_splits_no_admin": case_revenue_splits_no_admin,
        "vvip_list": vvip_list,
        "all_suites": all_suites_list,
        "summary": summary,
    }


# ============================================================================
# 유틸리티
# ============================================================================

def _safe(val):
    """NaN/Inf를 JSON-safe 값으로 변환"""
    if isinstance(val, (float, np.floating)):
        if np.isnan(val) or np.isinf(val):
            return 0
        return float(val)
    if isinstance(val, (int, np.integer)):
        return int(val)
    return val


def _safe_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame을 JSON-safe records 리스트로 변환"""
    records = df.to_dict(orient="records")
    return [{k: _safe(v) for k, v in r.items()} for r in records]


# ============================================================================
# 메인
# ============================================================================

def update_competitor_volumes(case_dfs: dict[int, pd.DataFrame]) -> None:
    """BigQuery case 데이터에서 carrier별 출고량 집계 → competitor_rate_data.json 업데이트

    - 무게 기준: dimension_weight (부피 무게), 0.5kg 간격
    - carrier 구분: carrier_service='KPACKET' → kpacket, 나머지는 carrier 컬럼(DHL/EMS/FEDEX/UPS)
    - 날짜 필터: 2025-01-01 ~ 2025-12-31 UTC
    - 국가 필터: competitor_rate_data.json에 있는 10개국 (ISO 2자리 코드)
    """
    import math

    VOLUME_COUNTRIES = {'AU', 'BR', 'CA', 'DE', 'FR', 'GB', 'JP', 'MX', 'SG', 'US'}
    CARRIER_NORMALIZE = {'DHL': 'DHL', 'DHL_EXPRESS': 'DHL', 'EMS': 'EMS', 'FEDEX': 'FEDEX', 'UPS': 'UPS'}

    def assign_wg(w_g):
        w_kg = float(w_g) / 1000.0
        g = math.ceil(w_kg / 0.5) * 0.5
        if g <= 0:
            g = 0.5
        return round(g, 1)

    dfs = []
    for case_num, df in case_dfs.items():
        df = df.copy()

        # 날짜 컬럼 확인
        if 'trans_date_utc_package' in df.columns:
            date_col = 'trans_date_utc_package'
        elif 'trans_at_utc_package' in df.columns:
            date_col = 'trans_at_utc_package'
        else:
            print(f"  Case {case_num}: 날짜 컬럼 없음, 스킵")
            continue

        required = {'package_id', 'country_code', 'dimension_weight'}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            print(f"  Case {case_num}: 컬럼 누락 {missing}, 스킵")
            continue

        keep = list(required | {date_col}
                    | ({'carrier'} if 'carrier' in df.columns else set())
                    | ({'carrier_service'} if 'carrier_service' in df.columns else set()))
        df = df[keep].copy()
        df = df.rename(columns={date_col: '_date'})
        dfs.append(df)

    if not dfs:
        print("update_competitor_volumes: 처리할 데이터 없음")
        return

    all_df = pd.concat(dfs, ignore_index=True)

    # 날짜 필터 2025
    all_df['_date_str'] = all_df['_date'].astype(str).str[:10]
    all_df = all_df[
        (all_df['_date_str'] >= '2025-01-01') & (all_df['_date_str'] <= '2025-12-31')
    ].copy()

    # 국가 필터
    all_df = all_df[all_df['country_code'].isin(VOLUME_COUNTRIES)].copy()

    # dimension_weight → weight_group
    all_df['dimension_weight'] = pd.to_numeric(all_df['dimension_weight'], errors='coerce').fillna(0)
    all_df['weight_group'] = all_df['dimension_weight'].apply(assign_wg)

    # carrier_key 결정
    def get_carrier_key(row):
        cs = str(row.get('carrier_service') or '').upper()
        if cs == 'KPACKET':
            return 'kpacket'
        if cs == 'EMS':
            return 'EMS'
        c = str(row.get('carrier') or '').upper()
        return CARRIER_NORMALIZE.get(c, 'other')

    all_df['carrier_key'] = all_df.apply(get_carrier_key, axis=1)

    # 진단: carrier 컬럼 고유값 출력 (DHL/EMS 매칭 확인용)
    if 'carrier' in all_df.columns:
        unique_carriers = sorted(all_df['carrier'].dropna().unique().tolist())
        print(f"  [진단] BigQuery carrier 고유값: {unique_carriers}")
    other_df = all_df[all_df['carrier_key'] == 'other']
    if len(other_df) > 0 and 'carrier' in other_df.columns:
        other_carriers = sorted(other_df['carrier'].dropna().unique().tolist())
        print(f"  [진단] 'other'로 분류된 carrier값 (매칭 실패): {other_carriers}")

    # 전체 출고량 (total): country_code × weight_group, unique package_id
    total_agg = (
        all_df.groupby(['country_code', 'weight_group'])['package_id']
        .nunique().reset_index().rename(columns={'package_id': 'total'})
    )

    # carrier별 출고량
    carrier_agg = (
        all_df[all_df['carrier_key'] != 'other']
        .groupby(['country_code', 'weight_group', 'carrier_key'])['package_id']
        .nunique().reset_index().rename(columns={'package_id': 'count'})
    )

    # lookup 생성: {iso: {wg: {total, DHL, EMS, FEDEX, UPS, kpacket}}}
    vol_lookup: dict[str, dict[float, dict]] = {}
    for _, row in total_agg.iterrows():
        iso, wg, total = row['country_code'], row['weight_group'], int(row['total'])
        vol_lookup.setdefault(iso, {})[wg] = {
            'total': total, 'DHL': 0, 'EMS': 0, 'FEDEX': 0, 'UPS': 0, 'kpacket': 0
        }
    for _, row in carrier_agg.iterrows():
        iso, wg, ck, cnt = (
            row['country_code'], row['weight_group'],
            row['carrier_key'], int(row['count'])
        )
        if iso in vol_lookup and wg in vol_lookup[iso]:
            vol_lookup[iso][wg][ck] = cnt

    # competitor_rate_data.json 업데이트 (dk_volume_2025 dict만 갱신, gap 필드 유지)
    with open(COMP_JSON_PATH, 'r', encoding='utf-8') as fh:
        comp_data = json.load(fh)

    updated = 0
    for country_code, country_data in comp_data['data'].items():
        if country_code not in vol_lookup:
            continue
        cvols = vol_lookup[country_code]
        for entry in country_data.get('rows', []):
            w = entry.get('weight')
            if w is None:
                continue
            wg_key = round(float(w), 1)
            new_vol = cvols.get(wg_key, {
                'total': 0, 'DHL': 0, 'EMS': 0, 'FEDEX': 0, 'UPS': 0, 'kpacket': 0
            })
            existing = entry.get('dk_volume_2025')
            if isinstance(existing, dict):
                existing.update(new_vol)
            else:
                entry['dk_volume_2025'] = new_vol
            updated += 1

    with open(COMP_JSON_PATH, 'w', encoding='utf-8') as fh:
        json.dump(comp_data, fh, ensure_ascii=False, indent=2)

    print(f"competitor_rate_data.json 출고량 업데이트 완료 ({updated}개 항목)")


def main():
    print("=" * 60)
    print("BigQuery -> dashboard_data.json 자동 갱신")
    print("=" * 60)

    # BigQuery 클라이언트 초기화
    # GOOGLE_APPLICATION_CREDENTIALS 환경변수 또는 서비스 계정 키로 인증
    client = bigquery.Client()
    print(f"BigQuery project: {client.project}")

    # 쿼리 실행
    case_dfs = run_queries(client)

    # 데이터 가공
    print("\nProcessing dashboard data...")
    dashboard_data = build_dashboard_data(case_dfs)

    # JSON 출력
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"KPI summary:")
    print(f"  Total Profit: {dashboard_data['kpis']['total_profit']:,} KRW")
    print(f"  Total Revenue: {dashboard_data['kpis']['total_revenue']:,} KRW")
    print(f"  Total Packages: {dashboard_data['kpis']['total_packages']:,}")
    print(f"  Total Customers: {dashboard_data['kpis']['total_customers']:,}")

    # competitor_rate_data.json 출고량 업데이트
    print("\nUpdating competitor volume data...")
    update_competitor_volumes(case_dfs)

    print("Done!")


if __name__ == "__main__":
    main()
