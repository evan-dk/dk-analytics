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

# 분석 기간 상한 (ship_date_kst 기준)
DATE_UPPER_LIMIT = "2026-02-20"

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

        # 날짜 필터
        if "ship_date_kst" in df.columns:
            df["ship_date_kst"] = pd.to_datetime(df["ship_date_kst"], errors="coerce")
            df = df[df["ship_date_kst"] <= DATE_UPPER_LIMIT]

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
    total_df["suite_number"] = total_df["suite_number"].fillna("Unknown").astype(str)

    # profit_krw가 NULL인 패키지는 배송원가 미확보 → profit 집계에서 제외
    total_df = total_df[total_df["profit_krw"].notna()].copy()

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

    # all_suites 리스트
    all_suites_list = []
    for suite_num, row in suite_df.iterrows():
        suite_data = {k: _safe(v) for k, v in row.to_dict().items()}
        suite_data["suite_number"] = str(suite_num)
        suite_data["case_stats"] = case_stats_map.get(str(suite_num), {})
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
    print("Done!")


if __name__ == "__main__":
    main()
