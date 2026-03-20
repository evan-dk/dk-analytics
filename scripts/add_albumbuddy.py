#!/usr/bin/env python3
"""
Albumbuddy 운임 데이터를 competitor_rate_data.json에 추가하고 Gap 재계산.
- US 제외
- weight > 10kg 제외
- Gap 계산 대상: korgou, kfriday, albumbuddy (COMP_PLATFORMS)
"""

import csv
import json
from pathlib import Path

CSV_PATH = Path(r"C:\Users\김승곤\Desktop\딜리버드코리아\da_data\dk_data_project\profit_project\Comparative Analysis of Competitor Shipping Costs\competitor_shipping_costs.csv")
JSON_PATH = Path(r"C:\Users\김승곤\Desktop\딜리버드코리아\da_data\dk_data_project\dk-analytics\profit\competitor_rate_data.json")

STANDARD_CARRIERS = {'k-packet'}
EXPRESS_CARRIERS  = {'DHL', 'EMS', 'FEDEX', 'UPS'}
DK_PLATFORMS      = {'delivered'}
COMP_PLATFORMS    = {'korgou', 'kfriday', 'albumbuddy'}
SEA_CARRIERS      = {'korea post sea parcel', 'Sagawa Express'}


def recalculate_gaps(row: dict) -> None:
    fees = row.get('fees', {})

    dk_std, comp_std = [], []
    dk_exp, comp_exp = [], []

    for carrier, carrier_fees in fees.items():
        if carrier in SEA_CARRIERS or carrier == '최소':
            continue
        is_std = carrier in STANDARD_CARRIERS
        is_exp = carrier in EXPRESS_CARRIERS
        if not is_std and not is_exp:
            continue
        for pl, fee in carrier_fees.items():
            if not fee or fee <= 0:
                continue
            if pl in DK_PLATFORMS:
                if is_std: dk_std.append((fee, carrier))
                if is_exp: dk_exp.append((fee, carrier))
            elif pl in COMP_PLATFORMS:
                if is_std: comp_std.append((fee, carrier))
                if is_exp: comp_exp.append((fee, carrier))

    # Standard gap
    if dk_std and comp_std:
        dk_min = min(dk_std)
        comp_min_fee = min(f for f, _ in comp_std)
        row['dk_gap_standard_pct'] = round((dk_min[0] - comp_min_fee) / comp_min_fee * 100, 1)
        row['dk_standard_cheapest'] = {'fee': dk_min[0], 'carrier': dk_min[1]}
        row['competitor_standard_min'] = comp_min_fee

    # Express gap
    if dk_exp and comp_exp:
        dk_min = min(dk_exp)
        comp_min_fee = min(f for f, _ in comp_exp)
        row['dk_gap_express_pct'] = round((dk_min[0] - comp_min_fee) / comp_min_fee * 100, 1)
        row['dk_express_cheapest'] = {'fee': dk_min[0], 'carrier': dk_min[1]}
        row['competitor_express_min'] = comp_min_fee


# ── 1. CSV 읽기 ───────────────────────────────────────────────────────────────
# {country: {carrier: {weight(float): fee(int)}}}
albumbuddy: dict[str, dict[str, dict[float, int]]] = {}

encodings = ['cp949', 'euc-kr', 'latin-1', 'utf-8-sig', 'utf-8']
for enc in encodings:
    try:
        with open(CSV_PATH, encoding=enc) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['Platform'].strip() != 'Albumbuddy':
                    continue
                country = row['Country'].strip()
                if country == 'US':
                    continue
                carrier = row['Carrier'].strip()
                weight  = float(row['Weight (kg)'].strip())
                if weight > 10:
                    continue
                fee_str = row['Fee'].replace(',', '').strip()
                fee = int(float(fee_str))
                albumbuddy.setdefault(country, {}).setdefault(carrier, {})[weight] = fee
        print(f"CSV 읽기 성공 (encoding={enc})")
        break
    except (UnicodeDecodeError, UnicodeError):
        continue

if not albumbuddy:
    raise RuntimeError("CSV 읽기 실패 또는 Albumbuddy 데이터 없음")

print("Albumbuddy 데이터 국가/carrier:")
for country in sorted(albumbuddy):
    for carrier in sorted(albumbuddy[country]):
        weights = sorted(albumbuddy[country][carrier])
        print(f"  {country} {carrier}: {weights[0]}~{weights[-1]}kg ({len(weights)}개)")


# ── 2. JSON 읽기 ──────────────────────────────────────────────────────────────
with open(JSON_PATH, encoding='utf-8') as f:
    data = json.load(f)


# ── 3. JSON 업데이트 ──────────────────────────────────────────────────────────
updated_rows = 0

for country, country_data in data['data'].items():
    if country == 'US' or country not in albumbuddy:
        continue

    country_ab = albumbuddy[country]

    # columns에 albumbuddy 플랫폼 추가
    for col in country_data['columns']:
        carrier = col['carrier']
        if carrier in country_ab and 'albumbuddy' not in col['platforms']:
            col['platforms'].append('albumbuddy')
            print(f"  {country} {carrier} → albumbuddy 플랫폼 추가")

    # rows에 albumbuddy 운임 추가 + gap 재계산
    for row in country_data['rows']:
        weight = round(float(row['weight']), 1)
        for carrier, wt_fees in country_ab.items():
            if weight in wt_fees:
                row['fees'].setdefault(carrier, {})['albumbuddy'] = wt_fees[weight]
        recalculate_gaps(row)
        updated_rows += 1


# ── 4. JSON 저장 ──────────────────────────────────────────────────────────────
with open(JSON_PATH, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\ncompetitor_rate_data.json 업데이트 완료 ({updated_rows}개 행 처리)")
