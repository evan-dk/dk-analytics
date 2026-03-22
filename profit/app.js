// 전역 설정 변수
const ADMIN_SUITES = ['Z9996', 'Z9997', 'Z9998', 'Z9999', 'z9996', 'z9997', 'z9998', 'z9999'];

// Case 이름 매핑
const CASE_NAMES = {
    'Case 1': '배송대행(ASN) 단독',
    'Case 2': '구매대행(Buy Request) 단독',
    'Case 3': '배송대행(ASN) 합포장',
    'Case 4': '구매대행(Buy Request) 합포장',
    'Case 5': '배송대행(ASN) + 구매대행(Buy Request) 혼합'
};

// 환율 설정 (전역 변수)
window.exchangeRate = 1450; // 1 USD = 1450 KRW (필요시 수정 가능)
window.currencyMode = 'KRW'; // 기본값: 'KRW' 또는 'USD'
window.chartViewMode = 'revenue'; // 'revenue' 또는 'profit'

let dashboardData = null;
let caseChart = null;
let _lastKpis = null;
let _lastCaseName = 'Case 1';

function setChartMode(mode) {
    window.chartViewMode = mode;
    const revBtn = document.getElementById('chart-mode-revenue');
    const proBtn = document.getElementById('chart-mode-profit');
    if (mode === 'revenue') {
        revBtn.style.cssText = 'padding:6px 20px; border-radius:20px; border:1px solid #3b82f6; background:#3b82f6; color:#fff; font-weight:600; cursor:pointer; font-size:13px;';
        proBtn.style.cssText = 'padding:6px 20px; border-radius:20px; border:1px solid #475569; background:transparent; color:#94a3b8; cursor:pointer; font-size:13px;';
    } else {
        proBtn.style.cssText = 'padding:6px 20px; border-radius:20px; border:1px solid #22c55e; background:#22c55e; color:#fff; font-weight:600; cursor:pointer; font-size:13px;';
        revBtn.style.cssText = 'padding:6px 20px; border-radius:20px; border:1px solid #475569; background:transparent; color:#94a3b8; cursor:pointer; font-size:13px;';
    }
    if (_lastKpis) updateOverallCharts(_lastKpis);
    renderCaseChart(_lastCaseName);
}

async function initDashboard() {
    // datalabels 플러그인 등록 (CDN 자동 등록 실패 시 대비)
    if (typeof ChartDataLabels !== 'undefined') {
        Chart.register(ChartDataLabels);
    }
    try {
        console.log("Fetching dashboard_data.json...");
        const response = await fetch('dashboard_data.json?t=' + Date.now());

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        dashboardData = await response.json();
        console.log("Data loaded successfully");

        // 데이터에서 환율 로드 (없으면 기본값 1450 사용)
        if (dashboardData.kpis && dashboardData.kpis.exchange_rate) {
            window.exchangeRate = dashboardData.kpis.exchange_rate;
            console.log(`Exchange rate loaded from data: ${window.exchangeRate}`);
        } else {
            console.warn('Exchange rate not found in data, using default: 1450');
        }

        // 관리자 제외 토글 이벤트 리스너
        const adminToggle = document.getElementById('exclude-admin');
        if (adminToggle) {
            adminToggle.addEventListener('change', () => {
                updateDashboard(adminToggle.checked);
            });
            // 초기 로드 시 체크박스 상태에 따라 렌더링
            updateDashboard(adminToggle.checked);
        } else {
            // 토글이 없는 경우 기본 데이터 사용
            updateDashboard(false);
        }

        // 통화 변환 토글 이벤트 리스너
        const currencyToggle = document.getElementById('currency-toggle');
        if (currencyToggle) {
            currencyToggle.addEventListener('change', () => {
                window.currencyMode = currencyToggle.checked ? 'USD' : 'KRW';
                updateDashboard(adminToggle ? adminToggle.checked : false);
            });
        }

        // Total Profit 라벨에 측정 일수 표시
        const profitDays = dashboardData.kpis?.profit_days;
        const labelEl = document.getElementById('total-profit-label');
        if (labelEl && profitDays) {
            labelEl.textContent = `Total Profit (${profitDays.toLocaleString()}일)`;
        }

        // 데이터 메타 정보 렌더링 (배너 위, 우측 정렬)
        const lastUpdated = dashboardData.kpis?.last_updated;
        const updatedEl   = document.getElementById('meta-last-updated');
        if (updatedEl && lastUpdated) {
            updatedEl.textContent = `마지막 업데이트: ${lastUpdated}`;
        }

        // 로딩 오버레이 숨김
        const loadingOverlay = document.getElementById('loading-overlay');
        if (loadingOverlay) loadingOverlay.style.display = 'none';

        // 6. Init Features
        initModal();
        initSearch();
        renderCharts();

    } catch (error) {
        console.error('Error loading dashboard data:', error);
        // 에러 시에도 반드시 loading overlay 숨김
        const loadingOverlayErr = document.getElementById('loading-overlay');
        if (loadingOverlayErr) loadingOverlayErr.style.display = 'none';
        const container = document.querySelector('.dashboard-container');
        if (container) {
            container.innerHTML = `
                <div style="background: rgba(239, 68, 68, 0.1); border: 1px solid #ef4444; color: #ef4444; padding: 20px; border-radius: 12px; margin: 50px auto; max-width: 600px; text-align: center;">
                    <h2 style="margin-bottom: 10px;">⚠️ Dashboard Load Failed</h2>
                    <p style="margin-bottom: 15px;">${error.message}</p>
                    <button onclick="location.reload()" style="background: #ef4444; color: white; border: none; padding: 8px 20px; border-radius: 6px; cursor: pointer;">Refresh Page</button>
                </div>
            `;
        }
    }
}

// 무게 포맷 함수
function formatWeight(kg) {
    if (kg === null || kg === undefined || isNaN(kg)) return '- kg';
    return kg.toLocaleString('ko-KR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' kg';
}

// 통화 포맷 함수 (실제 USD 값 사용)
function formatCurrency(amountKRW, amountUSD) {
    const krw = (amountKRW == null || isNaN(amountKRW)) ? 0 : amountKRW;
    const usd = (amountUSD == null || isNaN(amountUSD)) ? null : amountUSD;
    if (window.currencyMode === 'USD') {
        if (usd !== null) {
            return `$ ${Math.round(usd).toLocaleString('en-US')}`;
        }
        // fallback: USD 값이 없으면 환율 변환
        const converted = krw / window.exchangeRate;
        return `$ ${Math.round(converted).toLocaleString('en-US')}`;
    }
    return `₩ ${Math.round(krw).toLocaleString()}`;
}

function updateDashboard(excludeAdmin) {
    if (!dashboardData) return;

    // 1. 데이터 필터링
    let filteredSuites = dashboardData.all_suites;
    if (excludeAdmin) {
        filteredSuites = dashboardData.all_suites.filter(s => !ADMIN_SUITES.includes(s.suite_number));
    }

    // 2. KPI 재계산 (all_suites 기반 합계)
    const kpis = filteredSuites.reduce((acc, curr) => {
        return {
            total_profit:          acc.total_profit          + (curr.total_profit        || 0),
            we_buy_profit:         acc.we_buy_profit         + (curr.total_buy_profit     || 0),
            storage_profit:        acc.storage_profit        + (curr.total_storage_profit || 0),
            ship_profit:           acc.ship_profit           + (curr.total_ship_profit    || 0),
            total_revenue:         acc.total_revenue         + (curr.total_revenue       || 0),
            total_buy_revenue:     acc.total_buy_revenue     + (curr.total_rev_buy       || 0),
            total_storage_revenue: acc.total_storage_revenue + (curr.total_rev_storage   || 0),
            total_ship_revenue:    acc.total_ship_revenue    + (curr.total_rev_ship      || 0),
            total_packages:        acc.total_packages        + (curr.total_packages      || 0),
            total_customers:       acc.total_customers       + 1,
            total_profit_usd:          acc.total_profit_usd          + (curr.total_profit_usd       || 0),
            we_buy_profit_usd:         acc.we_buy_profit_usd         + (curr.total_buy_profit_usd     || 0),
            storage_profit_usd:        acc.storage_profit_usd        + (curr.total_storage_profit_usd || 0),
            ship_profit_usd:           acc.ship_profit_usd           + (curr.total_ship_profit_usd    || 0),
            total_revenue_usd:         acc.total_revenue_usd         + (curr.total_revenue_usd      || 0),
            total_buy_revenue_usd:     acc.total_buy_revenue_usd     + (curr.total_rev_buy_usd      || 0),
            total_storage_revenue_usd: acc.total_storage_revenue_usd + (curr.total_rev_storage_usd  || 0),
            total_ship_revenue_usd:    acc.total_ship_revenue_usd    + (curr.total_rev_ship_usd     || 0),
            total_package_weight:      acc.total_package_weight      + (curr.total_package_weight   || 0),
            total_dim_weight:          acc.total_dim_weight          + (curr.total_dim_weight        || 0),
        };
    }, {
        total_profit: 0, we_buy_profit: 0, storage_profit: 0, ship_profit: 0,
        total_revenue: 0, total_buy_revenue: 0, total_storage_revenue: 0, total_ship_revenue: 0,
        total_packages: 0, total_customers: 0,
        total_profit_usd: 0, we_buy_profit_usd: 0, storage_profit_usd: 0, ship_profit_usd: 0,
        total_revenue_usd: 0, total_buy_revenue_usd: 0, total_storage_revenue_usd: 0, total_ship_revenue_usd: 0,
        total_package_weight: 0, total_dim_weight: 0,
    });

    // KPI UI 업데이트 — Row 1: Profit
    document.getElementById('total-profit').textContent   = formatCurrency(kpis.total_profit,   kpis.total_profit_usd);
    document.getElementById('we-buy-profit').textContent  = formatCurrency(kpis.we_buy_profit,  kpis.we_buy_profit_usd);
    document.getElementById('storage-profit').textContent = formatCurrency(kpis.storage_profit, kpis.storage_profit_usd);
    document.getElementById('ship-profit').textContent    = formatCurrency(kpis.ship_profit,    kpis.ship_profit_usd);
    // Row 2: Revenue
    document.getElementById('total-revenue').textContent   = formatCurrency(kpis.total_revenue,         kpis.total_revenue_usd);
    document.getElementById('buy-revenue').textContent     = formatCurrency(kpis.total_buy_revenue,     kpis.total_buy_revenue_usd);
    document.getElementById('storage-revenue').textContent = formatCurrency(kpis.total_storage_revenue, kpis.total_storage_revenue_usd);
    document.getElementById('ship-revenue').textContent    = formatCurrency(kpis.total_ship_revenue,    kpis.total_ship_revenue_usd);
    // Row 3: Count
    document.getElementById('total-packages').textContent  = `${kpis.total_packages.toLocaleString()} 건`;
    document.getElementById('total-customers').textContent = `${kpis.total_customers.toLocaleString()} 명`;
    // Row 4: Weight
    const avg_actual_weight = kpis.total_packages > 0 ? kpis.total_package_weight / kpis.total_packages : 0;
    const avg_vol_weight    = kpis.total_packages > 0 ? kpis.total_dim_weight      / kpis.total_packages : 0;
    document.getElementById('total-actual-weight').textContent = formatWeight(kpis.total_package_weight);
    document.getElementById('avg-actual-weight').textContent   = formatWeight(avg_actual_weight);
    document.getElementById('total-vol-weight').textContent    = formatWeight(kpis.total_dim_weight);
    document.getElementById('avg-vol-weight').textContent      = formatWeight(avg_vol_weight);

    // 3. Suite Summary Table 업데이트
    // Suite별 단위 기술통계 (완벽 재계산)

    // 4. Charts 업데이트 (overall splits 재계산)
    updateOverallCharts(kpis);

    // 6. 하단 요약 테이블 업데이트 (필터링 반영 핵심)
    const pkgBody = document.getElementById('pkg-summary-body');
    const caseBody = document.getElementById('case-summary-body');
    const suiteBody = document.getElementById('suite-summary-body');

    if (!pkgBody || !caseBody || !suiteBody) return;

    pkgBody.innerHTML = '';
    caseBody.innerHTML = '';
    suiteBody.innerHTML = '';

    // A. Suite별 단위 기술통계 (완벽 재계산 가능)
    const suiteMetrics = {
        'total_profit': '수익',
        'total_buy_profit': '구매대행 수익',
        'total_storage_profit': '창고 서비스 수익',
        'total_ship_profit': '배송비 수익',
        'total_revenue': '매출',
        'total_rev_buy': '구매대행 매출',
        'total_rev_storage': '창고 서비스 매출',
        'total_rev_ship': '배송비 매출',
    };

    const recalculatedSuiteStats = {};
    const profitSuiteKeys = new Set(['total_profit', 'total_buy_profit', 'total_storage_profit', 'total_ship_profit']);
    Object.keys(suiteMetrics).forEach(key => {
        const relevantSuites = profitSuiteKeys.has(key)
            ? filteredSuites.filter(s => (s.profit_pkg_count || 0) > 0)
            : filteredSuites;
        const values = relevantSuites.map(s => s[key] || 0);
        recalculatedSuiteStats[key] = calculateStats(values);
    });


    // B. Case 단위 요약 (Suite별 case_stats 기반 완벽 재계산)
    // Python에서 미리 계산된 데이터 사용 (성능 및 정확도 중심)
    const targetCaseSummary = excludeAdmin ? dashboardData.case_summary_no_admin : dashboardData.case_summary;

    targetCaseSummary.forEach(item => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td style="font-weight: 600; color: #fbbf24;">${CASE_NAMES[item.source_case] || item.source_case}</td>
            <td>${item.package_id.toLocaleString()} 건</td>
            <td style="color: #a78bfa;">${(item.profit_pkg_count || 0).toLocaleString()} 건</td>
            <td>${(item.suite_count || 0).toLocaleString()} 명</td>
            <td style="color: #a78bfa;">${(item.profit_suite_count || 0).toLocaleString()} 명</td>
            <td style="color: #22c55e; font-weight: 600;">${formatCurrency(item.profit_krw, item.profit_usd)}</td>
            <td style="color: #22c55e;">${formatCurrency(item.goods_profit_krw, item.goods_profit_usd)}</td>
            <td style="color: #22c55e;">${formatCurrency(item.warehouse_profit_krw, item.warehouse_profit_usd)}</td>
            <td style="color: #22c55e;">${formatCurrency(item.shipping_profit_krw, item.shipping_profit_usd)}</td>
            <td style="color: #fbbf24; font-weight: 600;">${formatCurrency(item.profit_per_pkg, item.profit_per_pkg_usd)}</td>
            <td style="color: #60a5fa; font-weight: 600;">${formatCurrency(item.profit_per_suite, item.profit_per_suite_usd)}</td>
            <td>${formatCurrency(item.revenue_krw, item.revenue_usd)}</td>
            <td>${formatCurrency(item.revenue_buy_krw, item.revenue_buy_usd)}</td>
            <td>${formatCurrency(item.revenue_storage_krw, item.revenue_storage_usd)}</td>
            <td>${formatCurrency(item.revenue_ship_krw, item.revenue_ship_usd)}</td>
            <td style="color: #f97316;">${formatCurrency(item.revenue_krw / (item.package_id || 1), item.revenue_usd / (item.package_id || 1))}</td>
            <td style="color: #38bdf8;">${formatCurrency(item.revenue_krw / (item.suite_count || 1), item.revenue_usd / (item.suite_count || 1))}</td>
        `;
        caseBody.appendChild(row);
    });

    // 현재 선택된 케이스 차트 업데이트 (필터링 반영)
    const activeCaseBtn = document.querySelector('.case-btn.active');
    if (activeCaseBtn) {
        renderCaseChart(activeCaseBtn.getAttribute('data-case'));
    }

    // C. 패키지 단위 요약 (Python에서 미리 계산된 정밀 데이터 사용)
    const pkgMetricsMap = {
        'profit_krw': '수익',
        'goods_profit_krw': '구매대행 수익',
        'warehouse_profit_krw': '창고 서비스 수익',
        'shipping_profit_krw': '배송비 수익',
        'revenue_krw': '매출',
        'revenue_buy_krw': '구매대행 매출',
        'revenue_storage_krw': '창고 서비스 매출',
        'revenue_ship_krw': '배송비 매출',
    };

    // 필터 상태에 따라 미리 계산된 스탯 선택
    const targetPkgStats = excludeAdmin ? dashboardData.pkg_stats_no_admin : dashboardData.pkg_stats;

    // 명확한 타겟팅과 단위 전달 (Defensive render)
    renderTable('pkg-summary-body', targetPkgStats, pkgMetricsMap, '#ffffff', '건');
    renderTable('suite-summary-body', recalculatedSuiteStats, suiteMetrics, '#ffffff', '명');
}

// 실시간 기술통계 계산 함수
function calculateStats(arr) {
    if (arr.length === 0) return { count: 0, sum: 0, mean: 0, min: 0, '25%': 0, '50%': 0, '75%': 0, max: 0 };
    const sorted = [...arr].sort((a, b) => a - b);
    const sum = sorted.reduce((a, b) => a + b, 0);
    const getPercentile = (p) => {
        const idx = (sorted.length - 1) * p;
        const base = Math.floor(idx);
        const rest = idx - base;
        if (sorted[base + 1] !== undefined) return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
        return sorted[base];
    };
    return {
        count: sorted.length,
        sum: sum,
        mean: sum / sorted.length,
        min: sorted[0],
        '25%': getPercentile(0.25),
        '50%': getPercentile(0.5),
        '75%': getPercentile(0.75),
        max: sorted[sorted.length - 1]
    };
}

function renderSummaryTables(data) {
    const adminToggle = document.getElementById('exclude-admin');
    updateDashboard(adminToggle ? adminToggle.checked : false);
}

function updateOverallCharts(kpis) {
    _lastKpis = kpis;
    const overallCtx = document.getElementById('revenuePieChart').getContext('2d');

    // 기존 차트 파괴 (캔버스 재사용)
    let chartStatus = Chart.getChart("revenuePieChart");
    if (chartStatus) chartStatus.destroy();

    const isUSD = window.currencyMode === 'USD';
    const isProfit = window.chartViewMode === 'profit';

    let buyVal, storageVal, shipVal;
    if (isProfit) {
        buyVal     = isUSD ? kpis.we_buy_profit_usd   : kpis.we_buy_profit;
        storageVal = isUSD ? kpis.storage_profit_usd   : kpis.storage_profit;
        shipVal    = isUSD ? kpis.ship_profit_usd      : kpis.ship_profit;
    } else {
        buyVal     = isUSD ? kpis.total_buy_revenue_usd     : kpis.total_buy_revenue;
        storageVal = isUSD ? kpis.total_storage_revenue_usd : kpis.total_storage_revenue;
        shipVal    = isUSD ? kpis.total_ship_revenue_usd    : kpis.total_ship_revenue;
    }

    new Chart(overallCtx, {
        type: 'doughnut',
        data: {
            labels: ['Buy (구매)', 'Storage (창고)', 'Ship (배송)'],
            datasets: [{
                data: [buyVal, storageVal, shipVal],
                backgroundColor: ['#fbbf24', '#10b981', '#3b82f6'],
                borderWidth: 0, hoverOffset: 15
            }]
        },
        options: getChartOptions(buyVal + storageVal + shipVal)
    });

    // 케이스 차트는 첫 로드 시 이미 렌더링됨 (Case 1 기본값)
    if (!caseChart) {
        renderCaseChart('Case 1');
    }
}

function renderTable(bodyId, statsData, metricsMap, titleColor, countUnit = '명') {
    const body = document.getElementById(bodyId);
    if (!body) return;

    // 중복 방지를 위해 렌더링 전 항상 초기화
    body.innerHTML = '';

    Object.keys(metricsMap).forEach(key => {
        if (!statsData[key]) return;
        const stats = statsData[key];
        const row = document.createElement('tr');
        const isVol = key.includes('package') || key.includes('id');

        row.innerHTML = `
            <td style="font-weight: 600; color: ${titleColor};">${metricsMap[key]}</td>
            <td style="white-space: nowrap;">${stats.count.toLocaleString()} ${countUnit}</td>
            <td style="font-weight: 700; color: #fbbf24;">${isVol ? stats.sum.toLocaleString() + ' 건' : formatCurrency(stats.sum)}</td>
            <td>${isVol ? Math.round(stats.mean).toLocaleString() + ' 건' : formatCurrency(stats.mean)}</td>
            <td>${isVol ? Math.round(stats.min).toLocaleString() + ' 건' : formatCurrency(stats.min)}</td>
            <td style="color: #94a3b8;">${isVol ? Math.round(stats['25%']).toLocaleString() + ' 건' : formatCurrency(stats['25%'])}</td>
            <td>${isVol ? Math.round(stats['50%']).toLocaleString() + ' 건' : formatCurrency(stats['50%'])}</td>
            <td style="color: #94a3b8;">${isVol ? Math.round(stats['75%']).toLocaleString() + ' 건' : formatCurrency(stats['75%'])}</td>
            <td>${isVol ? Math.round(stats.max).toLocaleString() + ' 건' : formatCurrency(stats.max)}</td>
        `;
        body.appendChild(row);
    });
}

function renderCharts() {
    console.log('Initializing case chart buttons...');
    document.querySelectorAll('.case-btn').forEach(btn => {
        // 기존 리스너 중복 방지를 위해 복제 후 교체 (선택사항이나 안전함)
        const newBtn = btn.cloneNode(true);
        btn.parentNode.replaceChild(newBtn, btn);

        newBtn.addEventListener('click', (e) => {
            const caseName = e.currentTarget.getAttribute('data-case');
            console.log(`Case button clicked: ${caseName}`);

            document.querySelectorAll('.case-btn').forEach(b => b.classList.remove('active'));
            e.currentTarget.classList.add('active');

            renderCaseChart(caseName);
        });
    });
}

function renderCaseChart(caseName) {
    _lastCaseName = caseName;
    const ctx = document.getElementById('caseRevenuePieChart').getContext('2d');
    const adminToggle = document.getElementById('exclude-admin');
    const excludeAdmin = adminToggle ? adminToggle.checked : false;

    const isUSD = window.currencyMode === 'USD';
    const isProfit = window.chartViewMode === 'profit';

    let buyVal, storageVal, shipVal;
    if (isProfit) {
        const summarySource = excludeAdmin ? dashboardData.case_summary_no_admin : dashboardData.case_summary;
        const caseData = summarySource.find(c => c.source_case === caseName);
        if (!caseData) return;
        buyVal     = isUSD ? (caseData.goods_profit_usd     || 0) : (caseData.goods_profit_krw     || 0);
        storageVal = isUSD ? (caseData.warehouse_profit_usd || 0) : (caseData.warehouse_profit_krw || 0);
        shipVal    = isUSD ? (caseData.shipping_profit_usd  || 0) : (caseData.shipping_profit_krw  || 0);
    } else {
        const splitSource = excludeAdmin ? dashboardData.case_revenue_splits_no_admin : dashboardData.case_revenue_splits;
        const split = splitSource[caseName];
        if (!split) {
            console.warn(`No revenue split data found for ${caseName}`);
            return;
        }
        buyVal     = isUSD ? (split.buy_usd     || 0) : split.buy;
        storageVal = isUSD ? (split.storage_usd || 0) : split.storage;
        shipVal    = isUSD ? (split.ship_usd    || 0) : split.ship;
    }

    if (caseChart) caseChart.destroy();

    caseChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Buy (구매)', 'Storage (창고)', 'Ship (배송)'],
            datasets: [{
                data: [buyVal, storageVal, shipVal],
                backgroundColor: ['#fbbf24', '#10b981', '#3b82f6'],
                borderWidth: 0, hoverOffset: 15
            }]
        },
        options: getChartOptions(buyVal + storageVal + shipVal)
    });
}

function getChartOptions(total) {
    const prefix = window.currencyMode === 'USD' ? '$ ' : '₩ ';
    return {
        responsive: true, maintainAspectRatio: false, cutout: '65%',
        layout: { padding: { top: 90, right: 90, bottom: 200, left: 90 } },
        plugins: {
            legend: { position: 'bottom', labels: { color: '#f8fafc', padding: 20, font: { size: 12 } } },
            tooltip: { callbacks: { label: (ctx) => ` ${prefix}${Math.round(ctx.raw).toLocaleString()} (${((ctx.raw / total) * 100).toFixed(1)}%)` } },
            datalabels: {
                anchor: 'end',
                align: 'end',
                offset: 10,
                clip: false,
                color: '#f8fafc',
                font: { size: 13, weight: 'bold' },
                formatter: (value, ctx) => {
                    if (!value || value <= 0) return null;
                    const pct = ((value / total) * 100).toFixed(1);
                    return `${prefix}${Math.round(value).toLocaleString()} (${pct}%)`;
                },
                display: (ctx) => (ctx.dataset.data[ctx.dataIndex] || 0) > 0
            }
        }
    };
}

function initSearch() {
    const searchInput = document.getElementById('suite-search');
    const resultsContainer = document.getElementById('search-results');

    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.toUpperCase();
        if (query.length < 2) {
            resultsContainer.style.display = 'none';
            return;
        }

        const adminToggle = document.getElementById('exclude-admin');
        const excludeAdmin = adminToggle ? adminToggle.checked : false;
        let filtered = dashboardData.all_suites.filter(s => s.suite_number.includes(query));

        if (excludeAdmin) {
            filtered = filtered.filter(s => !ADMIN_SUITES.includes(s.suite_number));
        }

        filtered = filtered.slice(0, 10);

        if (filtered.length > 0) {
            resultsContainer.innerHTML = filtered.map(s => `
                <div class="search-item" onclick="showSuiteDetail('${s.suite_number}')">
                    <span>${s.suite_number}</span>
                    <span style="color: #22c55e; font-size: 11px;">${formatCurrency(s.total_profit, s.total_profit_usd)}</span>
                </div>
            `).join('');
            resultsContainer.style.display = 'block';
        } else {
            resultsContainer.style.display = 'none';
        }
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.search-container')) {
            resultsContainer.style.display = 'none';
        }
    });
}

function showSuiteDetail(suiteNum) {
    window.location.href = `customer_profile.html?suite=${suiteNum}`;
}

function initModal() {
    const suiteModal = document.getElementById('suite-modal');
    const closeBtns = document.querySelectorAll('.close-btn');

    closeBtns.forEach(btn => {
        btn.onclick = () => {
            if (suiteModal) suiteModal.style.display = 'none';
        };
    });

    window.onclick = (e) => {
        if (e.target == suiteModal) suiteModal.style.display = 'none';
    };
}

document.addEventListener('DOMContentLoaded', initDashboard);
