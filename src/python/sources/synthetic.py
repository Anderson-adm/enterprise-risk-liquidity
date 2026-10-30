"""Deterministic synthetic data for the seven sources.

Everything here is invented: names, codes and amounts are generated from a fixed seed.
Instrument codes use a ``SYN-`` prefix so they cannot be mistaken for real ISINs/tickers.

Intentional, controlled problems (see docs/DATA_MODEL.md for the expected outcome of each):

1. Divergent identifiers  - each system names portfolios differently (``prt0001`` /
   ``C000101`` / ``FND001/01`` / ``ADM77:EXT77-0001``); resolved through source_identity_map.
2. Duplicate record        - D2: market data line repeated; risk DB duplicate position row.
3. Unmapped portfolio      - risk DB positions for ``prt0099`` (no registry entry).
4. Invalid XML             - D2: truncated internal XML; external XML failing XSD (PRT-0008).
5. Late reference date     - D3 inbox carries the corrected PRT-0008 statement for D2.
6. Correction of a processed position - D2: PRT-0002 position reprocessed (version 2);
   ``erl scenario correct-position`` adds another version after publication.
7. Re-execution of the same load - handled by checksums/idempotent publication (no data needed).
8. Source unavailable       - ``erl scenario remove-market-data`` removes a day's delivery.
"""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal

DEFAULT_DATES = (date(2026, 10, 26), date(2026, 10, 27), date(2026, 10, 28))
SEED = 20261026

CENT = Decimal("0.01")
MICRO = Decimal("0.000001")

FUNDS = [
    ("FND-001", "Synthetic Multimarket Alpha", "MULTIMARKET"),
    ("FND-002", "Synthetic Fixed Income Prime", "FIXED_INCOME"),
    ("FND-003", "Synthetic Equity Value", "EQUITY"),
    ("FND-004", "Synthetic Transfer-In Mandates", "MULTIMARKET"),
]
# portfolio_id, fund_id, position source of record, compliance ref, legacy code
PORTFOLIOS = [
    ("PRT-0001", "FND-001", "portfolio_risk", "FND001/01", "C000101"),
    ("PRT-0002", "FND-001", "portfolio_risk", "FND001/02", "C000102"),
    ("PRT-0003", "FND-002", "portfolio_risk", "FND002/01", "C000103"),
    ("PRT-0004", "FND-003", "portfolio_risk", "FND003/01", "C000104"),
    ("PRT-0005", "FND-002", "portfolio_risk", "FND002/02", "C000105"),
    ("PRT-0006", "FND-003", "internal_xml", "FND003/02", "C000106"),
    ("PRT-0007", "FND-004", "external_xml", "FND004/01", "C000107"),
    ("PRT-0008", "FND-004", "external_xml", "FND004/02", "C000108"),
]
EXTERNAL_IDS = {"PRT-0007": ("ADM77", "EXT77-0001"), "PRT-0008": ("ADM77", "EXT77-0002")}
UNMAPPED_RISK_CODE = "prt0099"

# instrument_code, type, issuer_code, issuer_type, base price, avg daily volume (units)
INSTRUMENTS = [
    ("SYN-GB-001", "GOV_BOND", "TSY-SYN", "SOVEREIGN", "1012.35", "900000"),
    ("SYN-GB-002", "GOV_BOND", "TSY-SYN", "SOVEREIGN", "987.10", "750000"),
    ("SYN-GB-003", "GOV_BOND", "TSY-SYN", "SOVEREIGN", "1150.42", "500000"),
    ("SYN-GB-004", "GOV_BOND", "TSY-SYN", "SOVEREIGN", "1003.00", "650000"),
    ("SYN-EQ-001", "EQUITY", "ISS-A", "CORPORATE", "35.20", "2500000"),
    ("SYN-EQ-002", "EQUITY", "ISS-B", "CORPORATE", "18.75", "40000"),
    ("SYN-EQ-003", "EQUITY", "ISS-C", "CORPORATE", "52.10", "1200000"),
    ("SYN-EQ-004", "EQUITY", "ISS-D", "CORPORATE", "9.85", "15000"),
    ("SYN-EQ-005", "EQUITY", "ISS-E", "CORPORATE", "27.40", "800000"),
    ("SYN-EQ-006", "EQUITY", "ISS-F", "CORPORATE", "61.30", "300000"),
    ("SYN-CB-001", "CORP_BOND", "ISS-A", "CORPORATE", "1020.00", "1500"),
    ("SYN-CB-002", "CORP_BOND", "ISS-G", "CORPORATE", "998.50", "900"),
    ("SYN-CB-003", "CORP_BOND", "ISS-H", "CORPORATE", "1045.20", "600"),
    ("SYN-CB-004", "CORP_BOND", "ISS-I", "CORPORATE", "1001.75", "1200"),
    ("SYN-FQ-001", "FUND_QUOTA", "ISS-J", "FUND", "1.452310", "0"),
    ("SYN-FQ-002", "FUND_QUOTA", "ISS-K", "FUND", "2.118400", "0"),
]

# canonical holdings: portfolio -> {instrument: quantity}
HOLDINGS = {
    "PRT-0001": {
        "SYN-GB-001": "2000",
        "SYN-EQ-001": "15000",
        "SYN-EQ-003": "12000",
        "SYN-CB-001": "300",
        "SYN-FQ-001": "250000",
    },
    "PRT-0002": {"SYN-GB-002": "2000", "SYN-EQ-005": "20000", "SYN-EQ-006": "8000", "SYN-CB-002": "250"},
    "PRT-0003": {"SYN-GB-001": "2500", "SYN-GB-003": "1500", "SYN-CB-003": "900", "SYN-CB-004": "700"},
    # PRT-0004: heavy single-issuer (ISS-B) equity concentration -> the only synthetic limit breach
    "PRT-0004": {"SYN-EQ-002": "160000", "SYN-EQ-004": "90000", "SYN-EQ-001": "20000", "SYN-GB-004": "400"},
    "PRT-0005": {"SYN-GB-002": "1800", "SYN-GB-004": "1000", "SYN-CB-001": "350", "SYN-FQ-002": "120000"},
    "PRT-0006": {"SYN-EQ-003": "8000", "SYN-EQ-006": "6000", "SYN-EQ-005": "10000", "SYN-GB-003": "1500"},
    "PRT-0007": {"SYN-GB-001": "1500", "SYN-EQ-001": "10000", "SYN-EQ-003": "5000", "SYN-CB-002": "150"},
    "PRT-0008": {"SYN-GB-004": "1500", "SYN-EQ-005": "12000"},
}
CORRECTED_QTY_D2 = "21500"  # PRT-0002 / SYN-EQ-005 after reprocessing on D2 (was 20000)
CASH = {
    "PRT-0001": "1250000.00",
    "PRT-0002": "430000.00",
    "PRT-0003": "2100000.00",
    "PRT-0004": "85000.00",
    "PRT-0005": "960000.00",
    "PRT-0006": "310000.00",
    "PRT-0007": "150000.00",
    "PRT-0008": "72000.00",
}


def q2(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_HALF_EVEN)


def q6(x: Decimal) -> Decimal:
    return x.quantize(MICRO, rounding=ROUND_HALF_EVEN)


def risk_code(portfolio_id: str) -> str:
    return portfolio_id.replace("-", "").lower()  # PRT-0001 -> prt0001


@dataclass
class SyntheticDataset:
    dates: tuple[date, ...]
    tables: dict[str, dict[str, list[dict]]] = field(default_factory=dict)  # source -> table -> rows
    files: list[tuple[str, date, str, bytes]] = field(
        default_factory=list
    )  # (source, delivery date, name, body)


def prices_for(dates: tuple[date, ...], rng: random.Random) -> dict[date, dict[str, Decimal]]:
    out: dict[date, dict[str, Decimal]] = {}
    current = {code: Decimal(p) for code, _t, _i, _it, p, _v in INSTRUMENTS}
    for d in dates:
        for code in current:
            drift = Decimal(rng.randint(-150, 150)) / Decimal(10000)  # +-1.5 %
            current[code] = q6(current[code] * (1 + drift))
        out[d] = dict(current)
    return out


def scenario_dates(dates: tuple[date, ...]) -> tuple[date, date | None, date | None]:
    """(D1, D2, D3) roles of the requested dates; a role without a requested date is ``None``."""
    if not dates:
        raise ValueError("at least one date is required")
    if any(b <= a for a, b in itertools.pairwise(dates)):
        raise ValueError(f"dates must be strictly increasing, got {[d.isoformat() for d in dates]}")
    roles = (*dates[:3], None, None)
    return roles[0], roles[1], roles[2]


def generate(dates: tuple[date, ...] = DEFAULT_DATES, seed: int = SEED) -> SyntheticDataset:
    d1, d2, d3 = scenario_dates(dates)  # comparisons with a missing role (None) are False
    rng = random.Random(seed)
    ds = SyntheticDataset(dates=dates)
    px = prices_for(dates, rng)
    epoch = date(2020, 1, 1)

    # ---- source 1: fund_ops -----------------------------------------------------------
    ds.tables["fund_ops"] = {
        "fund": [
            {
                "fund_id": f,
                "fund_name": n,
                "fund_type": t,
                "base_currency": "BRL",
                "inception_date": date(2019, 3, 1),
                "status": "ACTIVE",
            }
            for f, n, t in FUNDS
        ],
        "portfolio": [
            {
                "portfolio_id": p,
                "fund_id": f,
                "portfolio_name": f"Synthetic portfolio {p[-2:]}",
                "position_source_system": src,
                "status": "ACTIVE",
                "opened_on": date(2019, 6, 3),
            }
            for p, f, src, _c, _l in PORTFOLIOS
        ],
        "source_identity_map": _identity_map(epoch),
    }

    # ---- source 2: portfolio_risk -----------------------------------------------------
    positions, batches = [], []
    pid = 1
    for bi, d in enumerate(dates, start=1):
        started = datetime(d.year, d.month, d.day, 19, 0, 0)
        batches.append(
            {
                "batch_id": bi,
                "reference_date": d,
                "status": "COMPLETED",
                "started_at": started,
                "finished_at": started + timedelta(minutes=42),
            }
        )
        for p, _f, src, _c, _l in PORTFOLIOS:
            if src != "portfolio_risk":
                continue
            for inst, qty in HOLDINGS[p].items():
                qty_d = Decimal(qty)
                positions.append(_risk_row(pid, risk_code(p), inst, d, qty_d, px[d][inst], 1, started))
                pid += 1
        # unmapped portfolio (scenario 3)
        for inst, qty in (("SYN-EQ-001", "1000"), ("SYN-GB-002", "50")):
            positions.append(
                _risk_row(pid, UNMAPPED_RISK_CODE, inst, d, Decimal(qty), px[d][inst], 1, started)
            )
            pid += 1
        if d == d2:
            # scenario 6: PRT-0002 / SYN-EQ-005 reprocessed with corrected quantity (version 2)
            positions.append(
                _risk_row(
                    pid,
                    risk_code("PRT-0002"),
                    "SYN-EQ-005",
                    d,
                    Decimal(CORRECTED_QTY_D2),
                    px[d]["SYN-EQ-005"],
                    2,
                    started + timedelta(hours=3),
                    "REPROCESSED",
                )
            )
            pid += 1
            # scenario 2: exact duplicate of a PRT-0003 position (same key and version, new id)
            positions.append(
                _risk_row(
                    pid,
                    risk_code("PRT-0003"),
                    "SYN-CB-003",
                    d,
                    Decimal("900"),
                    px[d]["SYN-CB-003"],
                    1,
                    started,
                )
            )
            pid += 1
    ds.tables["portfolio_risk"] = {"processing_batch": batches, "processed_position": positions}

    # ---- source 3: legacy_finance -----------------------------------------------------
    ds.tables["legacy_finance"] = {
        "tb_cart": [
            {"cod_cart": leg, "nm_cart": f"CART SINT {leg[-3:]}", "dt_abert": date(2019, 6, 3)}
            for _p, _f, _s, _c, leg in PORTFOLIOS
        ],
        "tb_saldo_caixa": [
            {
                "cod_cart": leg,
                "dt_ref": d,
                "vl_saldo": q2(Decimal(CASH[p]) * (1 + Decimal(i) / 100)),
                "cd_moeda": "BRL",
            }
            for i, d in enumerate(dates)
            for p, _f, _s, _c, leg in PORTFOLIOS
        ],
        "tb_resgate": [
            {
                "id_resgate": 1,
                "cod_cart": "C000101",
                "dt_pedido": d1 - timedelta(days=2),
                "dt_cotizacao": d1 + timedelta(days=5),
                "vl_resgate": Decimal("800000.00"),
                "st_resgate": "PENDENTE",
            },
            {
                "id_resgate": 2,
                "cod_cart": "C000103",
                "dt_pedido": d1 - timedelta(days=1),
                "dt_cotizacao": d1 + timedelta(days=4),
                "vl_resgate": Decimal("9500000.00"),
                "st_resgate": "PENDENTE",
            },
            *(
                [
                    {  # requested on D2: does not exist when only D1 is generated
                        "id_resgate": 3,
                        "cod_cart": "C000104",
                        "dt_pedido": d2,
                        "dt_cotizacao": d2 + timedelta(days=3),
                        "vl_resgate": Decimal("1200000.00"),
                        "st_resgate": "PENDENTE",
                    }
                ]
                if d2 is not None
                else []
            ),
            {
                "id_resgate": 4,
                "cod_cart": "C000105",
                "dt_pedido": d1,
                "dt_cotizacao": d1 + timedelta(days=3),
                "vl_resgate": Decimal("5000000.00"),
                "st_resgate": "CANCELADO",
            },  # excluded by the view
        ],
    }

    # ---- source 4: exposure_compliance ------------------------------------------------
    stock, sid = [], 1
    for d in dates:
        for p, _f, _s, cref, _l in PORTFOLIOS:
            for inst, qty in HOLDINGS[p].items():
                qty_d = Decimal(qty)
                if p == "PRT-0002" and inst == "SYN-EQ-005" and d == d2:
                    qty_d = Decimal(CORRECTED_QTY_D2)  # compliance already reflects the risk correction
                if p == "PRT-0003" and inst == "SYN-GB-003" and d == d3:
                    qty_d = Decimal("1450")  # reconciliation mismatch vs risk (scenario: divergence)
                stock.append(
                    {
                        "stock_id": sid,
                        "fund_ref": cref,
                        "instrument_code": inst,
                        "position_date": d,
                        "quantity": q6(qty_d),
                        "book_value": q2(qty_d * px[d][inst]),
                    }
                )
                sid += 1
    ds.tables["exposure_compliance"] = {
        "instrument_registry": [
            {
                "instrument_code": c,
                "instrument_name": f"Synthetic {t.replace('_', ' ').title()} {c[-3:]}",
                "instrument_type": t,
                "issuer_code": i,
                "issuer_name": f"Synthetic issuer {i}",
                "issuer_type": it,
            }
            for c, t, i, it, _p, _v in INSTRUMENTS
        ],
        "stock_position": stock,
        "concentration_limit": [
            {
                "rule_id": "CONC-ISSUER-01",
                "rule_scope": "ISSUER",
                "limit_pct": Decimal("0.250000"),
                "excluded_issuer_type": "SOVEREIGN",
                "valid_from": epoch,
                "valid_to": None,
            },
        ],
    }

    # ---- source 5: market data CSV ----------------------------------------------------
    for d in dates:
        lines = ["reference_date;instrument_code;instrument_type;close_price;avg_daily_volume;currency"]
        for c, t, _i, _it, _p, vol in INSTRUMENTS:
            price = format(px[d][c], "f").replace(".", ",")  # Brazilian decimal comma, as delivered
            lines.append(f"{d.isoformat()};{c};{t};{price};{vol};BRL")
        if d == d2:
            lines.append(lines[5])  # scenario 2: duplicated line
        ds.files.append(("market_data", d, f"market_data_{d:%Y%m%d}.csv", ("\n".join(lines) + "\n").encode()))

    # ---- source 6: internal XML -------------------------------------------------------
    for d in dates:
        body = internal_xml("PRT-0006", d, px[d])
        ds.files.append(("internal_xml", d, f"statement_PRT-0006_{d:%Y%m%d}.xml", body))
        if d == d2:  # scenario 4: truncated resend
            ds.files.append(
                ("internal_xml", d, f"statement_PRT-0006_{d:%Y%m%d}_resend.xml", body[: len(body) // 2])
            )

    # ---- source 7: external transfer-in XML -------------------------------------------
    for d in dates:
        for p in ("PRT-0007", "PRT-0008"):
            admin, ext = EXTERNAL_IDS[p]
            broken = p == "PRT-0008" and d == d2  # scenario 4: fails XSD (missing qty)
            body = external_xml(admin, ext, d, HOLDINGS[p], px[d], drop_qty=broken)
            ds.files.append(("external_xml", d, f"transfer_in_{admin}_{ext}_{d:%Y%m%d}.xml", body))
    if d2 is not None and d3 is not None:  # scenario 5: late PRT-0008 retransmission for D2, on D3
        admin, ext = EXTERNAL_IDS["PRT-0008"]
        body = external_xml(admin, ext, d2, HOLDINGS["PRT-0008"], px[d2])
        ds.files.append(
            ("external_xml", d3, f"transfer_in_{admin}_{ext}_{d2:%Y%m%d}_retransmission.xml", body)
        )
    return ds


def _identity_map(epoch: date) -> list[dict]:
    rows = []
    for p, _f, _s, cref, leg in PORTFOLIOS:
        rows += [
            {"source_system": "portfolio_risk", "external_identifier": risk_code(p).upper()},
            {"source_system": "legacy_finance", "external_identifier": leg},
            {"source_system": "exposure_compliance", "external_identifier": cref},
            {"source_system": "internal_xml", "external_identifier": p},
        ]
        for r in rows[-4:]:
            r |= {"portfolio_id": p, "valid_from": epoch, "valid_to": None}
    for p, (admin, ext) in EXTERNAL_IDS.items():
        rows.append(
            {
                "source_system": "external_xml",
                "external_identifier": f"{admin}:{ext}",
                "portfolio_id": p,
                "valid_from": epoch,
                "valid_to": None,
            }
        )
    return rows


def _risk_row(pid, code, inst, d, qty, price, version, processed_at, status="PROCESSED") -> dict:
    return {
        "position_id": pid,
        "portfolio_code": code,
        "instrument_code": inst,
        "reference_date": d,
        "quantity": q6(qty),
        "market_value": q2(qty * price),
        "currency": "BRL",
        "processing_status": status,
        "processing_version": version,
        "processed_at": processed_at,
    }


def internal_xml(portfolio: str, d: date, prices: dict[str, Decimal]) -> bytes:
    rows = "".join(
        f"<Position><InstrumentCode>{i}</InstrumentCode><Quantity>{q6(Decimal(q))}</Quantity>"
        f"<MarketValue>{q2(Decimal(q) * prices[i])}</MarketValue><Currency>BRL</Currency></Position>"
        for i, q in HOLDINGS[portfolio].items()
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n<PortfolioStatement version="1.0">'
        f"<Header><PortfolioCode>{portfolio}</PortfolioCode><ReferenceDate>{d.isoformat()}</ReferenceDate>"
        f"<GeneratedAt>{d.isoformat()}T21:15:00</GeneratedAt></Header><Positions>{rows}</Positions>"
        "</PortfolioStatement>\n"
    ).encode()


def external_xml(
    admin: str,
    ext: str,
    d: date,
    holdings: dict[str, str],
    prices: dict[str, Decimal],
    *,
    drop_qty: bool = False,
) -> bytes:
    items = []
    for n, (i, q) in enumerate(holdings.items()):
        qty_attr = "" if (drop_qty and n == 0) else f' qty="{q6(Decimal(q))}"'
        items.append(f'<ext:Holding asset="{i}"{qty_attr} price="{prices[i]}" ccy="BRL"/>')
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<ext:TransferInPortfolio xmlns:ext="urn:erl:synthetic:transfer-in:v1" administrator="{admin}">'
        f'<ext:Fund externalId="{ext}"/><ext:AsOf>{d:%d/%m/%Y}</ext:AsOf>'
        f"<ext:Holdings>{''.join(items)}</ext:Holdings></ext:TransferInPortfolio>\n"
    ).encode()
