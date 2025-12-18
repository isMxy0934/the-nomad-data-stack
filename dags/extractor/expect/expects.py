from __future__ import annotations

import pandas as pd

from dags.extractor.backfill_specs import BackfillExtractorSpec


def identity(df: pd.DataFrame, *, trade_date: str, spec: BackfillExtractorSpec) -> pd.DataFrame:
    _ = trade_date
    _ = spec
    return df


def dedupe_trade_date_symbol(
    df: pd.DataFrame, *, trade_date: str, spec: BackfillExtractorSpec
) -> pd.DataFrame:
    _ = trade_date
    _ = spec
    if df.empty:
        return df
    if "trade_date" in df.columns and "symbol" in df.columns:
        return df.drop_duplicates(subset=["trade_date", "symbol"], keep="last")
    return df
