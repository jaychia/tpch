from __future__ import annotations

from typing import TYPE_CHECKING, Any

from queries.common_utils import (
    check_query_result_pd,
    get_table_path,
    run_query_generic,
)
from settings import Settings

settings = Settings()

import daft


def get_df(table_name: str) -> daft.DataFrame:

    path = str(get_table_path(table_name))

    if settings.run.io_type == "parquet":
        df = daft.read_parquet(path)
    elif settings.run.io_type == "csv":
        df = daft.read_csv(path)
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)
    
    if settings.run.io_type == "skip":
        df.collect()

    return df


def run_query(query_number: int, query: daft.DataFrame) -> None:
    callable_query = query.to_pandas
    run_query_generic(callable_query, query_number, "daft", query_checker=check_query_result_pd)
