from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 6

def q() -> None:
    lineitem = get_df("lineitem")
    daft_df = lineitem.where(
        (col("l_shipdate") >= datetime.date(1994, 1, 1))
        & (col("l_shipdate") < datetime.date(1995, 1, 1))
        & (col("l_discount") >= 0.05)
        & (col("l_discount") <= 0.07)
        & (col("l_quantity") < 24)
    ).sum(col("l_extendedprice") * col("l_discount"))

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()


