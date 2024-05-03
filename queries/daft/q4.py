from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 4

def q() -> None:
    orders = get_df("orders")
    lineitems = get_df("lineitem")

    orders = orders.where(
        (col("o_orderdate") >= datetime.date(1993, 7, 1)) & (col("o_orderdate") < datetime.date(1993, 10, 1))
    )

    lineitems = lineitems.where(col("l_commitdate") < col("l_receiptdate")).select(col("l_orderkey")).distinct()

    daft_df = (
        lineitems.join(orders, left_on=col("l_orderkey"), right_on=col("o_orderkey"))
        .groupby(col("o_orderpriority"))
        .agg(col("l_orderkey").count().alias("order_count"))
        .sort(col("o_orderpriority"))
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()

