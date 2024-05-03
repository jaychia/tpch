from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 3

def q() -> None:
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer").where(col("c_mktsegment") == "BUILDING")
    orders = get_df("orders").where(col("o_orderdate") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem").where(col("l_shipdate") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=col("c_custkey"), right_on=col("o_custkey"))
        .select(col("o_orderkey"), col("o_orderdate"), col("o_shippriority"))
        .join(lineitem, left_on=col("o_orderkey"), right_on=col("l_orderkey"))
        .select(
            col("o_orderkey"),
            decrease(col("l_extendedprice"), col("l_discount")).alias("volume"),
            col("o_orderdate"),
            col("o_shippriority"),
        )
        .groupby(col("o_orderkey"), col("o_orderdate"), col("o_shippriority"))
        .agg(col("volume").sum().alias("revenue"))
        .sort(by=["revenue", "o_orderdate"], desc=[True, False])
        .limit(10)
        .select("o_orderkey", "revenue", "o_orderdate", "o_shippriority")
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()

