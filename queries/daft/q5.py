from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 5

def q() -> None:
    orders = get_df("orders").where(
        (col("o_orderdate") >= datetime.date(1994, 1, 1)) & (col("o_orderdate") < datetime.date(1995, 1, 1))
    )
    region = get_df("region").where(col("r_name") == "ASIA")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    daft_df = (
        region.join(nation, left_on=col("r_regionkey"), right_on=col("n_regionkey"))
        .join(supplier, left_on=col("n_nationkey"), right_on=col("s_nationkey"))
        .join(lineitem, left_on=col("s_suppkey"), right_on=col("l_suppkey"))
        .select(col("n_name"), col("l_extendedprice"), col("l_discount"), col("l_orderkey"), col("n_nationkey"))
        .join(orders, left_on=col("l_orderkey"), right_on=col("o_orderkey"))
        .join(customer, left_on=[col("o_custkey"), col("n_nationkey")], right_on=[col("c_custkey"), col("c_nationkey")])
        .select(col("n_name"), (col("l_extendedprice") * (1 - col("l_discount"))).alias("value"))
        .groupby(col("n_name"))
        .agg(col("value").sum().alias("revenue"))
        .sort(col("revenue"), desc=True)
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()

