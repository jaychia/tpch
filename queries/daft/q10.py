from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 10

def q() -> None:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(col("l_returnflag") == "R")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

    daft_df = (
        orders.where(
            (col("o_orderdate") < datetime.date(1994, 1, 1)) & (col("o_orderdate") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=col("o_custkey"), right_on=col("c_custkey"))
        .join(nation, left_on=col("c_nationkey"), right_on=col("n_nationkey"))
        .join(lineitem, left_on=col("o_orderkey"), right_on=col("l_orderkey"))
        .select(
            col("o_custkey"),
            col("c_name"),
            decrease(col("l_extendedprice"), col("l_discount")).alias("volume"),
            col("c_acctbal"),
            col("n_name"),
            col("c_address"),
            col("c_phone"),
            col("c_comment"),
        )
        .groupby(
            col("o_custkey"),
            col("c_name"),
            col("c_acctbal"),
            col("c_phone"),
            col("n_name"),
            col("c_address"),
            col("c_comment"),
        )
        .agg(col("volume").sum().alias("revenue"))
        .sort(col("revenue"), desc=True)
        .select(
            col("o_custkey"),
            col("c_name"),
            col("revenue"),
            col("c_acctbal"),
            col("n_name"),
            col("c_address"),
            col("c_phone"),
            col("c_comment"),
        )
        .limit(20)
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()
