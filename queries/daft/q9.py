from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 9

def q() -> None:
    lineitem = get_df("lineitem")
    part = get_df("part")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    orders = get_df("orders")

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(col("p_name").str.contains("green")).join(
        lineitem, left_on=col("p_partkey"), right_on=col("l_partkey")
    )
    natsup = nation.join(supplier, left_on=col("n_nationkey"), right_on=col("s_nationkey"))

    daft_df = (
        linepart.join(natsup, left_on=col("l_suppkey"), right_on=col("s_suppkey"))
        .join(partsupp, left_on=[col("l_suppkey"), col("p_partkey")], right_on=[col("ps_suppkey"), col("ps_partkey")])
        .join(orders, left_on=col("l_orderkey"), right_on=col("o_orderkey"))
        .select(
            col("n_name"),
            col("o_orderdate").dt.year().alias("o_year"),
            expr(col("l_extendedprice"), col("l_discount"), col("ps_supplycost"), col("l_quantity")).alias("amount"),
        )
        .groupby(col("n_name"), col("o_year"))
        .agg(col("amount").sum())
        .sort(by=["n_name", "o_year"], desc=[False, True])
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()
