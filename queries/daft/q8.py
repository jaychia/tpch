from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 8

def q() -> None:
    def decrease(x, y):
        return x * (1 - y)

    region = get_df("region").where(col("r_name") == "AMERICA")
    orders = get_df("orders").where(
        (col("o_orderdate") <= datetime.date(1996, 12, 31)) & (col("o_orderdate") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part").where(col("p_type") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    nat = nation.join(supplier, left_on=col("n_nationkey"), right_on=col("s_nationkey"))

    line = (
        lineitem.select(
            col("l_partkey"),
            col("l_suppkey"),
            col("l_orderkey"),
            decrease(col("l_extendedprice"), col("l_discount")).alias("volume"),
        )
        .join(part, left_on=col("l_partkey"), right_on=col("p_partkey"))
        .join(nat, left_on=col("l_suppkey"), right_on=col("s_suppkey"))
    )

    daft_df = (
        nation.join(region, left_on=col("n_regionkey"), right_on=col("r_regionkey"))
        .select(col("n_nationkey"))
        .join(customer, left_on=col("n_nationkey"), right_on=col("c_nationkey"))
        .select(col("c_custkey"))
        .join(orders, left_on=col("c_custkey"), right_on=col("o_custkey"))
        .select(col("o_orderkey"), col("o_orderdate"))
        .join(line, left_on=col("o_orderkey"), right_on=col("l_orderkey"))
        .select(
            col("o_orderdate").dt.year().alias("o_year"),
            col("volume"),
            (col("n_name") == "BRAZIL").if_else(col("volume"), 0.0).alias("case_volume"),
        )
        .groupby(col("o_year"))
        .agg(col("case_volume").sum().alias("case_volume_sum"), col("volume").sum().alias("volume_sum"))
        .select(col("o_year"), col("case_volume_sum") / col("volume_sum"))
        .sort(col("o_year"))
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()
