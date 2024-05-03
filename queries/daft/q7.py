from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 7

def q() -> None:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(
        (col("l_shipdate") >= datetime.date(1995, 1, 1)) & (col("l_shipdate") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation").where((col("n_name") == "FRANCE") | (col("n_name") == "GERMANY"))
    supplier = get_df("supplier")
    customer = get_df("customer")
    orders = get_df("orders")

    supNation = (
        nation.join(supplier, left_on=col("n_nationkey"), right_on=col("s_nationkey"))
        .join(lineitem, left_on=col("s_suppkey"), right_on=col("l_suppkey"))
        .select(
            col("n_name").alias("supp_nation"),
            col("l_orderkey"),
            col("l_extendedprice"),
            col("l_discount"),
            col("l_shipdate"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=col("n_nationkey"), right_on=col("c_nationkey"))
        .join(orders, left_on=col("c_custkey"), right_on=col("o_custkey"))
        .select(col("n_name").alias("cust_nation"), col("o_orderkey"))
        .join(supNation, left_on=col("o_orderkey"), right_on=col("l_orderkey"))
        .where(
            ((col("supp_nation") == "FRANCE") & (col("cust_nation") == "GERMANY"))
            | ((col("supp_nation") == "GERMANY") & (col("cust_nation") == "FRANCE"))
        )
        .select(
            col("supp_nation"),
            col("cust_nation"),
            col("l_shipdate").dt.year().alias("l_year"),
            decrease(col("l_extendedprice"), col("l_discount")).alias("volume"),
        )
        .groupby(col("supp_nation"), col("cust_nation"), col("l_year"))
        .agg(col("volume").sum().alias("revenue"))
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()
