from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 1

def q() -> None:
    lineitem = get_df("lineitem")

    discounted_price = col("l_extendedprice") * (1 - col("l_discount"))
    taxed_discounted_price = discounted_price * (1 + col("l_tax"))
    daft_df = (
        lineitem.where(col("l_shipdate") <= datetime.date(1998, 9, 2))
        .groupby(col("l_returnflag"), col("l_linestatus"))
        .agg(
            col("l_quantity").sum().alias("sum_qty"),
            col("l_extendedprice").sum().alias("sum_base_price"),
            discounted_price.sum().alias("sum_disc_price"),
            taxed_discounted_price.sum().alias("sum_charge"),
            col("l_quantity").mean().alias("avg_qty"),
            col("l_extendedprice").mean().alias("avg_price"),
            col("l_discount").mean().alias("avg_disc"),
            col("l_quantity").count().alias("count_order"),
        )
        .sort(["l_returnflag", "l_linestatus"])
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()

