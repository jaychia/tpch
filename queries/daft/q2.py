from __future__ import annotations

from daft import col
import datetime

from queries.daft import utils
from queries.daft.utils import get_df

Q_NUM = 2

def q() -> None:
    region = get_df("region")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    part = get_df("part")

    europe = (
        region.where(col("r_name") == "EUROPE")
        .join(nation, left_on=col("r_regionkey"), right_on=col("n_regionkey"))
        .join(supplier, left_on=col("n_nationkey"), right_on=col("s_nationkey"))
        .join(partsupp, left_on=col("s_suppkey"), right_on=col("ps_suppkey"))
    )

    brass = part.where((col("p_size") == 15) & col("p_type").str.endswith("BRASS")).join(
        europe,
        left_on=col("p_partkey"),
        right_on=col("ps_partkey"),
    )
    min_cost = brass.groupby(col("p_partkey")).agg(col("ps_supplycost").min().alias("min"))

    daft_df = (
        brass.join(min_cost, on=col("p_partkey"))
        .where(col("ps_supplycost") == col("min"))
        .select(
            col("s_acctbal"),
            col("s_name"),
            col("n_name"),
            col("p_partkey"),
            col("p_mfgr"),
            col("s_address"),
            col("s_phone"),
            col("s_comment"),
        )
        .sort(by=["s_acctbal", "n_name", "s_name", "p_partkey"], desc=[True, False, False, False])
        .limit(100)
    )

    utils.run_query(Q_NUM, daft_df)


if __name__ == "__main__":
    q()

