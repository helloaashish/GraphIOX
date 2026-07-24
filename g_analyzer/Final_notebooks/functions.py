import dask.dataframe as dd
import pandas as pd


from pathlib import PurePath #for hash mapping
import networkx as nx

def get_IO_metrics(events):
   io_time = events.groupby("trange","pid","tid").agg({'dur':sum}).groupby("trange").max().sum()
   io_count = events.count()
   io_size = events["size"].sum()
   ipos = io_count/io_time
   bw = io_size/io_time
   return io_time, io_count, io_size, ipos, bw


def compute_io_metrics(
    df3_new: dd.DataFrame,
    result_df1: dd.DataFrame,
    *,
    drop_unmatched: bool = True,
    key_dtype: str = "string",
    compute_result: bool = False,
):
    """
    Compute I/O metrics per (fhash, hostname) for rows in df3_new
    using matching events from result_df1.

    Parameters
    ----------
    df3_new : dask.dataframe.DataFrame
        Must contain columns: ['fhash', 'hostname'].
    result_df1 : dask.dataframe.DataFrame
        Must contain columns: ['fhash','hostname','trange','pid','tid','dur','size'].
    drop_unmatched : bool, default True
        If True, drop df3_new rows that have no matching events.
        If False, keep them (metrics will be NaN).
    key_dtype : str, default "string"
        Dtype to cast the join keys to, to avoid float/string mismatches.
    compute_result : bool, default False
        If True, return a Pandas DataFrame (compute). Otherwise return a Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame or pandas.DataFrame
        df3_new with columns: io_time, io_count, io_size, iops, bw (for matched rows).
    """
    # --- Normalize dtypes on join keys ---
    df3_new = df3_new.assign(
        fhash=df3_new["fhash"].astype(key_dtype),
        hostname=df3_new["hostname"].astype(key_dtype),
    )
    result_df1 = result_df1.assign(
        fhash=result_df1["fhash"].astype(key_dtype),
        hostname=result_df1["hostname"].astype(key_dtype),
    )

    # Ensure numeric columns are numeric
    for col in ["dur", "size"]:
        if col in result_df1.columns:
            result_df1[col] = dd.to_numeric(result_df1[col], errors="coerce").fillna(0)

    # --- Keep only keys that appear in df3_new (reduces shuffle/work) ---
    keys = df3_new[["fhash", "hostname"]].drop_duplicates()
    filtered = result_df1.merge(keys, on=["fhash", "hostname"], how="inner")

    # --- Compute io_time = sum_over_trange( max_{pid,tid} sum(dur) ) ---
    # A) sum dur per (fhash, hostname, trange, pid, tid)
    agg1 = (
        filtered
        .groupby(["fhash", "hostname", "trange", "pid", "tid"], observed=True)[["dur"]]
        .sum()
        .reset_index()
    )

    # B) max dur per (fhash, hostname, trange)
    agg2 = (
        agg1
        .groupby(["fhash", "hostname", "trange"], observed=True)[["dur"]]
        .max()
        .rename(columns={"dur": "dur_max"})
        .reset_index()
    )

    # C) sum of dur_max across tranges → io_time
    io_time = (
        agg2
        .groupby(["fhash", "hostname"], observed=True)[["dur_max"]]
        .sum()
        .rename(columns={"dur_max": "io_time"})
        .reset_index()
    )

    # Counts and sizes per (fhash, hostname)
    io_count = (
        filtered
        .groupby(["fhash", "hostname"], observed=True)
        .size()
        .to_frame("io_count")
        .reset_index()
    )

    io_size = (
        filtered
        .groupby(["fhash", "hostname"], observed=True)[["size"]]
        .sum()
        .rename(columns={"size": "io_size"})
        .reset_index()
    )

    # Combine metrics
    metrics = (
        io_time
        .merge(io_count, on=["fhash", "hostname"], how="outer")
        .merge(io_size, on=["fhash", "hostname"], how="outer")
    )

    # Derived metrics (guard against divide-by-zero)
    metrics = metrics.assign(
        iops=(metrics["io_count"] / metrics["io_time"]).where(metrics["io_time"] > 0, 0.0),
        bw=(metrics["io_size"] / metrics["io_time"]).where(metrics["io_time"] > 0, 0.0),
    )

    # Merge onto df3_new
    out = df3_new.merge(metrics, on=["fhash", "hostname"], how="left")

    # Drop rows with no matching events (NaNs in metrics)
    if drop_unmatched:
        out = out.dropna(subset=["io_time", "io_count", "io_size"])

    if compute_result:
        return out.compute()
    return out




def compute_io_metrics_by_key(
    df_keys: dd.DataFrame,
    result_df1: dd.DataFrame,
    *,
    key_col: str = "hostname",          # use "hostname" or "pid"
    drop_unmatched: bool = True,
    key_dtype: str = "string",
    compute_result: bool = False,
):
    """
    Compute I/O metrics per (fhash, <key_col>) for rows in df_keys using matching events from result_df1.

    Parameters
    ----------
    df_keys : dask.dataframe.DataFrame
        Must contain: ['fhash', key_col].
    result_df1 : dask.dataframe.DataFrame
        Must contain: ['fhash','hostname','trange','pid','tid','dur','size'].
        (When key_col='pid', this function will match on result_df1['pid'] instead of ['hostname'].)
    key_col : {'hostname','pid'}, default 'hostname'
        Secondary key to group/select events.
    drop_unmatched : bool, default True
        Drop rows from df_keys that have no matching events.
    key_dtype : str, default "string"
        Dtype to cast join keys to.
    compute_result : bool, default False
        If True, compute and return a Pandas DataFrame.

    Returns
    -------
    Dask or Pandas DataFrame
        df_keys with columns: io_time, io_count, io_size, iops, bw (for matched rows).
    """
    # --- Normalize dtypes on join keys ---
    df_keys = df_keys.assign(
        fhash=df_keys["fhash"].astype(key_dtype),
        **{key_col: df_keys[key_col].astype(key_dtype)}
    )

    # Cast both potential key columns in result_df1
    result_df1 = result_df1.assign(
        fhash=result_df1["fhash"].astype(key_dtype),
        hostname=result_df1["hostname"].astype(key_dtype),
        pid=result_df1["pid"].astype(key_dtype),
    )

    # Ensure numeric columns are numeric
    for col in ["dur", "size"]:
        if col in result_df1.columns:
            result_df1[col] = dd.to_numeric(result_df1[col], errors="coerce").fillna(0)

    # --- Keep only keys that appear in df_keys (reduces shuffle/work) ---
    keys = df_keys[["fhash", key_col]].drop_duplicates()

    # Choose the column in result_df1 to match against
    right_key_col = key_col  # 'hostname' or 'pid'
    filtered = result_df1.merge(
        keys.rename(columns={key_col: right_key_col}),
        on=["fhash", right_key_col],
        how="inner",
    )

    # --- Compute io_time = sum_over_trange( max_{pid,tid} sum(dur) ) ---
    # Grouping dimensions for the "inner" sum:
    # If key_col == 'pid', we shouldn't include 'pid' twice; also within a single (fhash,pid, trange),
    # we want max over tids (pid is constant). If key_col == 'hostname', we want max over (pid,tid).
    group_inner = ["fhash", key_col, "trange"]
    if key_col != "pid":
        group_inner += ["pid"]
    group_inner += ["tid"]

    # A) sum dur per (..., tid)
    agg1 = (
        filtered
        .groupby(group_inner)[["dur"]]
        .sum()
        .reset_index()
    )

    # B) max dur per (fhash, key_col, trange)
    agg2 = (
        agg1
        .groupby(["fhash", key_col, "trange"])[["dur"]]
        .max()
        .rename(columns={"dur": "dur_max"})
        .reset_index()
    )

    # C) sum of dur_max across tranges → io_time
    io_time = (
        agg2
        .groupby(["fhash", key_col])[["dur_max"]]
        .sum()
        .rename(columns={"dur_max": "io_time"})
        .reset_index()
    )

    # Counts and sizes per (fhash, key_col)
    io_count = (
        filtered
        .groupby(["fhash", key_col])
        .size()
        .to_frame("io_count")
        .reset_index()
    )

    io_size = (
        filtered
        .groupby(["fhash", key_col])[["size"]]
        .sum()
        .rename(columns={"size": "io_size"})
        .reset_index()
    )

    # Combine metrics
    metrics = (
        io_time
        .merge(io_count, on=["fhash", key_col], how="outer")
        .merge(io_size, on=["fhash", key_col], how="outer")
    )

    # Derived metrics (guard against divide-by-zero)
    metrics = metrics.assign(
        iops=(metrics["io_count"] / metrics["io_time"]).where(metrics["io_time"] > 0, 0.0),
        bw=(metrics["io_size"] / metrics["io_time"]).where(metrics["io_time"] > 0, 0.0),
    )

    # Merge onto df_keys
    out = df_keys.merge(metrics, on=["fhash", key_col], how="left")

    if drop_unmatched:
        out = out.dropna(subset=["io_time", "io_count", "io_size"])

    return out.compute() if compute_result else out


# Convenience wrappers (optional)
def compute_io_metrics_host(*args, **kwargs):
    return compute_io_metrics_by_key(*args, key_col="hostname", **kwargs)

def compute_io_metrics_pid(*args, **kwargs):
    return compute_io_metrics_by_key(*args, key_col="pid", **kwargs)




def unify_fhash_dask(fhash_dd: dd.DataFrame, analyze_dd: dd.DataFrame):
    # Ensure string dtype
    fhash_dd = fhash_dd.assign(
        name=fhash_dd["name"].astype("string"),
        hash=fhash_dd["hash"].astype("string"),
    )
    analyze_dd = analyze_dd.assign(fhash=analyze_dd["fhash"].astype("string"))

    # Extract basename and check if it's a bare filename
    def _basename_part(df: pd.DataFrame) -> pd.Series:
        return df["name"].map(lambda p: PurePath(str(p)).name)
    fhash_dd = fhash_dd.assign(
        basename=fhash_dd.map_partitions(_basename_part, meta=("basename", "string")),
        is_bare=fhash_dd["name"].map(lambda n: "/" not in str(n), meta=("is_bare", "bool")),
    )

    # Identify basenames that have at least one bare version
    eligible = (
        fhash_dd[fhash_dd["is_bare"]]
        ["basename"]
        .drop_duplicates()
        .compute()
        .tolist()
    )
    eligible_set = set(eligible)
    if not eligible_set:
        return fhash_dd, analyze_dd, {}

    # Pick one existing hash per eligible basename (first occurrence)
    canonical = (
        fhash_dd[fhash_dd["basename"].isin(eligible_set)]
        .groupby("basename")["hash"]
        .first()
        .compute()
        .to_dict()
    )

    # Build mapping: old hash → canonical hash (for eligible only)
    hb = fhash_dd[["hash", "basename"]].drop_duplicates().compute()
    hb = hb[hb["basename"].isin(eligible_set)]
    old2base = dict(zip(hb["hash"], hb["basename"]))
    old2new = {h: canonical.get(old2base.get(h), h) for h in old2base}

    # Apply mapping
    fhash_dd = fhash_dd.assign(hash=fhash_dd["hash"].map(old2new).fillna(fhash_dd["hash"]))
    analyze_dd = analyze_dd.assign(fhash=analyze_dd["fhash"].map(old2new).fillna(analyze_dd["fhash"]))

    return fhash_dd, analyze_dd, old2new



def compute_node_metrics(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute unweighted & weighted betweenness centrality,
    in-degree, and out-degree for all nodes in a directed weighted graph.

    Parameters
    ----------
    G : nx.DiGraph
        A directed NetworkX graph with optional 'weight' attributes on edges.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        ['node', 'betweenness_unweighted', 'betweenness_weighted', 'in_degree', 'out_degree']
    """
    # Betweenness centrality
    bc_unweighted = nx.betweenness_centrality(G, weight=None, normalized=True)
    print("unweightedComputation Done!")
    bc_weighted = nx.betweenness_centrality(G, weight='inv_size', normalized=True) #computing based on size of datatransfer (high transfer given more importance)
    print("weightedComputation Done!")
    # In-degree and out-degree (unweighted)
    indeg = dict(G.in_degree(weight=None))
    outdeg = dict(G.out_degree(weight=None))

    # Combine into a dataframe
    df = pd.DataFrame({
        'node': list(G.nodes()),
        'betweenness_unweighted': [bc_unweighted.get(n, 0) for n in G.nodes()],
        'betweenness_weighted': [bc_weighted.get(n, 0) for n in G.nodes()],
        'in_degree': [indeg.get(n, 0) for n in G.nodes()],
        'out_degree': [outdeg.get(n, 0) for n in G.nodes()],
    })

    return df


#Took Verly Long so not currently used
def merge_and_sum_durations(df):
    """Merge overlapping intervals and return total duration."""
    intervals = df[['ts', 'te']].sort_values('ts').to_numpy()
    total = 0
    current_start, current_end = intervals[0]

    for start, end in intervals[1:]:
        if start <= current_end:  # overlap → merge
            current_end = max(current_end, end)
        else:                     # disjoint → add and start new
            total += current_end - current_start
            current_start, current_end = start, end

    total += current_end - current_start  # last interval
    return total