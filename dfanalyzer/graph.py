
import dask.dataframe as dd
import re
import os
import glob
import pandas as pd
import networkx as nx
import logging
import pyarrow as pa



class GraphIOXBursts:
    """
    This class provides methods to compute the I/O burstiness in workloads.
    """
    def __init__(self, ddf=None, app_name="", operation="", delta=0, cp_dir="",existing=False):
        self.ddf = ddf
        self.operation = operation
        self.app_name = app_name
        self.cp_dir = cp_dir
        self.node_local_mounts = ["/dev/shm", "/var/tmp","/l/ssd","/tmp","/dev","/var","/sys","/proc"]
        self.ddf_bur = None
        self.delta = delta
        self.meta = {
            'id': str,
            'name': str,
            'cat': str,
            'size': int,
            'ts': int,
            'te': int,
            'dur': int,
            'trange': int,
            'mount_point': str,
            'hostname': str,
            'b_id':int,
        }
        if not existing:
            self.ddf = self.ddf[['id','name','cat','size','ts','te','dur','trange','mount_point','hostname']]
        


    def compute_burstiness(self):
        def get_burstiness(df_group, delta):
            df_group = df_group.sort_values(by='ts').reset_index(drop=True)
            group_length = len(df_group)
            b_id = [0]*group_length

            row = 0 # start with first row
            while row < group_length:
                current_id = df_group.at[row,'id']
                b_id[row] = current_id
                max_te = df_group.at[row,"te"] 
                next_row = row + 1
                while next_row < group_length and df_group.at[next_row, "ts"] < (max_te + self.delta):
                    b_id[next_row] = current_id
                    max_te = max(max_te, df_group.at[next_row,"te"])
                    next_row += 1
                row = next_row
            df_group["b_id"] = b_id

            # df_group =df_group.drop(columns=["group_key"], errors="ignore")


            return df_group
        
        self.meta = self.ddf._meta.assign(
            group_key=pa.array([], type=pa.string()),  # Create an empty PyArrow array for group_key with string type
            b_id=pa.array([], type=pa.int64())  # Create an empty PyArrow array for deg with int64 type
        )

        #compute the grouping for 
        self.ddf["group_key"] = self.ddf.apply(
        lambda row: (row["trange"], row["mount_point"], row["hostname"]) 
                    # if row["mount_point"] in self.node_local_mounts
                    if pd.notna(row["mount_point"]) and row["mount_point"] in self.node_local_mounts 
                    else (row["trange"], row["mount_point"]), axis=1
        )
        self.ddf_bur = self.ddf.groupby("group_key").apply(get_burstiness,self.delta, meta=self.meta).reset_index(drop=True)
        # self.ddf_bur = self.ddf_bur.drop(columns=["group_key"])
        # print(self.ddf_bur.dtypes)
        # self.ddf_bur = self.ddf.groupby("group_key").count()
    
    def write_checkpoint(self, id, cp_dir):
        '''
        write the datafame as parquet files
        '''
        write_df = getattr(self, id)

        idenifier = id+"-"+self.operation
        write_df.to_parquet(f"{cp_dir}/{self.app_name}",
                            name_function=lambda i: f'{idenifier}-{i}.parquet')


    def read_checkpoint(self, id, cp_dir):
        '''
        read the dataframe from checkpoint files
        '''
        identifier = id+"-"+self.operation
        read_df = dd.read_parquet(f"{cp_dir}/{self.app_name}/{identifier}*.parquet")
        setattr(self, id, read_df)


class GraphIOXInterference:
    """
    This class provides methods to manage the graph based representation of IO traces for interference computation.
    init parameters:
    ddf (dask dataframe): dask dataframe (analyzer.events) for computing the metrices.
    app_name (str): Application identifier. Used for reading/writing the checkpoint files.
    cp_dir (str): Checkpoint directory path
    existing (bool): If true, computation can be avoided and the instance can be loaded from checkpoint files for downstream analysis
    """

    '''
    analyzer-> is DFAnalyzer type
    analyzer.events -> is list of all the events dask dataframe type

    
    '''

    def __init__(self, ddf=None, app_name="", operation="", cp_dir="", existing=False):
        self.ddf = ddf 
        self.operation=operation
        self.deg = None
        self.inter = None
        self.node_local_mounts = ["/dev/shm", "/var/tmp","/l/ssd","/tmp","/dev","/var","/sys","/proc"]
        self.app_name = app_name
        self.cp_dir = cp_dir
        self.meta = {
            'id': str,
            'name': str,
            'cat': str,
            'size': int,
            'size_category': str,
            'ts': int,
            'te': int,
            'dur': int,
            'trange': int,
            'mount_point': str,
            'hostname': str,
            'deg': int
        }
        if existing:
            self.read_checkpoint(id="inter", cp_dir = cp_dir)
            # self.read_checkpoint(id='inter_metadata', cp_dir=cp_dir)
        else:
            self.ddf = self.select_data_cols(cols=['id','name','cat','size','size_category','ts','te','dur','trange','mount_point','hostname'])

    def select_data_cols(self, cols=[]):
        '''
        returns the data with size > 0 and with selected columns 
        '''
        return self.ddf[cols]
    

    def computeDegree(self):
        def calculateDegree(df_group):
            df_group = df_group.sort_values(by='ts').reset_index(
                drop=True)  # check drop true
            # logging.info(f"computing for {df_group.name[0]} and {df_group.name[1]}")
            group_length = len(df_group)
            degrees = [1]*group_length
            # update degrees by looping throught the group
            for row in range(group_length):
                current_stop_time = df_group.at[row, 'te']
                # look for neighbors
                for neigh in range(row+1, group_length):
                    neigh_start_time = df_group.at[neigh, 'ts']
                    if (neigh_start_time < current_stop_time):
                        degrees[row] += 1
                        degrees[neigh] += 1
                    else:
                        break
            # Add a new column 'deg' representing the count of overlaps
            df_group['deg'] = degrees
            return df_group
        
        def getDegreePartition(df_partition):
            return df_partition.groupby('group_key').apply(calculateDegree).reset_index(
            drop=True)
        
        self.meta = self.ddf._meta.assign(
            group_key=pa.array([], type=pa.string()),  # Create an empty PyArrow array for group_key with string type
            deg=pa.array([], type=pa.int64())  # Create an empty PyArrow array for deg with int64 type
        )

        self.ddf["group_key"] = self.ddf.apply(
        lambda row: (row["trange"], row["mount_point"], row["hostname"]) 
                    # if row["mount_point"] in self.node_local_mounts
                    if pd.notna(row["mount_point"]) and row["mount_point"] in self.node_local_mounts 
                    else (row["trange"], row["mount_point"]), axis=1
        )
        self.ddf = self.ddf.set_index("trange", sorted=False, drop=False) # ensure same group key are in same partition
        self.deg = self.ddf.map_partitions(getDegreePartition)
        # self.deg = self.deg.drop(columns='group_key')

    def computeInterferenceData(self):
        '''
        calculate the interference factor for each events.
        step1: calculate the duration of minimum degree for all size and mount point combination.
        step2: calculate the Interference factor based on duration of the event and the duration of min degree event. 
        '''

        def dur_of_min_deg(ddf):
            '''
            calculate the duration of minimum degree for all size and mount point combination.
            '''
            ddf1 = ddf.copy()
            list_deg = ddf1.groupby('group_key_inter')["deg"].min().compute()
            agg_dict = {str(deg): min for deg in list_deg.unique()}
            
            # Create a function to process each partition independently
            def process_partition(partition):
                result = partition.copy()
                # For each unique minimum degree value
                for deg in list_deg.unique():
                    col_name = str(deg)
                    result[col_name] = 9223372036854775807
                    result[col_name] = result[col_name].mask(
                        result['deg'] == deg, result['dur']
                    )
                return result
            ddf1 = ddf.map_partitions(process_partition)
            
            return ddf1, agg_dict, list_deg

        logging.info(f"Computing duration of minimum degree event for all degrees.")
        self.meta = self.deg._meta.assign(
            group_key_inter=pa.array([], type=pa.string())  # Create an empty PyArrow array for group_key with string type
        )
        
        self.deg["group_key_inter"] = self.deg.apply(
        lambda row: (row["size_category"], row["mount_point"], row["hostname"]) 
                    # if row["mount_point"] in self.node_local_mounts
                    if pd.notna(row["mount_point"]) and row["mount_point"] in self.node_local_mounts 
                    else (row["size_category"], row["mount_point"]), axis=1
        )

        dft1, agg_dict, list_deg = dur_of_min_deg(self.deg)

        
        def calulateInterferencePartition(df_partition, agg_dict, list_deg):
            agg_dict["deg"] = min
            val = df_partition.groupby('group_key_inter').agg(agg_dict)
            val['min_dur'] = 0
            for deg in list_deg:
                val['min_dur'] = val['min_dur'].mask(
                    val['deg'].eq(deg), val[str(deg)])
            ddf2 = val.reset_index()
            merge = df_partition.merge(ddf2, on=['group_key_inter'], how='left', suffixes=('_caller', '_other'))[['name','cat','size','size_category','ts','te','dur','trange','mount_point','hostname', 'deg_caller', 'deg_other', 'min_dur']]
            merge['interference'] = merge['min_dur']/merge['dur']
            return merge

        self.inter = dft1.map_partitions(calulateInterferencePartition, agg_dict, list_deg, align_dataframes=False)

       



    def get_interference_metadata(self):
        '''
        calculate the interference factor for each events.
        step1: calculate the duration of minimum degree for all size and mount point combination.
        step2: calculate the Interference factor based on duration of the event and the duration of min degree event. 
        '''
        def dur_of_min_deg_metadata(ddf):
            '''
            calculate the duration of minimum degree for all size and mount point combination.
            '''
            ddf1 = ddf.copy()
            list_deg = ddf1.groupby(['group_key'])[
                "deg"].min()
            agg_dict = {}
            for deg in list_deg:
                agg_dict[str(deg)] = min
                ddf1[str(deg)] = 9223372036854775807
                ddf1[str(deg)] = ddf1[str(deg)].mask(
                    ddf1['deg'] == deg, ddf1['dur'])
            return ddf1, agg_dict, list_deg

        def calculate_interference_metadata(ddf, agg_dict, list_deg):
            '''
            calculate the Interference factor based on duration of the event and the duration of min degree event. 
            '''
            agg_dict["deg"] = min
            val = ddf.groupby(['group_key']).agg(agg_dict)
            val['min_dur'] = 0
            for deg in list_deg:
                val['min_dur'] = val['min_dur'].mask(
                    val['deg'].eq(deg), val[str(deg)])
            ddf2 = val.reset_index()
            merge = ddf.merge(ddf2, on=['group_key'], how='left', suffixes=('_caller', '_other'))[['name','cat','size','size_category','ts','te','dur','trange','mount_point','hostname', 'deg_caller', 'deg_other', 'min_dur']]
            merge['interference'] = merge['min_dur']/merge['dur']
            return merge

        dft2, agg_dict2, list_deg2 = dur_of_min_deg_metadata(self.deg)

        self.inter = calculate_interference_metadata(
            dft2, agg_dict=agg_dict2, list_deg=list_deg2)

    

    def compute_interference(self):
        if self.operation == "data":
            self.computeInterferenceData()

        elif self.operation  == "metadata":
            # logging.info(f"Computing interference for metadata events.")
            self.get_interference_metadata()
        else:
            logging.info("Invalid Operation Type")

    
    def write_checkpoint(self, id, cp_dir):
        '''
        write the datafame as parquet files
        '''
        write_df = getattr(self, id)

        idenifier = id+"-"+self.operation
        # print(write_df.compute())
        # schema = {'id': int, 'name': str, 'pid': int, 'size': int, 'ts': int, 'te': int, 'mount_point': str, 'dur': int, 'trange': int, 'deg':int}
        write_df.to_parquet(f"{cp_dir}/{self.app_name}",
                            name_function=lambda i: f'{idenifier}-{i}.parquet')

    def read_checkpoint(self, id, cp_dir):
        '''
        read the dataframe from checkpoint files
        '''
        identifier = id+"-"+self.operation
        read_df = dd.read_parquet(f"{cp_dir}/{self.app_name}/{identifier}*.parquet")
        setattr(self, id, read_df)


    """
    Workflow utilities for selecting events and deriving workflow graphs from a Dask DataFrame.

    Expected columns in `ddf`:
      - fhash: file/content hash (any type; will be cast to str when composing keys)
      - hostname: machine/host name
      - pid: process identifier
      - prod: numeric amount (or count) of production
      - cons: numeric amount (or count) of consumption
      - ts: timestamp (numeric/datetime) for first/earliest access

    Side effects / cached members:
      - selected_events: filtered Dask DataFrame after `select_events()`
      - filelist: numpy array of fhash values used for filtering
      - prod_cons: per-file aggregated prod/cons table
      - both_prod_cons_file_host: numpy array of composite keys (fhash_host) used for L3
    """

    def __init__(self, ddf: dd.DataFrame):
        self.ddf: dd.DataFrame = ddf
        self.selected_events: Optional[dd.DataFrame] = None
        self.filelist = None
        self.prod_cons: Optional[dd.DataFrame] = None
        self.both_prod_cons_file_host = None

    # -------------------------
    # Stage 0: Base filtering
    # -------------------------
    def select_events(self) -> None:
        """
        Keep only rows where files (by fhash) are BOTH produced and consumed at least once.
        This avoids polluting later stages with producer-only or consumer-only files.
        """
        # Aggregate prod/cons per file
        self.prod_cons = (self.ddf
                          .groupby("fhash")[["prod", "cons"]]
                          .sum())

        # Keep files with both sides > 0
        prod_cons_ok = (self.prod_cons
                        .query("prod > 0 and cons > 0")
                        .reset_index())

        # Unique file list to filter original
        self.filelist = prod_cons_ok["fhash"].unique().compute()

        # Filter original events to only those files
        self.selected_events = self.ddf[self.ddf["fhash"].isin(self.filelist)]

    # -------------------------
    # Stage 1/2/3 orchestration
    # -------------------------
    def get_wfGraph(self, level: int = 1) -> dd.DataFrame:
        """
        Build workflow view at different levels.

          level=1: Within a hostname, keep (pid, fhash) pairs where the process BOTH produced & consumed the file.
          level=2: Cross-host filter where (fhash, hostname) had both prod & cons, then remove rows
                   where same (pid, fhash) also had both prod & cons (i.e., drop L1-like cases).
          level=3: Complement of level 2’s “both” per (fhash, hostname): rows where a host did NOT both prod & cons.

        Returns a Dask DataFrame.
        """
        if self.selected_events is None:
            raise RuntimeError("Call select_events() before get_wfGraph().")

        if level == 1:
            # Process each hostname independently
            data = (self.selected_events.groupby("hostname")
                    .apply(self._compute_workflow_for_group_l1, include_groups=False)
                    .reset_index())

            # Standardize column order
            data = data[["hostname", "pid", "fhash", "ts"]]

        elif level == 2:
            data = self._compute_workflow_group_l2()

        elif level == 3:
            data = self._compute_workflow_group_l3()

        else:
            raise ValueError("level must be 1, 2, or 3.")

        return data

    # -------------------------
    # Level 1
    # -------------------------
    def _compute_workflow_for_group_l1(self, group_df: dd.DataFrame) -> dd.DataFrame:
        """
        Per-hostname reduction:
        - Group by (fhash, pid)
        - Keep rows where that (pid) both produced and consumed the same file on this host
        - Return minimal columns for the workflow view
        """
        summary = (group_df.groupby(["fhash", "pid"])
                   .agg(prod=("prod", "sum"),
                        cons=("cons", "sum"),
                        ts=("ts", "min"))
                   .reset_index())

        final = summary.query("prod > 0 and cons > 0")
        return final[["pid", "fhash", "ts"]]

    # -------------------------
    # Level 2
    # -------------------------
    def _compute_workflow_group_l2(self) -> dd.DataFrame:
        """
        L2 logic:
          Part 1: Build composite key fhash_host; keep ONLY (fhash, hostname) pairs that both produced & consumed.
          Part 2: From those rows, remove cases where the SAME (pid, fhash) also had both prod & cons (i.e., L1-like).
        """
        # Composite key: one string col is easier to carry around
        se = self.selected_events.assign(
            fhash_host=self.selected_events["fhash"].astype(str) + "_" + self.selected_events["hostname"].astype(str)
        )

        # Part 1: retain only (fhash_host) that had both prod & cons (fully resolves to a small numpy once)
        both_prod_and_cons = (se.groupby("fhash_host")[["prod", "cons"]]
                              .sum()
                              .query("prod > 0 and cons > 0")
                              .compute()
                              .index.to_numpy())

        filtered = se[se["fhash_host"].isin(both_prod_and_cons)]

        # Cache for L3 (the complement)
        self.both_prod_cons_file_host = both_prod_and_cons

        # Part 2: drop (pid, fhash) pairs that also show both prod & cons (i.e., keep where NOT both)
        f1 = (filtered.groupby(["pid", "fhash"])[["prod", "cons"]]
              .sum()
              .reset_index()
              .query("prod <= 0 or cons <= 0"))

        # Return a tidy table (pid, fhash, prod, cons)
        return f1

    # -------------------------
    # Level 3
    # -------------------------
    def _compute_workflow_group_l3(self) -> dd.DataFrame:
        """
        L3 logic:
          Rows where (fhash, hostname) did NOT have both production and consumption.
          (Complement of the set used in L2 part 1.)
        """
        if self.both_prod_cons_file_host is None:
            # If L2 hasn't run yet, compute the same key set here
            se_tmp = self.selected_events.assign(
                fhash_host=self.selected_events["fhash"].astype(str) + "_" + self.selected_events["hostname"].astype(str)
            )
            self.both_prod_cons_file_host = (se_tmp.groupby("fhash_host")[["prod", "cons"]]
                                             .sum()
                                             .query("prod > 0 and cons > 0")
                                             .compute()
                                             .index.to_numpy())

        se = self.selected_events.assign(
            fhash_host=self.selected_events["fhash"].astype(str) + "_" + self.selected_events["hostname"].astype(str)
        )
        # Keep those NOT in the L2 “both sides” set
        filtered = se[~se["fhash_host"].isin(self.both_prod_cons_file_host)]
        return filtered[["fhash", "hostname", "prod", "cons"]]

    # -------------------------
    # Build & save graph (L3)
    # -------------------------
    def build_and_save_graph_l3(self, ddf: dd.DataFrame, output_path: str) -> None:
        """
        Build a DiGraph from (hostname_prod) -> fhash -> (hostname_cons) and save unique edges to CSV.

        Parameters
        ----------
        ddf : dd.DataFrame
            Must contain columns: ['hostname_prod', 'fhash', 'hostname_cons'].
        output_path : str
            Directory (or prefix) where 'l3.csv' will be written.
        """
        # Bring the minimal columns to pandas; ensures we don’t transfer extra data
        df = ddf[["hostname_prod", "fhash", "hostname_cons"]].compute()

        output_file = output_path.rstrip("/") + "/l3.csv"
        G = nx.DiGraph()

        # Add edges: host_prod -> fhash, then fhash -> host_cons
        for _, row in df.iterrows():
            G.add_edge(row["hostname_prod"], row["fhash"])
            G.add_edge(row["fhash"], row["hostname_cons"])

        # NetworkX DiGraph de-duplicates edges inherently
        with open(output_file, "w") as f:
            for src, dest in G.edges():
                f.write(f"{src} {dest}\n")

        print(f"Graph saved: {len(G.nodes)} nodes, {len(G.edges)} edges → {output_file}")