
import dask.dataframe as dd
import re
import os
import glob
import pandas as pd
import networkx as nx
import logging
import pyarrow as pa


class DFGrepInterference_original:
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
    


    def get_degree(self):
        """
        Calculates the degree for each event. The degree calculation is done on group of events with same mount point and within same timerange.

        get_deg calculates the degree for each group. It first sort the events according to start time, and for each events, look forward to determine if any events have overlapping time. If so, increase the degree by 1 for both events.
        """

        def get_deg(df_group):
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

        self.deg = self.ddf.groupby(['mount_point', 'trange']).apply(get_deg, meta=self.meta).reset_index(
            drop=True) 


    #logging 
    def get_interference_data(self):
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
            list_deg = ddf1.groupby(["size_category","mount_point"])[
                "deg"].min().compute()
            agg_dict = {}
            # print("starting loop")
            for deg in list_deg:
                agg_dict[str(deg)] = min
                ddf1[str(deg)] = 9223372036854775807
                ddf1[str(deg)] = ddf1[str(deg)].mask(
                    ddf1['deg'] == deg, ddf1['dur'])
            return ddf1, agg_dict, list_deg

        def calculate_interference(ddf, agg_dict, list_deg):
            '''
            calculate the Interference factor based on duration of the event and the duration of min degree event. 
            '''
            agg_dict["deg"] = min
            val = ddf.groupby(['size_category', 'mount_point']).agg(agg_dict)
            val['min_dur'] = 0
            for deg in list_deg:
                val['min_dur'] = val['min_dur'].mask(
                    val['deg'].eq(deg), val[str(deg)])
            ddf2 = val.reset_index()
            merge = ddf.merge(ddf2, on=['size_category', 'mount_point'], how='left', suffixes=('_caller', '_other'))[['name','cat','size','size_category','ts','te','dur','trange','mount_point','hostname', 'deg_caller', 'deg_other', 'min_dur']]
            merge['interference'] = merge['min_dur']/merge['dur']
            return merge

        logging.info(f"Computing duration of minimum degree event for all degrees.")
        dft1, agg_dict, list_deg = dur_of_min_deg(self.deg)
        logging.info(f"Computing duration of minimum degree event for all degrees.")
        self.inter = calculate_interference(
            dft1, agg_dict=agg_dict, list_deg=list_deg)



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
            list_deg = ddf1.groupby(["mount_point", "name"])[
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
            val = ddf.groupby(['name', 'mount_point']).agg(agg_dict)
            val['min_dur'] = 0
            for deg in list_deg:
                val['min_dur'] = val['min_dur'].mask(
                    val['deg'].eq(deg), val[str(deg)])
            ddf2 = val.reset_index()
            merge = ddf.merge(ddf2, on=['name', 'mount_point'], how='left', suffixes=('_caller', '_other'))[['name','cat','size','size_category','ts','te','dur','trange','mount_point','hostname', 'deg_caller', 'deg_other', 'min_dur']]
            merge['interference'] = merge['min_dur']/merge['dur']
            return merge
        logging.info(f"Computing duration of minimum degree event for all degrees.")
        dft2, agg_dict2, list_deg2 = dur_of_min_deg_metadata(self.deg)
        logging.info(f"Computing interference for all events.")
        self.inter = calculate_interference_metadata(
            dft2, agg_dict=agg_dict2, list_deg=list_deg2)

    

    def compute_interference(self):
        if self.operation == "data":
            logging.info(f"Computing interference for data events.")
            self.get_interference_data()
        elif self.operation  == "metadata":
            logging.info(f"Computing interference for metadata events.")
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




class DFGrepBurstiness:
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


class DFGrepWorkflowX:
    """
    This class provides methods to represent the IO traces as workflow graphs.
    init parameters: 
    """

    def __init__(self, ddf=None, app_name="", trace_path=""):
        self.ddf = ddf
        self.app_name = app_name
        self.trace_path = trace_path

    def select_cols(self, cols=[]):
        return self.ddf[cols]

    def compute_workflow(self,lvl):
        
        def create_workflow(df):
            '''
            1. Find number of times each file is prod/cons. And select the files that are both prod & cons atleast once
            2. Get the list of the files which are both produced and consumed and select only the events with these files
            '''
            prod_cons = df.groupby('fhash')['prod', 'cons'].sum()
            prod_cons = prod_cons.query('prod > 0 and cons > 0').reset_index()
            filelist = prod_cons.fhash.unique().compute()
            selected_events = df[df.fhash.isin(filelist)]
            selected_events_sum = selected_events.groupby(['fhash', 'src_id']).agg(
                {'prod': 'sum', 'cons': 'sum', 'ts': 'min'}).reset_index()
            merged = selected_events_sum.merge(
                prod_cons, on=["fhash"], how='left', suffixes=['_src_id', '_fid'])
            final = merged.query(
                'not (prod_src_id == prod_fid and cons_src_id == cons_fid)')
            return final
        
        if lvl == 1: # all threads in same process accessing common memory space
            self.ddf['group_events'] = self.ddf['hostname'].astype(str) + '_' + self.ddf['pid'].astype(str)
            self.ddf['src_id'] = self.ddf['tid'] # no need
            result_df = self.ddf.groupby('group_events').apply(create_workflow).reset_index()
            # group_id = hostname+pid
            '''
            calculating groupid for every prducer and consumer
            make sure groupId == consumerID
            '''



        elif lvl == 2: # node local accelerators
            self.ddf['group_events'] = self.ddf['hostname'].astype(str) 
            self.ddf['src_id'] = self.ddf['pid'] # no need same pid should not be producing and consuming
            result_df = self.ddf.groupby('group_events').apply(create_workflow).reset_index()

        else: # accessing shared storage resources
            #exclude first two cases
            self.ddf['src_id'] = self.ddf['hostname']
            result_df = create_workflow(self.ddf).reset_index()
        
        return result_df
   
    def get_pid_map(self):
        '''
        This function is designed for mummi traces to map pid with the application based on the filename
        '''
        all_files = glob.glob(self.trace_path)
        pid_map = {}
        for file in all_files:
            slices = os.path.basename(file).split('.')
            if (len(slices) > 4):
                pid_map[slices[3]] = slices[1]
        return pid_map

    def create_workflow(self):
        '''
        1. Find number of times each file is prod/cons. And select the files that are both prod & cons atleast once
        2. Get the list of the files which are both produced and consumed and select only the events with these files
        '''
        prod_cons = self.ddf.groupby('fhash')['prod', 'cons'].sum()
        prod_cons = prod_cons.query('prod > 0 and cons > 0').reset_index()
        filelist = prod_cons.fhash.unique().compute()
        selected_events = self.ddf[self.ddf.fhash.isin(filelist)]
        # selected_events_sum = selected_events.groupby(['fhash', 'pid']).agg(
        #     {'prod': 'sum', 'cons': 'sum', 'ts': 'min'}).reset_index()
        # merged = selected_events_sum.merge(
        #     prod_cons, on=["fhash"], how='left', suffixes=['_pid', '_fid'])
        # final = merged.query(
        #     'not (prod_pid == prod_fid and cons_pid == cons_fid)')
        #pid_x may be either thread lvl, process lvl or node lvl 
        selected_events_sum = selected_events.groupby(['fhash', 'pid_x']).agg(
            {'prod': 'sum', 'cons': 'sum', 'ts': 'min'}).reset_index()
        merged = selected_events_sum.merge(
            prod_cons, on=["fhash"], how='left', suffixes=['_pid_x', '_fid'])
        final = merged.query(
            'not (prod_pid_x == prod_fid and cons_pid_x == cons_fid)')
        return final


    def create_graph_df(self, df, pid_map):
        '''
        Function creates soruce and destination data for plotting the graph. This version is currently designed for mummi workflow
        '''
        def get_base_filename(path):
            return os.path.basename(path)

        def process_row(row):
            # filename = re.sub("\d+", "x", row['filename'])
            # filename = "f_"+get_base_filename(filename)
            filename = row['fhash']
            # pid = pid_map[str(row['pid'])] if str(
            #     row['pid']) in pid_map else str(row['pid'])
            pid = row['pid_x']
            prod = row['prod_pid_x']
            cons = row['cons_pid_x']

            if prod == 0:
                return [{'src': filename, 'dest': pid, 'wt': row['ts']}]

            elif cons == 0:
                return [{'src': pid, 'dest': filename, 'wt': row['ts']}]
            elif prod > 0 and cons > 0:
                return [{'src': filename, 'dest': pid, 'wt': row['ts']}, {'src': pid, 'dest': filename, 'wt': row['ts']}]

        graph_df = pd.DataFrame([item for sublist in df.apply(
            process_row, axis=1) for item in sublist])
        return graph_df
    
    def create_graph_finer_granularity(self, df):
        '''
        Function creates soruce and destination data for plotting the graph.
        Function for returning finer granular graph, files contains same 
        '''
        # def get_base_filename(path):
        #     return os.path.basename(path)

        def process_row(row):
            # filename = re.sub("\d+", "x", row['filename'])
            # filename = "f_"+get_base_filename(filename)
            filename = row['filename']
            # pid = pid_map[str(row['pid'])] if str(
            #     row['pid']) in pid_map else str(row['pid'])
            pid = "p_"+str(row['pid'])
            prod = row['prod_pid']
            cons = row['cons_pid']

            if prod == 0:
                return [{'src': filename, 'dest': pid, 'wt': row['ts']}]

            elif cons == 0:
                return [{'src': pid, 'dest': filename, 'wt': row['ts']}]
            elif prod > 0 and cons > 0:
                return [{'src': filename, 'dest': pid, 'wt': row['ts']}, {'src': pid, 'dest': filename, 'wt': row['ts']}]

        graph_df = pd.DataFrame([item for sublist in df.apply(
            process_row, axis=1) for item in sublist])
        return graph_df

    def compute_betweeness_centrality(self, graph_df, weighted = False):
        """
        Function computes the betweeness centrality of nodes in the graph,
        Input: Pandas dataframe as src, dest, and wt
        Output: Dict of key=node and val=centrality value
        """
        G = nx.from_pandas_edgelist(graph_df, source='src', target='dest', edge_attr=['wt'], create_using=nx.DiGraph())
        if not weighted:
            return nx.betweenness_centrality(G)
        else:
            return nx.betweenness_centrality(G, weight='wt')


   
class DFGrepInterference_groupKey:
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
    


    def get_degree(self):
        """
        Calculates the degree for each event. The degree calculation is done on group of events with same mount point and within same timerange.

        get_deg calculates the degree for each group. It first sort the events according to start time, and for each events, look forward to determine if any events have overlapping time. If so, increase the degree by 1 for both events.
        """

        def get_deg(df_group):
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

        #compute the grouping for 
        # Define PyArrow schema for the metadata
        self.meta = self.ddf._meta.assign(
            group_key_deg=pa.array([], type=pa.string()),  # Create an empty PyArrow array for group_key with string type
            group_key_inter = pa.array([], type=pa.string()),
            deg=pa.array([], type=pa.int64())  # Create an empty PyArrow array for deg with int64 type
        )

        #self.meta = self.ddf._meta.assign(group_key=pd.Series(dtype='object'), deg = pd.Series(dtype='int64'))
        # self.meta = self.ddf._meta.assign(deg=pd.Series(dtype='int64'))

        self.ddf["group_key"] = self.ddf.apply(
        lambda row: (row["trange"], row["mount_point"], row["hostname"]) 
                    # if row["mount_point"] in self.node_local_mounts
                    if pd.notna(row["mount_point"]) and row["mount_point"] in self.node_local_mounts 
                    else (row["trange"], row["mount_point"]), axis=1
        )

        self.deg = self.ddf.groupby('group_key').apply(get_deg, meta=self.meta).reset_index(
            drop=True) 


    #logging 
    def get_interference_data(self):
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
            list_deg = ddf1.groupby(["group_key"])[
                "deg"].min()
            agg_dict = {}
            # print("starting loop")
            for deg in list_deg:
                agg_dict[str(deg)] = min
                ddf1[str(deg)] = 9223372036854775807
                ddf1[str(deg)] = ddf1[str(deg)].mask(
                    ddf1['deg'] == deg, ddf1['dur'])
            return ddf1, agg_dict, list_deg

        def calculate_interference(ddf, agg_dict, list_deg):
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

        # logging.info(f"Computing duration of minimum degree event for all degrees.")
        dft1, agg_dict, list_deg = dur_of_min_deg(self.deg)
        # logging.info(f"Computing duration of minimum degree event for all degrees.")
        self.inter = calculate_interference(
            dft1, agg_dict=agg_dict, list_deg=list_deg)



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
        # logging.info(f"Computing duration of minimum degree event for all degrees.")
        dft2, agg_dict2, list_deg2 = dur_of_min_deg_metadata(self.deg)
        # logging.info(f"Computing interference for all events.")
        self.inter = calculate_interference_metadata(
            dft2, agg_dict=agg_dict2, list_deg=list_deg2)

    

    def compute_interference(self):
        if self.operation == "data":
            # logging.info(f"Computing interference for data events.")
            self.get_interference_data()
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




class DFGrepInterferencePartitionBased:
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
        # self.deg = self.deg.set_index('group_key_inter')
        dft1, agg_dict, list_deg = dur_of_min_deg(self.deg)
        logging.info(f"Computing IF for all ")
        # dft1 = dft1.set_index('mount_point')
        
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
        # self.deg = self.deg.set_index("group_key", sorted=False)
        self.inter = dft1.map_partitions(calulateInterferencePartition, agg_dict, list_deg, align_dataframes=False)

        # self.inter = calculate_interference(
        #     dft1, agg_dict=agg_dict, list_deg=list_deg)



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
        # logging.info(f"Computing duration of minimum degree event for all degrees.")
        dft2, agg_dict2, list_deg2 = dur_of_min_deg_metadata(self.deg)
        # logging.info(f"Computing interference for all events.")
        self.inter = calculate_interference_metadata(
            dft2, agg_dict=agg_dict2, list_deg=list_deg2)

    

    def compute_interference(self):
        if self.operation == "data":
            # Process partitions with enforced schema
            self.inter = self.ddf.map_partitions(
                get_interference_partition
            )

            self.get_interference_data()
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


class DFGrepWorkflow:
    def __init__(self, ddf: dd.DataFrame):
        self.ddf = ddf
        self.selected_events = None
        self.filelist = None
        self.prod_cons = None
    
    def select_events(self):
        '''
        Filter the dataframe such that it includes only data with the files that are produced and consumed at least once.
        '''
        # self.prod_cons = self.ddf.groupby('fhash')['prod','cons'].sum()
        # self.prod_cons = self.prod_cons.query('prod > 0 and cons > 0').reset_index()
        # self.filelist = self.prod_cons.fhash.unique().compute()
        # self.selected_events = self.ddf[self.ddf.fhash.isin(self.filelist)]
        self.selected_events = self.ddf

    def get_wfGraph(self,level=1):
        if level == 1:
            # Method 1: Pass each group to compute_workflow_lvl1
            data = (self.selected_events.groupby('hostname')
                    .apply(lambda group: self.compute_workflow_for_group_l1(group), include_groups=False)
                    .reset_index())
            
            # Ensure column order matches your requirement
            data = data[['hostname', 'pid', 'fhash', 'ts']]
        
        elif level == 2:
            data = self.compute_workflow_group_l2()

    
        elif level == 3:
            data =  self.compute_workflow_group_l3()
         
        return data

    def compute_workflow_for_group_l1(self, group_df):
        """
        processed per hostname
        All threads in same process accessing common memory space
        compute_granularity = H+P  -> F (consumed == produced)
        """
        selected_events_sum = group_df.groupby(['fhash','pid']).agg({
            'prod': 'sum', 
            'cons': 'sum', 
            'ts': 'min'
        }).reset_index()
        
        final = selected_events_sum.query('prod>0 and cons>0')
        # Select only the columns you need
        return final[['pid', 'fhash', 'ts']]
    
    def compute_workflow_group_l2(self):
        # make composite key
        se = self.selected_events.assign(
            fhash_host = self.selected_events["fhash"].astype(str) + "_" + self.selected_events["hostname"].astype(str)
        )
        #part 1: Filter original data using file_host where (File, Host) produces and consumes both 
        both_prod_and_cons =  se.groupby("fhash_host")[["prod","cons"]].sum().query("prod > 0 and cons > 0").compute().index.to_numpy()
        filtered = se[se["fhash_host"].isin(both_prod_and_cons)]
        self.both_prod_cons_file_host = both_prod_and_cons #save for L3 filtering
        #part 2: Remove L2 data where produced and consumed by same pid
        f1 = filtered.groupby(["pid","fhash"])[["prod","cons"]].sum().reset_index().query(" not (prod > 0 and cons > 0)")
        return f1 #return the df with pid,fhash,prod,cons
    

    def compute_workflow_group_l3(self):
        se = self.selected_events.assign(
            fhash_host = self.selected_events["fhash"].astype(str) + "_" + self.selected_events["hostname"].astype(str)
        )
        filtered = se[~se["fhash_host"].isin(self.both_prod_cons_file_host)]
        return filtered[["fhash","hostname","prod","cons"]]

    
    # def compute_workflow_for_l2(self):
    #     selected_events_sum = self..groupby(['fhash','hhash']).agg({
    #         'prod': 'sum', 
    #         'cons': 'sum', 
    #         'ts': 'min'
    '''
    one host productiing and consuming data (lvl 1 and lvl2)

    data = groupby(["fhash","hhash"]).query("prod and cons > 0")["hhhash","fhash"]
    concat(fhash_hhash) and filter original data using this fhash_hash data  second part will remove l1
    groupby.query("fhash_hhash_pair.is.in("data["fhash_hhash"]") ").groupby("pid", "fhash").query("!(prod > 0 and cons > 0))
    '''

    '''
    data = groupby(["fhash","hhash"]).query(not "prod and cons > 0")["hhhash","fhash"]
    groupby.query("fhash_hhash_pair.is.in("data["fhash_hhash"]") ")
    '''

    #     }).reset_index()a

    def write_graph_data(self, ddf, write_path):
        ddf.to_parquet(f'{write_path}',engine='pyarrow',write_index=False)

    def build_and_save_graph_l3(self, ddf: dd.DataFrame, output_path: str):
        # Select only necessary columns and compute to pandas
        df = ddf[['hostname_prod', 'fhash', 'hostname_cons']].compute()
        output_file = output_path+"l3.csv"
        G = nx.DiGraph()
        # Add edges row by row
        for _, row in df.iterrows():
            G.add_edge(row['hostname_prod'], row['fhash'])
            G.add_edge(row['fhash'], row['hostname_cons'])

        # Unique edges (automatically handled by NetworkX DiGraph)
        unique_edges = list(G.edges())

        # Write to file
        with open(output_file, 'w') as f:
            for src, dest in unique_edges:
                f.write(f"{src} {dest}\n")

        print(f"Graph saved: {len(G.nodes)} nodes, {len(G.edges)} edges written to '{output_file}'")




class DFGrepWorkflow1:
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