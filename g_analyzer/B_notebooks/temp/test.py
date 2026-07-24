
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

            df_group =df_group.drop(columns=["group_key"], errors="ignore")

            return df_group
        
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
        schema = {"id": "str", "name": "str", "cat": "str", 'size': "int", 'ts': "int", 'te': "int", 'dur': "int", 'trange': "int", 'mount_point': "str", 'hostname':"str", 'b_id':"int"}
        write_df.to_parquet(f"{cp_dir}/{self.app_name}",
                            name_function=lambda i: f'{idenifier}-{i}.parquet', schema=schema)

    def read_checkpoint(self, id, cp_dir):
        '''
        read the dataframe from checkpoint files
        '''
        identifier = id+"-"+self.operation
        read_df = dd.read_parquet(f"{cp_dir}/{self.app_name}/{identifier}*.parquet")
        setattr(self, id, read_df)

