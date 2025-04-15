import pandas as pd
#import pymysql

#BM Change

file_path = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Scanner Sample(advanced).xlsx"

sheet_name = "Scanning Data"
df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, names=["ID", "Input", "InputTime"])

df['InputTime'] = pd.to_datetime(df['InputTime'].astype(str)) #transform to datetime format

df.sort_values(by=['ID', 'InputTime'], inplace=True) #sort the data by time


#----remove duplicates--------
def remove_duplicates(sub_df):
    unique_rows = []  
    last_seen = {}   

    for _, row in sub_df.iterrows():
        input_val = row['Input']
        input_time = row['InputTime']

        if input_val in last_seen:
            if (input_time - last_seen[input_val]).total_seconds() <= 30:
                continue  

        unique_rows.append(row)          
        last_seen[input_val] = input_time 

    return pd.DataFrame(unique_rows)


df = df.groupby('ID', group_keys=False).apply(remove_duplicates).reset_index(drop=True)

#print(df)


#--------Group data based on 30s time interval---------
df['Group'] = 0

def group_scans(sub_df): #self-identified function, group the scanning result based on 30s gap
    group = 0 #group number, start from 1
    group_ids = [] #store the temporary group num for each group
    last_time = None #store the data from last record to compare with the next one
    for time in sub_df['InputTime']: #traverse all the records
        if last_time is None or (time - last_time).total_seconds() > 30: #if the next time input has time gap larger than 30s, then set it as a new group
            group += 1
        group_ids.append(group)
        last_time = time
    sub_df['Group'] = group_ids
    return sub_df

df = df.groupby('ID', group_keys=False).apply(group_scans) #group the data based on 'ID', apply function to each of the group, then return a ungrouped result
#print(df)



#--------integerate matched data--------------
def aggregate_group(group):
    result = {
        'ID': group['ID'].iloc[0],
        'Time': group['InputTime'].max()
    }

    # Job Number
    job = group.loc[group['Input'].str.contains(r'^G\d+', na=False), 'Input']
    result['Job_Number'] = job.iloc[0] if not job.empty else 'NA'

    # Sequence
    seq = group.loc[group['Input'].str.fullmatch(r'\d{3}', na=False), 'Input']
    result['Sequence'] = seq.iloc[0] if not seq.empty else 'NA'

    #  Status
    status = group.loc[group['Input'].isin(['Start', 'End']), 'Input']
    result['Status'] = status.iloc[0] if not status.empty else 'NA'

    return pd.Series(result)


df = df.groupby(['ID', 'Group'], as_index=False, group_keys=False).apply(aggregate_group)

cleandf = df
print(cleandf)


#-------------Fill in blank in Sequence---------------

def fill_na_sequence(sub_df):
    known_seq = sub_df[sub_df['Sequence'] != 'NA'].drop_duplicates(subset=['Job_Number'])[['Job_Number', 'Sequence']] #get all the sequence with NA filled in

    job_seq_dict = dict(zip(known_seq['Job_Number'], known_seq['Sequence']))

    for idx, row in sub_df.iterrows():   #fill in the value one by one 
        if row['Sequence'] == 'NA' and row['Job_Number'] in job_seq_dict:
            sub_df.at[idx, 'Sequence'] = job_seq_dict[row['Job_Number']]
    
    return sub_df

df = df.groupby('ID', group_keys=False).apply(fill_na_sequence)

fillseq_df = df
#print(fillseq_df)



#-------------Fill in blank in Status-----------------
def fill_missing_status(sub_df):
    sub_df = sub_df.sort_values(by='Time').reset_index(drop=True)
    expected_status = 'Start'
    for idx, row in sub_df.iterrows():
        if row['Status'] == 'NA':
            sub_df.at[idx, 'Status'] = expected_status
        expected_status = 'End' if sub_df.at[idx, 'Status'] == 'Start' else 'Start'
    return sub_df

df = df.groupby(['ID', 'Job_Number', 'Sequence'], group_keys=False).apply(fill_missing_status)

fillstatus_df = df
#print(fillstatus_df)



#-------------------Integrate Start Time and End Time---------
result = []

# tranverse ID + Job_Number + Sequence
for (id, job, seq), group in df.groupby(['ID', 'Job_Number', 'Sequence']):
    group = group.sort_values(by='Time').reset_index(drop=True)
    
    starts = group[group['Status'] == 'Start']
    ends = group[group['Status'] == 'End']
    
    end_idx = 0
    
    for _, start_row in starts.iterrows():
        # get the first end time whose Time > start
        while end_idx < len(ends) and ends.iloc[end_idx]['Time'] <= start_row['Time']:
            end_idx += 1
        
        # if theres is End
        if end_idx < len(ends):
            end_row = ends.iloc[end_idx]
            result.append({
                'ID': id,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': end_row['Time']
            })
            end_idx += 1
        else:
            # if there's no end，fill in 15:15:00
            fake_end = start_row['Time'].normalize() + pd.Timedelta(hours=15, minutes=15)
            result.append({
                'ID': id,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': fake_end
            })

# Generate DataFrame
df = pd.DataFrame(result)

paired_df = df
#print(paired_df)


#----------------Duration Calculation---------------------------

df['Duration_Hours'] = (df['EndTime'] - df['StartTime']).dt.total_seconds() / 3600 #add a new column to calculate the duration
Duration_df = df[df['EndTime'].notna()]

Duration_df['Duration_Hours'] = Duration_df['Duration_Hours'].round(2)
#print(Duration_df)


#--------------Calculation of job duration=-----------------------
grouped_duration = Duration_df.groupby(['Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()

grouped_duration.rename(columns={'Duration_Hours': 'Total_Duration'}, inplace=True)

#print(grouped_duration)

























































