import pymysql
import pandas as pd
from datetime import datetime
import numpy as np



# Connect to MariaDB
conn = pymysql.connect(
    host='172.20.0.166',
    user='jxiong',
    password='S1mc0na2025!',
    database='ScannerData'
)

query = """
SELECT * FROM Scans
"""
df = pd.read_sql(query, conn)
conn.close()

if 'id' in df.columns:
    df.drop(columns=['id'], inplace=True)

df.rename(columns={
'device_sn': 'ID',
'scanned_data': 'Input',
'scan_time': 'InputTime'
}, inplace=True)

cutoff = datetime(2025, 4, 16, 12, 0)
df = df[df['InputTime'] > cutoff]  #get the data after Apr 16th 12pm

#df.to_excel("C:/Users/jxiong/Downloads/check2.xlsx",index = False)
#print(df)


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

def group_scans(sub_df):
    group = 0
    group_ids = []
    group_start_time = None  # the start time of each group

    for time in sub_df['InputTime']:
        if group_start_time is None or (time - group_start_time).total_seconds() > 30:
            group += 1
            group_start_time = time  

        group_ids.append(group)

    sub_df['Group'] = group_ids
    return sub_df


df = df.groupby('ID', group_keys=False).apply(group_scans) #group the data based on 'ID', apply function to each of the group, then return a ungrouped result



#--------integerate matched data--------------
def aggregate_group(group):
    result = {
        'ID': group['ID'].iloc[0],
        'Time': group['InputTime'].max()
    }

    # Job Number
    job = group.loc[group['Input'].str.contains(r'^[A-Za-z]\d{5}$', na=False), 'Input'] #change the identification of job number, the input in the format of 'a letter + five digits'
    result['Job_Number'] = job.iloc[-1] if not job.empty else 'NA'  #take the last input of 'Job Number' within the group

    # Sequence
    seq = group.loc[group['Input'].str.fullmatch(r'\d{3}', na=False), 'Input']
    result['Sequence'] = seq.iloc[-1] if not seq.empty else 'NA'  #take the last input of 'Sequence' within the group

    # Status
    status = group.loc[group['Input'].isin(['Start', 'End','End Partially']), 'Input']
    result['Status'] = status.iloc[-1] if not status.empty else 'NA' #take the last input of 'status' within the group

    return pd.Series(result)

# Define desired input(exclude some unexpected input)
job_pattern = r'^[A-Za-z]\d{5}$'    
seq_pattern = r'^\d{3}$'            
status_values = ['Start', 'End','End Partially']    

# Only keep the input matching the format
df = df[
    df['Input'].str.match(job_pattern, na=False) |
    df['Input'].str.match(seq_pattern, na=False) |
    df['Input'].isin(status_values)
]


df = df.groupby(['ID', 'Group'], as_index=False, group_keys=False).apply(aggregate_group)


# Count the number of NA in each row
df['NA_Count'] = df[['Job_Number', 'Sequence', 'Status']].apply(lambda row: sum(row == 'NA'), axis=1)

# Only keeps rows with less than 2 NA
df = df[df['NA_Count'] < 2]

# Delecte the counting row
df.drop(columns=['NA_Count'], inplace=True)


cleandf = df
#print(cleandf)



#-------------Fill in blank in Status-----------------
def fill_missing_status(sub_df):
    sub_df = sub_df.sort_values(by='Time').reset_index(drop=True)
    expected_status = 'Start'
    
    sub_df['Remark_Status'] = 'NA'

    for idx, row in sub_df.iterrows():
        if row['Status'] == 'NA':
            sub_df.at[idx, 'Status'] = expected_status
            sub_df.at[idx, 'Remark_Status'] = f'Missing {expected_status}'

        # expected_status：Start → End Partially → Start → ...
        if sub_df.at[idx, 'Status'] == 'Start':
            expected_status = 'End Partially'
        elif sub_df.at[idx, 'Status'] in ['End', 'End Partially']: #fill in blank status data with 'Start'/'End Partially'
            expected_status = 'Start'
    
    return sub_df


df.sort_values(by=['ID', 'Time'], inplace=True) 

df = df.groupby(['ID'], group_keys=False).apply(fill_missing_status)

#remark the NA job Number and Sequence
df['Remark_Job'] = df['Job_Number'].apply(lambda x: 'Missing Job_Number' if x == 'NA' else '')
df['Remark_Seq'] = df['Sequence'].apply(lambda x: 'Missing Sequence' if x == 'NA' else '')
df['Remark_Status'] = df['Remark_Status'].replace('NA', '')

#Integrate remark columns
df['Remark'] = df[['Remark_Job', 'Remark_Seq', 'Remark_Status']].apply(
    lambda row: '/'.join(filter(None, row)), axis=1
)

df.drop(columns=['Remark_Job', 'Remark_Seq', 'Remark_Status'], inplace=True)

fillstatus_df = df
#print(fillstatus_df)






#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#
#Calculate units completed
completed_df = df[df['Status'] == 'End']

#Count the number of 'End' as Units completed
units_completed = completed_df.groupby(['ID', 'Job_Number', 'Sequence']) \
    .size() \
    .reset_index(name='Units_Completed')

#print(units_completed)
#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#








#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#
#Calculation of the job duration

#-------------------Integrate Start Time and End Time---------
# delete"Remark" if it exist
df_dur = df
df_dur = df_dur.drop(columns=['Remark'], errors='ignore')

result = []

# traverse ID + Job_Number + Sequence
for (id, job, seq), group in df_dur.groupby(['ID', 'Job_Number', 'Sequence']):
    group = group.sort_values(by='Time').reset_index(drop=True)

    starts = group[group['Status'] == 'Start']
    ends_combined = group[group['Status'].isin(['End', 'End Partially'])]

    end_idx = 0

    for _, start_row in starts.iterrows():
        while end_idx < len(ends_combined) and ends_combined.iloc[end_idx]['Time'] <= start_row['Time']:
            end_idx += 1

        if end_idx < len(ends_combined):
            end_row = ends_combined.iloc[end_idx]
            result.append({
                'ID': id,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': end_row['Time'],
                'Is_EndTime_Missing': False
            })
            end_idx += 1
        else:
            result.append({
                'ID': id,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': pd.NaT,
                'Is_EndTime_Missing': True
            })

# Create final DataFrame
df_dur = pd.DataFrame(result)

# add a Final_Remark column based on Is_EndTime_Missing
df_dur['EndTime_Remark'] = df_dur['Is_EndTime_Missing'].apply(lambda x: 'Missing EndTime' if x else 'NA')

# Remove the helper column
df_dur.drop(columns=['Is_EndTime_Missing'], inplace=True)


#print(df_dur)

#paired_df.to_excel("C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Comments Added.xlsx", index=False)



#-----------------------------Calculation of Duration----------------------------
#----------------Duration Calculation---------------------------

df_dur['Duration_Hours'] = (df_dur['EndTime'] - df_dur['StartTime']).dt.total_seconds() / 3600 #add a new column to calculate the duration
Duration_df = df_dur[df_dur['EndTime'].notna()]

Duration_df['Duration_Hours'] = Duration_df['Duration_Hours'].round(2)
Duration_df.drop(columns=['EndTime_Remark'], inplace=True)
#print(Duration_df)


#--------------Calculation of job duration=-----------------------
grouped_duration = Duration_df.groupby(['Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()

grouped_duration.rename(columns={'Duration_Hours': 'Total_Duration'}, inplace=True)

#print(grouped_duration)


#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#





#Merge to get the final result

merged_df = pd.merge(Duration_df, units_completed, on=['ID', 'Job_Number', 'Sequence'], how='left')

# if there's no units_completed, fill in with 0
merged_df['Units_Completed'] = merged_df['Units_Completed'].fillna(0).astype(int)

final_df = merged_df[['ID', 'Job_Number', 'Sequence', 'Units_Completed', 'Duration_Hours']]


#print(final_df)










