import pymysql
import pandas as pd
from datetime import datetime
from datetime import time
import numpy as np
import os
import re



# Connect to MariaDB
conn = pymysql.connect(
    host='172.20.0.166',
    user='jxiong',
    password='S1mc0na2025!',
    database='ScannerData'
)

def build_date_filter(target_date):
    if isinstance(target_date, str):
        return f"DATE(scan_time) = '{target_date}'"
    
    elif isinstance(target_date, (list, tuple)):
        target_date_sql = ", ".join([f"'{d}'" for d in target_date])
        return f"DATE(scan_time) IN ({target_date_sql})"

    else:
        raise ValueError("Invalid target_date format")


date_filter = build_date_filter('2025-4-30','2025-5-1')

query = f"""
SELECT * FROM Scans
WHERE {date_filter}
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



df['InputTime'] = pd.to_datetime(df['InputTime'].astype(str)) #transform to datetime format

df.sort_values(by=['ID', 'InputTime'], inplace=True) #sort the data by time

df.to_excel("C:/Users/jxiong/Downloads/check4.xlsx",index = False)
#print(df)

# import worker list
url = "https://raw.githubusercontent.com/JieXiong0111/TimeData_Processing/main/Worker%20List.xlsx"
df_worker = pd.read_excel(url, engine="openpyxl")


# left merge, based on df. change the ID number to worker name
df = df.merge(df_worker[['ID', 'Name']], on='ID', how='left')
df.drop(columns=['ID'], inplace=True)
df.rename(columns={'Name': 'ID'}, inplace=True)



#print(df)
df.to_excel("C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing//Commented_BeforeInt/check6.xlsx", index=False)


#--------Group data based on 15s time interval---------
def is_job_number(val):
    return bool(re.match(r'^[A-Za-z]\d{5}$', str(val).strip()))

def is_sequence(val):
    return bool(re.match(r'^\d{3}$', str(val).strip()))

def is_status(val):
    return str(val).strip() in ['Start', 'End', 'End Partially']



# First Group(based on 15s time interval)
def time_based_grouping(df):
    df = df.sort_values(by=['ID', 'InputTime']).reset_index(drop=True)
    df['Group'] = 0

    def group_scans(sub_df):
        group = 0
        group_ids = []
        group_start_time = None

        for _, row in sub_df.iterrows():
            time = row['InputTime']
            if group_start_time is None or (time - group_start_time).total_seconds() > 15:
                group += 1
                group_start_time = time
            group_ids.append(group)

        sub_df['Group'] = group_ids
        return sub_df

    return df.groupby('ID', group_keys=False).apply(group_scans)


df = time_based_grouping(df)
#df = logic_refine_group(df)
#print(df)



#--------integerate matched data--------------
def aggregate_group(group):
    result = {
        'ID': group['ID'].iloc[0],
        'Time': group['InputTime'].max()
    }

    # Job Number
    job = group.loc[group['Input'].str.contains(r'^[A-Za-z]\d{5}$', na=False), 'Input'] #change the identification of job number, the input in the format of 'a letter + five digits'
    result['Job_Number'] = job.iloc[-1] if not job.empty else 'NA'  #take the last input of 'Job Number' within the group
    # print("Job Match in Group:", job.tolist())  
    
    # Sequence
    seq = group.loc[
    group['Input'].apply(lambda x: bool(re.fullmatch(r'\d{3}', str(x))) or str(x) == 'Training'),'Input']    
    result['Sequence'] = seq.iloc[-1] if not seq.empty else 'NA'

    # Status
    status = group.loc[group['Input'].isin(['Start', 'End','End Partially']), 'Input']
    result['Status'] = status.iloc[-1] if not status.empty else 'NA' #take the last input of 'status' within the group
    
    if result['Sequence'] == 'Training':
        result['Job_Number'] = 'M00000'   
    
    return pd.Series(result)

# Define desired input(exclude some unexpected input)
job_pattern = r'^[A-Za-z]\d{5}$'    
seq_pattern = r'^\d{3}$|^Training$'
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
#print(df[df['ID']=='1C106BD2'])




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

df['Date'] = df['Time'].dt.date

df.sort_values(by=['ID', 'Time'], inplace=True) 

df = df.groupby(['ID', 'Date'], group_keys=False).apply(fill_missing_status)

#remark the NA job Number and Sequence
df['Remark_Job'] = df['Job_Number'].apply(lambda x: 'Missing Job_Number' if x == 'NA' else '')
df['Remark_Seq'] = df['Sequence'].apply(lambda x: 'Missing Sequence' if x == 'NA' else '')
df['Remark_Status'] = df['Remark_Status'].replace('NA', '')

#Integrate remark columns
df['Remark'] = df[['Remark_Job', 'Remark_Seq', 'Remark_Status']].apply(
    lambda row: '/'.join(filter(None, row)), axis=1
)

df.drop(columns=['Remark_Job', 'Remark_Seq', 'Remark_Status'], inplace=True)
df = df[['Date','ID', 'Job_Number', 'Sequence', 'Time','Status','Remark']]

df.sort_values(by=['Date','ID'], inplace=True) 

fillstatus_df = df
#print(fillstatus_df)




'''
#----------------------------Output result------------------------------------
# Create Output file
output_dir = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Commented_BeforeInt"
os.makedirs(output_dir, exist_ok=True)

# Get ID whose records including records
commented_df = fillstatus_df[fillstatus_df['Remark'] != '']
commented_ids = commented_df['ID'].unique()

output_file = os.path.join(output_dir, "Commented Records.xlsx")

# Get all the commented ID into a single excel, each ID records corresponds to a sheet
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    for id_ in commented_ids:
        id_data = fillstatus_df[fillstatus_df['ID'] == id_]
        id_data = id_data.drop(columns=['Group'], errors='ignore')
        sheet_name = str(id_)[:31].replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_')

        id_data.to_excel(writer, sheet_name=sheet_name, index=False)

'''



#----------Load the updated data------------------------------------
modified_file = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Commented_BeforeInt/Commented Records.xlsx"
xls = pd.ExcelFile(modified_file)

modified_df_list = []
for sheet in xls.sheet_names:
    df_sheet = pd.read_excel(xls, sheet_name=sheet, dtype={'Sequence': str})
    modified_df_list.append(df_sheet)

modified_df = pd.concat(modified_df_list, ignore_index=True)

# make sure the date format is the same
modified_df['Time'] = pd.to_datetime(modified_df['Time'])
df['Time'] = pd.to_datetime(df['Time'])

# Locate based on 'ID' and 'Input'
df.set_index(['ID', 'Time'], inplace=True)
modified_df.set_index(['ID', 'Time'], inplace=True)

# Update
df.update(modified_df)
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

# Reset index
df.reset_index(inplace=True)
df = df.drop(columns=['Remark'], errors='ignore')

df = df[['ID', 'Date', 'Job_Number', 'Sequence', 'Time','Status']]


#print(df)



#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#
#Calculate units completed
completed_df = df[df['Status'] == 'End']

#Count the number of 'End' as Units completed
units_completed = completed_df.groupby(['ID', 'Date', 'Job_Number', 'Sequence']) \
    .size() \
    .reset_index(name='Units_Completed')

units_completed.rename(columns={'ID': 'Name'}, inplace=True)

# print(units_completed)
#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#








#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#
#Calculation of the job duration


#-------------------Integrate Start Time and End Time (same-day only)---------
df_dur = df.copy()
df_dur = df_dur.drop(columns=['Remark'], errors='ignore')

# Get the date
df_dur['Date'] = df_dur['Time'].dt.date

result = []
used_end_times = set()

# Group data based on ID + Job_Number + Sequence + Date 
group_keys = ['ID', 'Job_Number', 'Sequence', 'Date']
for keys, group in df_dur.groupby(group_keys):
    id, job, seq, date = keys
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
                'Date': date,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': end_row['Time'],
                'Comment': ''
            })
            used_end_times.add(end_row['Time'])
            end_idx += 1
        else:
            result.append({
                'ID': id,
                'Date': date,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': pd.NaT,
                'Comment': 'Missing End'
            })

# End without Start will be filled with NaT at the Start Time and commented
all_ends = df_dur[df_dur['Status'].isin(['End', 'End Partially'])]
unused_ends = all_ends[~all_ends['Time'].isin(used_end_times)]

for _, end_row in unused_ends.iterrows():
    result.append({
        'ID': end_row['ID'],
        'Date': end_row['Time'].date(),
        'Job_Number': end_row['Job_Number'],
        'Sequence': end_row['Sequence'],
        'StartTime': pd.NaT,
        'EndTime': end_row['Time'],
        'Comment': 'Missing Start'
    })

# Output result
df_dur = pd.DataFrame(result)
df_dur = df_dur.sort_values(by=['ID', 'StartTime']).reset_index(drop=True)


#print(df_dur)



#----------------------Comment on record which doesn't check out during lunch/break-----
# Identify break&lunch time
break_times = [(time(9, 0), time(9, 15)), (time(14, 0), time(14, 15))]
lunch_time = (time(11, 55), time(13, 5))

def includes_time_range(start, end, check_start, check_end):
    if pd.isna(start) or pd.isna(end):
        return False
    return (start.time() <= check_start and end.time() >= check_end)

# Tranverse each row to see if it contains break/lunch time
for idx, row in df_dur.iterrows():
    start = row['StartTime']
    end = row['EndTime']
    comment = row['Comment']

    break_included = any(includes_time_range(start, end, bt_start, bt_end) for bt_start, bt_end in break_times)
    lunch_included = includes_time_range(start, end, *lunch_time)

    if break_included:
        comment += ' | Break Time Included' if comment else 'Break Time Included'
    if lunch_included:
        comment += ' | Lunch Included' if comment else 'Lunch Included'

    df_dur.at[idx, 'Comment'] = comment

# Mark(*) on work duration longer than 195 minutes
for idx, row in df_dur.iterrows():
    start = row['StartTime']
    end = row['EndTime']
    comment = row.get('Comment', '') or ''

    if not pd.isna(start) and not pd.isna(end):
        duration_minutes = (end - start).total_seconds() / 60
        if duration_minutes > 195 and '*' not in comment:
            comment += '*'
            df_dur.at[idx, 'Comment'] = comment.strip()

df_dur = df_dur[['Date','ID', 'Job_Number', 'Sequence', 'StartTime','EndTime','Comment']]

df_dur.sort_values(by=['Date','ID'], inplace=True) 
# print(df_dur)




'''
#---------output results----------
# Create output folder
output_dir = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Commented_AfterInt"
os.makedirs(output_dir, exist_ok=True)

# Extract records with comments
commented_df = df_dur[df_dur['Comment'] != '']
commented_ids = commented_df['ID'].unique()

# Output path
output_path = os.path.join(output_dir, "Commented Records.xlsx")

# Get all the output into a single excel file, each ID with a different sheet
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    for id_ in commented_ids:
        id_data = df_dur[df_dur['ID'] == id_]

        id_data_sorted = id_data.sort_values(by=['StartTime'])
        
        # Name the sheet name as 'ID' name and set rules to restrict some possible messy names
        sheet_name = str(id_)[:31].replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_')
        
        id_data.to_excel(writer, sheet_name=sheet_name, index=True)
'''




#--------------------Load data--------------------------------------
modified_file2 = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Commented_AfterInt/Commented Records.xlsx"
xls2 = pd.ExcelFile(modified_file2)

modified_df_list2 = []
for sheet in xls2.sheet_names:
    df_sheet = pd.read_excel(xls2, sheet_name=sheet, dtype={'Sequence': str}, index_col=0)
    modified_df_list2.append(df_sheet)

# Merge data
modified_df2 = pd.concat(modified_df_list2)

#update df_dur based on index
update_part = modified_df2[modified_df2.index.notna()]
new_rows = modified_df2[modified_df2.index.isna()]

df_dur.update(update_part)
df_dur = pd.concat([df_dur, new_rows], ignore_index=False)

df_dur['Date'] = pd.to_datetime(df_dur['Date']).dt.strftime('%Y-%m-%d')

#print(df_dur)



#----------------Duration Calculation---------------------------
df_dur = df_dur.drop(columns=['Comment'], errors='ignore')

df_dur['Duration_Hours'] = (df_dur['EndTime'] - df_dur['StartTime']).dt.total_seconds() / 3600 #add a new column to calculate the duration
Duration_df = df_dur[df_dur['EndTime'].notna()]

Duration_df['Duration_Hours'] = Duration_df['Duration_Hours'].round(2)
Duration_df.rename(columns={'ID': 'Name'}, inplace=True)

#Merge worker number
Duration_df = Duration_df.merge(df_worker[['Name','Number']], on='Name', how='left')
Duration_df = Duration_df.groupby(['Date','Name', 'Number', 'Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()

Duration_df = Duration_df[['Date', 'Name', 'Number', 'Job_Number', 'Sequence', 'Duration_Hours']]

#print(Duration_df)


#--------------Calculation of job duration=-----------------------
grouped_duration = Duration_df.groupby(['Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()

grouped_duration.rename(columns={'Duration_Hours': 'Total_Duration'}, inplace=True)

#print(grouped_duration)


#---------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------#







#Merge to get the final result

merged_df = pd.merge(Duration_df, units_completed, on=['Date','Name', 'Job_Number', 'Sequence'], how='left')

# if there's no units_completed, fill in with 0
merged_df['Units_Completed'] = merged_df['Units_Completed'].fillna(0).astype(int)

#Merge worker number
print(merged_df)










