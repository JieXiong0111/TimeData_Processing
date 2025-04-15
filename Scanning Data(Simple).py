import pandas as pd

file_path = "C:/Users/jxiong/OneDrive - Simcona Electronics/Documents/Scanning Data Processing/Scanner Sample(Simple).xlsx"

sheet_name = "Scanning Data"
df = pd.read_excel(file_path, header=None, names=["ID", "Input", "InputTime"])

df['InputTime'] = pd.to_datetime(df['InputTime'].astype(str)) #transform to datetime format

df.sort_values(by=['ID', 'InputTime'], inplace=True) #sort the data by time

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

def aggregate_group(group): #self-identified funtion, make the 'time', 'status','sequence','ID' be at the same row
    data = {'ID': group['ID'].iloc[0], 'End_Time': group['InputTime'].max()}
    
    #Sequence
    code_values = group['Input'][group['Input'].str.fullmatch(r'\d{3}', na=False)]
    if not code_values.empty:
        data['Sequence'] = code_values.iloc[0]
    
    #Job_Number
    g_values = group['Input'][group['Input'].str.fullmatch(r'G\d+', na=False)]
    if not g_values.empty:
        data['Job_Number'] = g_values.iloc[0]
    
    #Status
    status_values = group['Input'][group['Input'].isin(['Start', 'End'])]
    if not status_values.empty:
        data['Status'] = status_values.iloc[0]
    
    return pd.Series(data) #make the data in series format

df = df.groupby(['ID', 'Group']).apply(aggregate_group).reset_index(drop=True)


df.rename(columns=lambda x: x.replace("Code_", "Sequence")
                                  .replace("G_", "Job_Number")
                                  .replace("End_Time","Time")
                                  .replace("Status_", "Status"),
                 inplace=True)  #rename columns


cleanData = df
#print(cleanData)

#-------------------Integrate Start Time and End Time---------

def extract_start_end_pairs(df):   #self-identified function, pair the start and end data
    df = df[df['Status'].isin(['Start', 'End'])].copy()

    df.sort_values(by=['ID', 'Sequence', 'Job_Number', 'Time'], inplace=True) #make sure the records are sorted by time

    results = []
    
    grouped = df.groupby(['ID', 'Sequence', 'Job_Number'])

    for (id_, seq, job), group in grouped:
        starts = group[group['Status'] == 'Start']['Time'].tolist() #extract all the start data as a list
        ends = group[group['Status'] == 'End']['Time'].tolist() #extract all the end data as a list

        start_idx = 0
        end_idx = 0

        while start_idx < len(starts):
            start_time = starts[start_idx]
            end_time = None

            #find the first end time larger than start time
            while end_idx < len(ends) and ends[end_idx] <= start_time: #traverse all the end time for start time
                end_idx += 1

            if end_idx < len(ends):
                end_time = ends[end_idx]
                end_idx += 1 

            results.append({
                'ID': id_,
                'Sequence': seq,
                'Job_Number': job,
                'Start_Time': start_time,
                'End_Time': end_time
            })

            start_idx += 1

    return pd.DataFrame(results)

paired_df = extract_start_end_pairs(df)
# print(paired_df)


#----------------Duration Calculation---------------------------

paired_df['Duration_Minutes'] = (paired_df['End_Time'] - paired_df['Start_Time']).dt.total_seconds() / 60 #add a new column to calculate the duration
Duration_df = paired_df[paired_df['End_Time'].notna()]

Duration_df['Duration_Minutes'] = Duration_df['Duration_Minutes'].round(2)
#print(Duration_df)


#--------------Calculation of job duration=-----------------------
grouped_duration = Duration_df.groupby(['Job_Number', 'Sequence'])['Duration_Minutes'].sum().reset_index()

grouped_duration.rename(columns={'Duration_Minutes': 'Total_Duration'}, inplace=True)

#print(grouped_duration)


























































