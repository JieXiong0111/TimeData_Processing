import streamlit as st
import pandas as pd
import pymysql
from datetime import datetime, date, time
import io
from io import BytesIO

st.title("📊 Worker Time Data Portal")

# ---------- Initialize Step ----------
if "step" not in st.session_state:
    st.session_state.step = 1



# ------------------------ STEP 1 ----------------------
if st.session_state.step == 1:
    st.header("Get Started: Extract Desired Raw Data")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.today())
    with col2:
        end_date = st.date_input("End Date", datetime.today())

    if start_date > end_date:
        st.error("⚠️ End date must be after start date.")
        st.stop()

    # Button Setting
    ol_spacer2, col_load, col_skip = st.columns([3, 1.2, 1.5])
    with col_load:
        load_clicked = st.button("Check Raw data")
    with col_skip:
        skip_clicked = st.button("Start Data Processing")

    # ---- Load data ----
    if load_clicked or skip_clicked:
        conn = pymysql.connect(
            host='172.20.0.166',
            user='jxiong',
            password='S1mc0na2025!',
            database='ScannerData'
        )

        query = f"""
        SELECT * FROM Scans
        WHERE DATE(scan_time) BETWEEN '{start_date}' AND '{end_date}'
        """
        df = pd.read_sql(query, conn)
        conn.close()

        st.session_state.start_date = start_date
        st.session_state.end_date = end_date

        if 'id' in df.columns:
            df.drop(columns=['id'], inplace=True)

        df.rename(columns={
            'device_sn': 'ID',
            'scanned_data': 'Input',
            'scan_time': 'InputTime'
        }, inplace=True)

        df['InputTime'] = pd.to_datetime(df['InputTime'].astype(str))
        df['Date'] = df['InputTime'].dt.date
        df.sort_values(by=['ID', 'InputTime'], inplace=True)

        worker_url = "https://raw.githubusercontent.com/JieXiong0111/TimeData_Processing/main/Worker%20List.xlsx"
        df_worker = pd.read_excel(worker_url, engine="openpyxl")
        df = df.merge(df_worker[['ID', 'Name']], on='ID', how='left')
        df.drop(columns=['ID'], inplace=True)

        st.session_state.df_raw = df

        if load_clicked:
            st.session_state.step = 2
        elif skip_clicked:
            st.session_state.step = 3

        st.rerun()







#----------------------- STEP2-----------------------------  
elif st.session_state.step == 2:
    st.header("Raw Data View")

    df_raw = st.session_state.df_raw

    # ---------- Worker & Date Picker ----------
    col3, col4 = st.columns(2)
    with col3:
        worker_names = df_raw['Name'].dropna().unique().tolist()
        selected_name = st.selectbox("Select Worker", worker_names, key="worker_selector")
    with col4:
        date_options = sorted(df_raw['Date'].dropna().unique().tolist())
        selected_date = st.selectbox("Select Date to View", date_options, index=len(date_options) - 1, key="date_selector")
    

    # ---------- Filter Data ----------
    df_filtered = df_raw[
        (df_raw['Name'] == selected_name) &
        (df_raw['Date'] == selected_date)
    ].reset_index(drop=True)

    # ---------- Reroder Data ----------
    ordered_cols = ['Date', 'Name'] + [col for col in df_filtered.columns if col not in ['Date', 'Name']]
    df_editable = df_filtered[ordered_cols].copy()
    df_editable.sort_values(by=['Date', 'Name','InputTime'], inplace=True)



    # ---------- Download Button ----------
    col_spacer, col_download = st.columns([5.5, 1])
    with col_download:
        output = BytesIO()
        df_download = st.session_state.df_raw.copy()
        df_download = df_download[ordered_cols]
        df_download.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        start = st.session_state.get("start_date", date.today())
        end = st.session_state.get("end_date", date.today())

        if start == end:
            file_name = f"RawData_{start}.xlsx"
        else:
            file_name = f"RawData_{start}_to_{end}.xlsx"

        st.download_button(
            label="Download",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ---------- Show Table ----------
    st.dataframe(
        df_editable,
        use_container_width=True,
        column_config={
            "InputTime": st.column_config.DatetimeColumn("Input Time", format="YYYY-MM-DD HH:mm:ss")
        },
    )

    # ---------- Back&Continue Buttons ----------
    col_back, col_spacer2 = st.columns([1, 5])

    with col_back:
        if st.button("Back"):
            st.session_state.step = 1
            st.rerun()





#------------------------------STEP 3--------------------------------------
elif st.session_state.step == 3:
    st.header("Data Processing —— Stage 1")

    # Get data from step2
    df_raw = st.session_state.df_raw.copy()


    #--------Group data based on 15s time interval---------
    import re
    
    def time_based_grouping(df):
        df = df.sort_values(by=['Name', 'InputTime']).reset_index(drop=True)
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

        return df.groupby('Name', group_keys=False).apply(group_scans)

    df = time_based_grouping(df_raw)

    #--------integrate matched data--------------
    def aggregate_group(group):
        result = {
            'Name': group['Name'].iloc[0],
            'Time': group['InputTime'].max()
        }

        job = group.loc[group['Input'].str.contains(r'^[A-Za-z]\d{5}$', na=False), 'Input']
        result['Job_Number'] = job.iloc[-1] if not job.empty else 'NA'

        seq = group.loc[group['Input'].apply(lambda x: bool(re.fullmatch(r'\d{3}', str(x))) or str(x) == 'Training'), 'Input']
        result['Sequence'] = seq.iloc[-1] if not seq.empty else 'NA'

        status = group.loc[group['Input'].isin(['Start', 'End','End Partially']), 'Input']
        result['Status'] = status.iloc[-1] if not status.empty else 'NA'
        
        if result['Sequence'] == 'Training':
            result['Job_Number'] = 'M00000'

        return pd.Series(result)

    df = df[
        df['Input'].str.match(r'^[A-Za-z]\d{5}$', na=False) |
        df['Input'].str.match(r'^\d{3}$|^Training$', na=False) |
        df['Input'].isin(['Start', 'End','End Partially'])
    ]

    df = df.groupby(['Name', 'Group'], as_index=False, group_keys=False).apply(aggregate_group)

    df['NA_Count'] = df[['Job_Number', 'Sequence', 'Status']].apply(lambda row: sum(row == 'NA'), axis=1)
    df = df[df['NA_Count'] < 2]
    df.drop(columns=['NA_Count'], inplace=True)

    df['Date'] = df['Time'].dt.date
    df.sort_values(by=['Name', 'Time'], inplace=True)

    #-------------Fill in blank in Status-----------------
    def fill_missing_status(sub_df):
        sub_df = sub_df.sort_values(by='Time').reset_index(drop=True)
        expected_status = 'Start'
        
        sub_df['Remark_Status'] = 'NA'

        for idx, row in sub_df.iterrows():
            if row['Status'] == 'NA':
                sub_df.at[idx, 'Status'] = expected_status
                sub_df.at[idx, 'Remark_Status'] = f'Missing {expected_status}'

            if sub_df.at[idx, 'Status'] == 'Start':
                expected_status = 'End Partially'
            elif sub_df.at[idx, 'Status'] in ['End', 'End Partially']:
                expected_status = 'Start'
        
        return sub_df

    df = df.groupby(['Name', 'Date'], group_keys=False).apply(fill_missing_status)

    df['Remark_Job'] = df['Job_Number'].apply(lambda x: 'Missing Job_Number' if x == 'NA' else '')
    df['Remark_Seq'] = df['Sequence'].apply(lambda x: 'Missing Sequence' if x == 'NA' else '')
    df['Remark_Status'] = df['Remark_Status'].replace('NA', '')

    df['Remark'] = df[['Remark_Job', 'Remark_Seq', 'Remark_Status']].apply(
        lambda row: '/'.join(filter(None, row)), axis=1
    )

    df.drop(columns=['Remark_Job', 'Remark_Seq', 'Remark_Status'], inplace=True)
    df = df[['Date', 'Name', 'Job_Number', 'Sequence', 'Time', 'Status', 'Remark']]
    df.sort_values(by=['Date', 'Name','Time'], inplace=True)

    st.session_state.df_output2 = df.copy()

    # ---------picker setting--------
    col1, col2 = st.columns(2)
    with col1:
        worker_names = df['Name'].dropna().unique().tolist()
        selected_name = st.selectbox("Select Worker", worker_names, key="step3_worker_selector")
    with col2:
        date_options = sorted(df['Date'].dropna().unique().tolist())
        selected_date = st.selectbox("Select Date", date_options, index=len(date_options)-1, key="step3_date_selector")

    df_filtered = df[
        (df['Name'] == selected_name) &
        (df['Date'] == selected_date)
    ].reset_index(drop=True)


    # Download button
    col_spacer, col_download = st.columns([5.5, 1])
    with col_download:
        output = BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        start = st.session_state.get("start_date", date.today())
        end = st.session_state.get("end_date", date.today())

        if start == end:
            file_name = f"Stage1Data_{start}.xlsx"
        else:
            file_name = f"Stage1Data_{start}_to_{end}.xlsx"

        st.download_button(
            label="Download",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # data display
    st.dataframe(
        df_filtered,
        use_container_width=True,
        column_config={
            "Time": st.column_config.TimeColumn("Time",width=80),
            "Remark": st.column_config.TextColumn("Remark", width="medium"),
            "Sequence": st.column_config.TextColumn("Sequence", width=80),
            "Job_Number": st.column_config.TextColumn("Job Number", width=120)
        }
    )



# ---------- Upload file ----------
    st.divider()
    st.subheader("📤 Upload File")
    uploaded_file = st.file_uploader("Upload a file with cleaned data", type=["xlsx", "csv"])

    if uploaded_file:
        if uploaded_file.name.endswith(".xlsx"):
            upload_df = pd.read_excel(uploaded_file, engine='openpyxl', dtype={'Sequence': str})
        else:
            upload_df = pd.read_csv(uploaded_file, dtype={'Sequence': str})

        st.success(f"File '{uploaded_file.name}' uploaded successfully!")
        st.dataframe(upload_df.head(), use_container_width=True)
  
    # Save data for step4
        st.session_state.df_step4_input = upload_df.copy()

# Back&Continue Buttons
    col_back, col_spacer2, col_continue = st.columns([1, 5, 1])

    with col_back:
        if st.button("Back", key="back_to_step2"):
            st.session_state.step = 1
            st.rerun()

    with col_continue:
        if st.button("Continue", key="go_to_step4"):
            st.session_state.clicked_continue_to_step4 = True  # 记录一下点击

# Alert showing
    if st.session_state.get("clicked_continue_to_step4", False):
        if "df_step4_input" not in st.session_state:
            st.error("⚠️ Please upload a file before continuing to Step 4.")
        else:
            st.session_state.step = 4
            st.rerun()





#--------------------STEP4-------------------------
elif st.session_state.step == 4:
    st.header("Data Processing —— Stage 2")

    df_step4 = st.session_state.df_step4_input.copy()

    df = df_step4.drop(columns=['Remark'], errors='ignore')
    df = df[['Name', 'Date', 'Job_Number', 'Sequence', 'Time', 'Status']]

    # ---- Units Completed Calculation ----
    completed_df = df[df['Status'] == 'End']
    units_completed = completed_df.groupby(['Name', 'Date', 'Job_Number', 'Sequence']) \
        .size() \
        .reset_index(name='Units_Completed')

    # Save it for final merge
    st.session_state.units_completed = units_completed
    df_dur = df.copy()
    df_dur['Date'] = pd.to_datetime(df_dur['Date']).dt.date

    result = []
    group_keys = ['Name', 'Job_Number', 'Sequence', 'Date']
    used_end_times = set()  
    for keys, group in df_dur.groupby(group_keys):
        name, job, seq, date = keys
        group = group.sort_values(by='Time').reset_index(drop=True)
 
        # Order start time from latest to earliest
        starts = group[group['Status'] == 'Start'].sort_values(by='Time', ascending=False).reset_index(drop=True)
        ends_combined = group[group['Status'].isin(['End', 'End Partially'])].reset_index(drop=True)

        for _, start_row in starts.iterrows():
            matched = False

            for _, end_row in ends_combined.iterrows():
                end_time = end_row['Time']
                if end_time > start_row['Time'] and end_time not in used_end_times:
                    result.append({
                        'Name': name,
                        'Date': date,
                        'Job_Number': job,
                        'Sequence': seq,
                        'StartTime': start_row['Time'],
                        'EndTime': end_time,
                        'Comment': ''
                    })
                    used_end_times.add(end_time)
                    matched = True
                    break

            if not matched:
                result.append({
                    'Name': name,
                    'Date': date,
                    'Job_Number': job,
                    'Sequence': seq,
                    'StartTime': start_row['Time'],
                    'EndTime': pd.NaT,
                    'Comment': 'Missing End'
                })


    # End without corresponding start
    all_ends = df_dur[df_dur['Status'].isin(['End', 'End Partially'])]
    unused_ends = all_ends[~all_ends['Time'].isin(used_end_times)]

    for _, end_row in unused_ends.iterrows():
        result.append({
            'Name': end_row['Name'],
            'Date': end_row['Time'].date(),
            'Job_Number': end_row['Job_Number'],
            'Sequence': end_row['Sequence'],
            'StartTime': pd.NaT,
            'EndTime': end_row['Time'],
            'Comment': 'Missing Start'
        })

    df_dur = pd.DataFrame(result)
    df_dur.sort_values(by=['Name', 'StartTime'], inplace=True)

    # ---- Comment on Lunch/Break time ----
    break_times = [(time(9, 0), time(9, 15)), (time(14, 0), time(14, 15))]
    lunch_time = (time(11, 55), time(13, 5))

    def includes_time_range(start, end, check_start, check_end):
        if pd.isna(start) or pd.isna(end):
            return False
        return (start.time() <= check_start and end.time() >= check_end)

    for idx, row in df_dur.iterrows():
        start, end = row['StartTime'], row['EndTime']
        comments = row['Comment']

        if any(includes_time_range(start, end, bt[0], bt[1]) for bt in break_times):
            comments += ' | Break Time Included' if comments else 'Break Time Included'

        if includes_time_range(start, end, *lunch_time):
            comments += ' | Lunch Included' if comments else 'Lunch Included'

        if not pd.isna(start) and not pd.isna(end):
            duration_minutes = (end - start).total_seconds() / 60
            if duration_minutes > 195 and '*' not in comments:
                comments += '*' if comments else '*'

        df_dur.at[idx, 'Comment'] = comments.strip()

    df_dur = df_dur[['Date', 'Name', 'Job_Number', 'Sequence', 'StartTime', 'EndTime', 'Comment']]
    df_dur['MinTime'] = df_dur[['StartTime', 'EndTime']].min(axis=1)
    df_dur.sort_values(by=['Date', 'Name', 'MinTime'], inplace=True)
    df_dur.drop(columns=['MinTime'], inplace=True)


    st.session_state.df_output4 = df_dur

    # ---- Page layout design ----
    col1, col2 = st.columns(2)
    with col1:
        worker_names = df_dur['Name'].dropna().unique().tolist()
        selected_name = st.selectbox("Select Worker", worker_names, key="step4_worker_selector")
    with col2:
        date_options = sorted(df_dur['Date'].dropna().unique().tolist())
        selected_date = st.selectbox("Select Date", date_options, index=len(date_options)-1, key="step4_date_selector")

    df_filtered = df_dur[
        (df_dur['Name'] == selected_name) &
        (df_dur['Date'] == selected_date)
    ].reset_index(drop=True)
     
      # Download button
    col_spacer, col_download = st.columns([5.5, 1])
    with col_download:
        output = BytesIO()
        df_dur.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        start = st.session_state.get("start_date", date.today())
        end = st.session_state.get("end_date", date.today())

        file_name = f"Stage2Data_{start}_{end}.xlsx" if start != end else f"Stage2Data_{start}.xlsx"

        st.download_button(
            label="Download",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.dataframe(
        df_filtered,
        use_container_width=True,
        column_config={
            "StartTime": st.column_config.DatetimeColumn("Start Time", format="HH:mm:ss"),
            "EndTime": st.column_config.DatetimeColumn("End Time", format="HH:mm:ss"),
            "Sequence": st.column_config.TextColumn("Sequence", width=80),
            "Job_Number": st.column_config.TextColumn("Job Number", width=100),
            "Comment": st.column_config.TextColumn("Comment", width=250),
        }
    )


# ---------- File upload ----------
    st.divider()
    st.subheader("📤 Upload File")
    uploaded_file_step4 = st.file_uploader("Upload a file with cleaned data", type=["xlsx", "csv"], key="step4_file_uploader")

    if uploaded_file_step4:
        if uploaded_file_step4.name.endswith(".xlsx"):
            upload_df_step4 = pd.read_excel(uploaded_file_step4, engine='openpyxl', dtype={'Sequence': str})
        else:
            upload_df_step4 = pd.read_csv(uploaded_file_step4, dtype={'Sequence': str})

        st.success(f"File '{uploaded_file_step4.name}' uploaded successfully!")
        st.dataframe(upload_df_step4.head(), use_container_width=True)

        # save data for step5
        st.session_state.df_step5_input = upload_df_step4.copy()

# ---------- continue button ----------
    col_spacer2, col_continue = st.columns([5, 1])

    with col_continue:
        if st.button("Continue", key="go_to_step5"):
            st.session_state.clicked_continue = True

    if st.session_state.get("clicked_continue", False):
        if "df_step5_input" not in st.session_state:
            st.error("⚠️ Please upload a file before proceeding to Step 5.")
        else:
            st.session_state.step = 5
            st.rerun()






#--------------------STEP5--------------------------------
elif st.session_state.step == 5:
    st.header("Final Review")

    # load data
    df_dur = st.session_state.df_step5_input.copy()

    # load units_completed
    units_completed = st.session_state.units_completed.copy()

    # load Worker List 
    worker_url = "https://raw.githubusercontent.com/JieXiong0111/TimeData_Processing/main/Worker%20List.xlsx"
    df_worker = pd.read_excel(worker_url, engine='openpyxl')
    
    #data processing
    df_dur = df_dur.drop(columns=['Comment'], errors='ignore')

    # drop time in date
    df_dur['Date'] = pd.to_datetime(df_dur['Date']).dt.date

    df_dur['Duration_Hours'] = (df_dur['EndTime'] - df_dur['StartTime']).dt.total_seconds() / 3600

    Duration_df = df_dur[df_dur['EndTime'].notna()].copy()
    Duration_df['Duration_Hours'] = Duration_df['Duration_Hours'].round(2)

    # Merge worker number
    Duration_df = Duration_df.merge(df_worker[['Name', 'Number']], on='Name', how='left')

    # Group by for duration
    Duration_df = Duration_df.groupby(['Date', 'Name', 'Number', 'Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()

    Duration_df = Duration_df[['Date', 'Name', 'Number', 'Job_Number', 'Sequence', 'Duration_Hours']]

    # ------------------- job duration-------------------
    Duration_df['Date'] = pd.to_datetime(Duration_df['Date']).dt.date
    units_completed['Date'] = pd.to_datetime(units_completed['Date']).dt.date
    grouped_duration = Duration_df.groupby(['Job_Number', 'Sequence'])['Duration_Hours'].sum().reset_index()
    grouped_duration.rename(columns={'Duration_Hours': 'Total_Duration'}, inplace=True)

    # ------------------- Merge -------------------
    merged_df = pd.merge(Duration_df, units_completed, on=['Date', 'Name', 'Job_Number', 'Sequence'], how='left')
    merged_df['Units_Completed'] = merged_df['Units_Completed'].fillna(0).astype(int)

    merged_df['Date'] = pd.to_datetime(merged_df['Date']).dt.date

    #-------------------Group by Week---------------
    # Copy a clean version for weekly summary
    weekly_df = merged_df.copy()

    # Add Week column
    weekly_df['Week'] = pd.to_datetime(weekly_df['Date']).dt.to_period('W').apply(lambda r: r.start_time)

    # Group by week
    weekly_summary = weekly_df.groupby(
        ['Week', 'Name', 'Number', 'Job_Number', 'Sequence'],
        as_index=False
    ).agg({
        'Duration_Hours': 'sum',
        'Units_Completed': 'sum'
    })

    # Format Week column
    weekly_summary['Week'] = weekly_summary['Week'].dt.strftime('%Y-%m-%d')

    # ------------------- Layout design -------------------
    col1, col2 = st.columns(2)
    with col1:
        worker_names = merged_df['Name'].dropna().unique().tolist()
        selected_name = st.selectbox("Select Worker", worker_names, key="step5_worker_selector")
    with col2:
        date_options = sorted(merged_df['Date'].dropna().unique().tolist())
        selected_date = st.selectbox("Select Date", date_options, index=len(date_options)-1, key="step5_date_selector")

    df_filtered = merged_df[
        (merged_df['Name'] == selected_name) &
        (merged_df['Date'] == selected_date)
    ].reset_index(drop=True)

    st.dataframe(
        df_filtered,
        use_container_width=True,
        column_config={
            "Duration_Hours": st.column_config.NumberColumn("Duration (Hours)", format="%.2f"),
            "Units_Completed": st.column_config.NumberColumn("Units Completed"),
            "Sequence": st.column_config.TextColumn("Sequence", width=80),
            "Job_Number": st.column_config.TextColumn("Job Number", width=100),
            "Date": st.column_config.DateColumn("Date"),  
        }
    )

    # ------------------- Download button -------------------
    col_reload, col_spacer, col_download, col_downloadw = st.columns([1.2, 3, 1.55,1.8])

    with col_download:
        output = BytesIO()
        merged_df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        start = st.session_state.get("start_date", date.today())
        end = st.session_state.get("end_date", date.today())

        if start == end:
            file_name = f"FinalDailyData_{start}.xlsx"
        else:
            file_name = f"FinalDailyData_{start}_to_{end}.xlsx"

        st.download_button(
            label="Daily Summary",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    with col_reload:
        if st.button("Start Over"):
           st.session_state.clear()  
           st.rerun()  

    
    with col_downloadw:
        output = BytesIO()
        weekly_summary.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        start = st.session_state.get("start_date", date.today())
        end = st.session_state.get("end_date", date.today())

        if start == end:
            file_name = f"FinalWeeklyData_{start}.xlsx"
        else:
            file_name = f"FinalWeeklyData_{start}_to_{end}.xlsx"
    
        st.download_button(
            label="Weekly Summary",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
