import streamlit as st
import pandas as pd
import pymysql
import re
from io import BytesIO
from datetime import datetime, time
import numpy as np

# App title
st.title("📊 Scanning Data Processing Interface")

# Step tracker
step = st.session_state.get("step", 1)

# Utility: Convert DataFrame to downloadable Excel
@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data

# Utility: Create Excel download button
def download_excel_button(df, label, filename):
    excel_data = to_excel(df)
    st.download_button(
        label=label,
        data=excel_data,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Database connection utility
@st.cache_data
def load_raw_data(start_date, end_date):
    worker_url = "https://raw.githubusercontent.com/JieXiong0111/TimeData_Processing/main/Worker%20List.xlsx"
    df_worker = pd.read_excel(worker_url, engine="openpyxl")
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

    if 'id' in df.columns:
        df.drop(columns=['id'], inplace=True)

    df.rename(columns={
        'device_sn': 'ID',
        'scanned_data': 'Input',
        'scan_time': 'InputTime'
    }, inplace=True)

    df['Date'] = df['InputTime'].dt.date
    df['InputTime'] = pd.to_datetime(df['InputTime'].astype(str))
    df.sort_values(by=['ID', 'InputTime'], inplace=True)

    df = df.merge(df_worker[['ID', 'Name']], on='ID', how='left')
    df.drop(columns=['ID'], inplace=True)
    df = df[['Date', 'Name', 'Input', 'InputTime']]
    return df

# Grouping and remark logic
@st.cache_data
def process_output1(df):
    def is_job_number(val):
        return bool(re.match(r'^[A-Za-z]\d{5}$', str(val).strip()))

    def is_sequence(val):
        return bool(re.match(r'^\d{3}$', str(val).strip()))

    def is_status(val):
        return str(val).strip() in ['Start', 'End', 'End Partially']

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

    def aggregate_group(group):
        result = {
            'Name': group['Name'].iloc[0],
            'Time': group['InputTime'].max()
        }
        job = group.loc[group['Input'].str.contains(r'^[A-Za-z]\d{5}$', na=False), 'Input']
        result['Job_Number'] = job.iloc[-1] if not job.empty else 'NA'
        seq = group.loc[group['Input'].str.fullmatch(r'\d{3}', na=False), 'Input']
        result['Sequence'] = seq.iloc[-1] if not seq.empty else 'NA'
        status = group.loc[group['Input'].isin(['Start', 'End', 'End Partially']), 'Input']
        result['Status'] = status.iloc[-1] if not status.empty else 'NA'
        return pd.Series(result)

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

    job_pattern = r'^[A-Za-z]\d{5}$'
    seq_pattern = r'^\d{3}$'
    status_values = ['Start', 'End', 'End Partially']

    df = time_based_grouping(df)
    df = df[
        df['Input'].str.match(job_pattern, na=False) |
        df['Input'].str.match(seq_pattern, na=False) |
        df['Input'].isin(status_values)
    ]
    df = df.groupby(['Name', 'Group'], as_index=False, group_keys=False).apply(aggregate_group)

    df['NA_Count'] = df[['Job_Number', 'Sequence', 'Status']].apply(lambda row: sum(row == 'NA'), axis=1)
    df = df[df['NA_Count'] < 2]
    df.drop(columns=['NA_Count'], inplace=True)

    df['Date'] = df['Time'].dt.date
    df.sort_values(by=['Name', 'Time'], inplace=True)
    df = df.groupby(['Name', 'Date'], group_keys=False).apply(fill_missing_status)

    df['Remark_Job'] = df['Job_Number'].apply(lambda x: 'Missing Job_Number' if x == 'NA' else '')
    df['Remark_Seq'] = df['Sequence'].apply(lambda x: 'Missing Sequence' if x == 'NA' else '')
    df['Remark_Status'] = df['Remark_Status'].replace('NA', '')

    df['Remark'] = df[['Remark_Job', 'Remark_Seq', 'Remark_Status']].apply(
        lambda row: '/'.join(filter(None, row)), axis=1
    )

    df.drop(columns=['Remark_Job', 'Remark_Seq', 'Remark_Status'], inplace=True)
    df.rename(columns={'ID': 'Name'}, inplace=True)
    df = df[['Date', 'Name', 'Job_Number', 'Sequence', 'Time', 'Status', 'Remark']]
    df.sort_values(by=['Date', 'Name'], inplace=True)

    commented_ids = df[df['Remark'] != '']['Name'].unique()
    df = df[df['Name'].isin(commented_ids)]
    return df

# Step 4: Units Completed and Job Duration Calculations
def process_output2(df):
    # Rename 'Name' to 'ID' to match Output2.py logic
    df = df.rename(columns={'Name': 'ID'})

    # Remove duplicates in the input DataFrame
    df = df.drop_duplicates(subset=['ID', 'Job_Number', 'Sequence', 'Time', 'Status'], keep='first')

    # Calculate Units Completed
    completed_df = df[df['Status'] == 'End']
    units_completed = completed_df.groupby(['ID', 'Date', 'Job_Number', 'Sequence']) \
        .size() \
        .reset_index(name='Units_Completed')
    units_completed.rename(columns={'ID': 'Name'}, inplace=True)

    # Calculate Job Duration
    df_dur = df.copy()
    df_dur['Date'] = pd.to_datetime(df_dur['Date']).dt.date
    result = []
    used_end_times = set()

    # Group data based on ID + Job_Number + Sequence + Date
    group_keys = ['ID', 'Job_Number', 'Sequence', 'Date']
    for keys, group in df_dur.groupby(group_keys):
        id, job, seq, date = keys
        group = group.sort_values(by='Time').reset_index(drop=True)

        # Remove duplicates within the group
        group = group.drop_duplicates(subset=['Time', 'Status'], keep='first')

        starts = group[group['Status'] == 'Start']
        ends_combined = group[group['Status'].isin(['End', 'End Partially'])]

        end_idx = 0
        start_idx = 0
        while start_idx < len(starts) and end_idx < len(ends_combined):
            start_row = starts.iloc[start_idx]
            end_row = ends_combined.iloc[end_idx]

            if end_row['Time'] <= start_row['Time']:
                # End time is before or at the same time as Start, skip this End
                end_idx += 1
                continue

            # Valid pairing: End time is after Start time
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
            start_idx += 1
            end_idx += 1

        # Handle remaining Starts without Ends
        while start_idx < len(starts):
            start_row = starts.iloc[start_idx]
            result.append({
                'ID': id,
                'Date': date,
                'Job_Number': job,
                'Sequence': seq,
                'StartTime': start_row['Time'],
                'EndTime': pd.NaT,
                'Comment': 'Missing End'
            })
            start_idx += 1

    # Handle End without Start
    all_ends = df_dur[df_dur['Status'].isin(['End', 'End Partially'])]
    # Deduplicate End events
    all_ends = all_ends.drop_duplicates(subset=['ID', 'Job_Number', 'Sequence', 'Time'], keep='first')
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

    df_dur = pd.DataFrame(result)
    df_dur = df_dur.sort_values(by=['ID', 'StartTime']).reset_index(drop=True)

    # Comment on records overlapping break/lunch time
    break_times = [(time(9, 0), time(9, 15)), (time(14, 0), time(14, 15))]
    lunch_time = (time(11, 55), time(13, 5))

    def includes_time_range(start, end, check_start, check_end):
        if pd.isna(start) or pd.isna(end):
            return False
        return (start.time() <= check_start and end.time() >= check_end)

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

    # Mark durations longer than 195 minutes
    for idx, row in df_dur.iterrows():
        start = row['StartTime']
        end = row['EndTime']
        comment = row.get('Comment', '') or ''
        if not pd.isna(start) and not pd.isna(end):
            duration_minutes = (end - start).total_seconds() / 60
            if duration_minutes > 195 and '*' not in comment:
                comment += '*'
                df_dur.at[idx, 'Comment'] = comment.strip()

    df_dur = df_dur[['Date', 'ID', 'Job_Number', 'Sequence', 'StartTime', 'EndTime', 'Comment']]
    df_dur.sort_values(by=['Date', 'ID'], inplace=True)
    df_dur.rename(columns={'ID': 'Name'}, inplace=True)

    return units_completed, df_dur

# Step 1: Load Raw Data
if step == 1:
    st.header("Step 1: Select Date Range and Load Raw Data")
    start_date = st.date_input("Start Date", datetime.today())
    end_date = st.date_input("End Date", datetime.today())
    if st.button("Load Raw Data"):
        df_raw = load_raw_data(start_date, end_date)
        st.session_state.df_raw = df_raw
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date
        st.session_state.step = 2
        st.rerun()

# Step 2: Review Raw Data & Continue
if step == 2:
    st.header("Step 2: Review and Modify Raw Data")
    df_raw = st.session_state.df_raw
    workers = df_raw['Name'].unique().tolist()
    selected_worker = st.selectbox("Select Worker to View Raw Data", workers)
    filtered_df_raw = df_raw[df_raw['Name'] == selected_worker]
    selected_date_raw = st.selectbox("Select Date to View", sorted(filtered_df_raw['Date'].unique()))
    filtered_df_raw = filtered_df_raw[filtered_df_raw['Date'] == selected_date_raw]
    start_date = st.session_state.get("start_date")
    end_date = st.session_state.get("end_date")
    if start_date == end_date:
        filename = f"{start_date}_raw_data.xlsx"
    else:
        filename = f"{start_date}_to_{end_date}_raw_data.xlsx"
    col1, col2 = st.columns([5, 1])
    with col2:
        download_excel_button(filtered_df_raw, "Download", filename)
    edited_df_raw = st.data_editor(
        filtered_df_raw,
        num_rows="dynamic",
        use_container_width=True,
        key="editor_step2"
    )
    col_back, col_spacer, col_continue = st.columns([1, 5, 1])
    with col_back:
        if st.button("Back"):
            st.session_state.step = 1
            st.rerun()
    with col_continue:
        if st.button("Continue"):
            mask = (df_raw['Name'] == selected_worker) & (df_raw['Date'] == selected_date_raw)
            df_raw.loc[mask] = edited_df_raw
            st.session_state.df_output1 = df_raw
            st.session_state.step = 3
            st.rerun()

# Step 3: Output1 Logic
if step == 3:
    st.header("Step 3: Output1 – Grouping & Cleaning")
    df_output1 = st.session_state.df_output1
    processed_df1 = process_output1(df_output1)
    st.session_state.df_output2 = processed_df1
    workers = processed_df1['Name'].unique().tolist()
    selected_worker = st.selectbox("Select Worker to View", workers)
    filtered_df1 = processed_df1[processed_df1['Name'] == selected_worker]
    selected_date1 = st.selectbox("Select Date to View", sorted(filtered_df1['Date'].unique()))
    filtered_df1 = filtered_df1[filtered_df1['Date'] == selected_date1]
    start_date = st.session_state.get("start_date")
    end_date = st.session_state.get("end_date")
    if start_date == end_date:
        filename = f"{start_date}_output1.xlsx"
    else:
        filename = f"{start_date}_to_{end_date}_output1.xlsx"
    col1, col2 = st.columns([5, 1])
    with col2:
        download_excel_button(filtered_df1, "Download", filename)
    edited_df1 = st.data_editor(
        filtered_df1,
        num_rows="dynamic",
        use_container_width=True,
        column_config={"Remark": st.column_config.TextColumn(width="large")},
        key="editor_step3"
    )
    col_back, col_spacer, col_continue = st.columns([1, 5, 1])
    with col_back:
        if st.button("Back"):
            st.session_state.step = 2
            st.rerun()
    with col_continue:
        if st.button("Continue"):
            processed_df1.update(edited_df1)
            st.session_state.df_output2 = processed_df1
            st.session_state.step = 4
            st.rerun()

# Step 4: Output2 – Units Completed & Job Duration
if step == 4:
    st.header("Step 4: Output2 – Units Completed & Job Duration")
    df_output2 = st.session_state.df_output2
    units_completed, df_dur = process_output2(df_output2)

    # Store units_completed and df_dur for the next step
    st.session_state.units_completed = units_completed
    st.session_state.df_dur = df_dur

    # Get unique workers
    workers = df_dur['Name'].unique().tolist()
    
    # Use a container for better control over selectbox rendering
    with st.container():
        selected_worker = st.selectbox(
            "Select Worker to View",
            workers,
            key="worker_select_step4",  # Unique key to force re-render
            help="Select a worker to filter data"
        )

        # Filter Job Duration to get unique dates
        filtered_dur = df_dur[df_dur['Name'] == selected_worker]
        
        # Ensure dates are unique, sorted, and formatted as strings for display
        date_options = sorted(filtered_dur['Date'].unique())
        date_options = [str(date) for date in date_options]  # Convert to strings for consistent rendering
        
        # Display date selectbox in a wider column layout
        col1, col2 = st.columns([3, 1])  # Wider column for selectbox
        with col1:
            selected_date = st.selectbox(
                "Select Date to View",
                date_options,
                key="date_select_step4",  # Unique key to force re-render
                help="Select a date to view data"
            )
        
        # Convert selected_date back to original format (date object)
        selected_date = pd.to_datetime(selected_date).date()

        # Filter Job Duration
        filtered_dur = df_dur[(df_dur['Name'] == selected_worker) & (df_dur['Date'] == selected_date)]

        # Download button filenames
        start_date = st.session_state.get("start_date")
        end_date = st.session_state.get("end_date")
        if start_date == end_date:
            filename_dur = f"{start_date}_job_duration.xlsx"
        else:
            filename_dur = f"{start_date}_to_{end_date}_job_duration.xlsx"

        # Display Job Duration (Main Table)
        st.subheader("Job Duration")
        col1, col2 = st.columns([5, 1])
        with col2:
            download_excel_button(filtered_dur, "Download Duration", filename_dur)
        st.dataframe(filtered_dur, use_container_width=True)

        # Navigation buttons
        col_back, col_spacer, col_finish = st.columns([1, 5, 1])
        with col_back:
            if st.button("Back"):
                st.session_state.step = 3
                st.rerun()
        with col_finish:
            if st.button("Finish"):
                st.session_state.step = 1
                st.session_state.df_raw = None
                st.session_state.df_output1 = None
                st.session_state.df_output2 = None
                st.session_state.units_completed = None
                st.session_state.df_dur = None
                st.success("Processing complete! Returning to Step 1.")
                st.rerun()