import streamlit as st
import pandas as pd
import pymysql
import re
from io import BytesIO
from datetime import datetime

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
    # Load worker name mapping
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

    # Merge worker names
    df = df.merge(df_worker[['ID', 'Name']], on='ID', how='left')
    df.drop(columns=['ID'], inplace=True)
    df.rename(columns={'Name': 'Name'}, inplace=True)

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
        status = group.loc[group['Input'].isin(['Start', 'End','End Partially']), 'Input']
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
    status_values = ['Start', 'End','End Partially']

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
    df = df[['Date', 'Name', 'Job_Number', 'Sequence', 'Time','Status','Remark']]
    df.sort_values(by=['Date','Name'], inplace=True)

    # Only keep workers with non-blank remarks
    # Get list of worker IDs with comments
    commented_ids = df[df['Remark'] != '']['Name'].unique()
    df = df[df['Name'].isin(commented_ids)]

    return df




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

# Download button layout 
    start_date = st.session_state.get("start_date")
    end_date = st.session_state.get("end_date")

    if start_date == end_date:
        filename = f"{start_date}_output1.xlsx"
    else:
        filename = f"{start_date}_to_{end_date}_rawdata.xlsx"
        col1, col2 = st.columns([5, 1])
        with col2:
            download_excel_button(filtered_df_raw, "Download", filename)

# show table
    edited_df_raw = st.data_editor(
        filtered_df_raw,
        num_rows="dynamic",
        use_container_width=True,
        key="editor_step2"
    )

# buttons layout on the bottom 
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
 



# Step 3: Output1 Logic with user selection by worker
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

    edited_df1 = st.data_editor(
        filtered_df1,
        num_rows="dynamic",
        use_container_width=True,
        column_config={"Remark": st.column_config.TextColumn(width="large")},
        key="editor_step3"
    )

    # ✅ 下载按钮
    download_excel_button(filtered_df1, "📥 Download Step 3 Output1", "step3_output1.xlsx")

    # ✅ 返回 Step 2
    if st.button("⬅️ Back to Step 2"):
        st.session_state.step = 2
        st.rerun()

    if st.button("Continue to Output2"):
        processed_df1.update(edited_df1)
        st.session_state.df_output2 = processed_df1
        st.session_state.step = 4
        st.rerun()

# Output2 skipped based on user request — placeholder for future logic
if step == 4:
    st.header("Step 4: Output2 – Duration & Comments")
    st.info("Output2 is currently disabled for review. Please focus on Output1 and prior steps.")
    if st.button("Show Final Result"):
        st.session_state.step = 5
        st.rerun()

if step == 5:
    # Final Step: Download
    st.header("🎉 Final Processed Result")
    df_final = st.session_state.df_final

    workers = df_final['Name'].unique().tolist()
    selected_worker = st.selectbox("Select Worker to View Final Result", workers)

    st.dataframe(df_final[df_final['Name'] == selected_worker])

    excel_data = to_excel(df_final)
    st.download_button(
        label="📥 Download Final Result",
        data=excel_data,
        file_name="Final_Result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
