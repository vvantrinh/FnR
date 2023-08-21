import datetime as dt
import time
import pytz
import gspread
import pandas as pd
import warnings
import json
import requests
from google.oauth2 import service_account

warnings.filterwarnings("ignore")
IST = pytz.timezone("Asia/Jakarta")

# Path to the service account JSON file
SERVICE_ACCOUNT_FILE = 'C:\\Users\\Admin\\AppData\\Roaming\\gspread\\service_account.json'  # Thay đổi path đến file service_account.json

# Load credentials
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/spreadsheets'])

# Create a client
client = gspread.Client(auth=scoped_credentials)

# Read the spreadsheet
spreadsheet_id = '1NH2iElGqZDDwl7aUP5ZO5h5YdO4ryO8t1VFaI0ZBZFA'  # Replace with your spreadsheet ID
sheet_name = 'summary'  # Replace with your sheet name
ws = client.open_by_key(spreadsheet_id)


def merge_row(df, key_group, col_name):  # Hàm gộp nhiều dòng  (chung user_id) thành 1 dòng
    return df.groupby(key_group)[col_name].apply(';'.join).reset_index()


hold_type_d = {'Hold type': ['Hold chờ confirm', 'Hold cố định toàn bộ tiền', 'Not_hold', 'Hold theo amount',
                             'Block Payout Request', 'Block SB Balance Payout', 'Block SPay Balance Payout'],
               'Hold_level': [1, 4, 0, 2, 5, 5, 5]}  # Phân loại cấp độ Hold type
hold_type_ref = pd.DataFrame(data=hold_type_d)


def anal_violation():
    fraud_log_sheet = ws.worksheet('fraud_log')
    fraud_log_data = pd.DataFrame.from_records(
        fraud_log_sheet.get_all_records(head=2))  # Đọc dữ liệu từ sheet Fraud_log
    dmca_log_sheet = ws.worksheet('dmca_log')
    dmca_log_data = pd.DataFrame.from_records(dmca_log_sheet.get_all_records(head=2))  # Đọc dữ liệu từ sheet dmca_log
    fraud_log_data.insert(0, 'type', 'fraud_log')
    dmca_log_data.insert(0, 'type', 'dmca_log')
    master_data = fraud_log_data.append(dmca_log_data)
    active_hold_data = master_data[(master_data.Hold_stt == 'active') & (master_data.user_email != '') & (
                master_data.user_id != '')]  # Lọc những case trạng thái hold đang là active
    active_hold_data.end_hold_date = active_hold_data.end_hold_date.replace(
        ['Không xác định', 'chưa xác định', 'Chưa xác định', 'không xác định', 'Until No TM', 'until no TM',
         'Until no TM'], '31-Dec-2100')  # Đặt các case Hold ko xác định end date có end date là 31-Dec-2100
    active_hold_data.end_hold_date = pd.to_datetime(active_hold_data.end_hold_date)  # Chuyển định dạng sang date time
    active_hold_data.log_date = pd.to_datetime(active_hold_data.log_date)
    active_hold_data.loc[(active_hold_data['Hold type'] != 'Not_hold') & (active_hold_data['Hold amount'] == ''), 'Hold amount'] = '99999999'  # Đặt các case hold toàn bộ thành hold amount 99999999
    active_hold_data['Hold amount'] = active_hold_data['Hold amount'].astype(float)
    active_hold_data = pd.merge(active_hold_data, hold_type_ref, on='Hold type', how='left')
    active_hold_data.sort_values(ascending=False, by=['user_id', 'end_hold_date', 'Hold_level', 'Hold amount',
                                                      'log_date'])  # Sắp xếp lại data
    active_hold_data = active_hold_data.reset_index(drop=True)  # Reset lại index từng dòng
    active_hold_data['id'] = active_hold_data.index
    hold_stt_list = active_hold_data.groupby('user_id', as_index=False).agg(
        {'Hold_level': 'idxmax', 'end_hold_date': 'max', 'Hold amount': 'sum', 'Log reason': 'count'}).rename(
        columns={'Hold_level': 'id', 'Log reason': 'Log_reason_count'})  # Tạo bảng tổng hợp từ dữ liệu hold active
    active_hold_log_reason = pd.pivot_table(active_hold_data, values='Hold type', index='user_id', columns='Log reason',
                                            aggfunc='count', fill_value=0, margins=True).reset_index()
    active_hold_log_reason['Fraud_violation'] = active_hold_log_reason.All - active_hold_log_reason.DMCA_violation
    summary_data = pd.merge(hold_stt_list, active_hold_data[['id', 'Hold type']], on='id', how='left')
    summary_data.end_hold_date = summary_data.end_hold_date.dt.strftime('%Y-%m-%d')
    email_df = active_hold_data[['user_id', 'user_email']]
    email_df = email_df.drop_duplicates(subset=['user_email'], keep='first')
    logger = merge_row(active_hold_data, 'user_id', 'Logger')  # Liệt kê các logger những case active hold
    logreason = merge_row(active_hold_data, 'user_id', 'Log reason')  # Liệt kê các log reason
    logpltype = merge_row(active_hold_data, 'user_id', 'PL_type')  # Liệt kê các PL_type
    user_email_list = merge_row(email_df, 'user_id', 'user_email')  # gộp các user email cùng chung user_id
    summary_data = pd.merge(summary_data, logreason, on='user_id', how='left')
    summary_data = pd.merge(summary_data, logger, on='user_id', how='left')
    summary_data = pd.merge(summary_data, logpltype, on='user_id', how='left')
    summary_data = pd.merge(summary_data, user_email_list, on='user_id', how='left')
    summary_data = pd.merge(summary_data, active_hold_log_reason[['user_id', 'DMCA_violation', 'Fraud_violation']],
                            on='user_id', how='left')
    summary_data.loc[summary_data['Hold amount'] >= 99999999, "Hold amount"] = 99999999
    summary_data['last_update_time'] = dt.datetime.now(IST).strftime("%m/%d/%Y, %H:%M:%S")
    summary_data = summary_data[
        ['user_id', 'user_email', 'Log_reason_count', 'DMCA_violation', 'Fraud_violation', 'PL_type',
         'Log reason', 'Logger', 'Hold amount', 'Hold type', 'end_hold_date',
         'last_update_time']]
    summary_data.fillna('', inplace=True)
    summary_sheet = ws.worksheet('summary')
    summary_sheet.clear()
    summary_sheet.update(None, [
        summary_data.columns.to_list()] + summary_data.to_numpy().tolist())  # Ghi data lên google sheet "Summary"


# In[7]:


# for i in range(1,80):# chạy 40 lần 1 ngày, nếu giảm thời gian cập nhật xuống thì cần tăng số vòng lặp lên
# anal_violation()
# print('Last Update: ',dt.datetime.now(IST).strftime("%Y-%m-%d, %H:%M:%S"))
# time.sleep(300) # đặt thơi gian mỗi vòng lặp 600 giây=10 phút cập nhật 1 lần/ 300 giây=5 phút/lần


# In[ ]:


webhook_url = 'https://hooks.slack.com/services/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'  # Link webhook của channel Error-noti-violation-log
slack_data = {
    'text': "Code violation đang bị lỗi, <!subteam^S02JEGDLREK|fraud-risk> check lại input ở 2 sheet fraud_log và dmca_log"}  # thông báo lỗi về channel
for i in range(1, 80):  # chạy 80 lần 1 ngày, nếu giảm thời gian cập nhật xuống thì cần tăng số vòng lặp lên
    try:
        anal_violation()
        print('Last Update: ', dt.datetime.now(IST).strftime("%Y-%m-%d, %H:%M:%S"))
        time.sleep(300)  # đặt thơi gian mỗi vòng lặp 600 giây=10 phút cập nhật 1 lần/ 300 giây=5 phút/lần
    except Exception as ex:  # gửi tin nhắn về slack từ colab
        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
