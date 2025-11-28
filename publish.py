import pandas as pd
from sqlalchemy import create_engine
import gspread
from google.oauth2.service_account import Credentials

def main():
    engine = create_engine('postgresql://root:root@localhost:5432/online_retail_II')
    print("กำลังอ่านข้อมูลจาก production schema...")
    
    df_main = pd.read_sql_table('online_retail_clean', engine, schema='production')
    df_main = df_main.head(10000)  
    
    
    df_daily = pd.read_sql_table('daily_sales_summary', engine, schema='production')
    df_monthly = pd.read_sql_table('monthly_sales_summary', engine, schema='production')
    df_product_pairs = pd.read_sql_table('product_pair_summary', engine, schema='production')
    
    for df in [df_main, df_daily, df_monthly, df_product_pairs]:
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
        df.fillna(0, inplace=True)  
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('0', '') 
    
    print("กำลังเชื่อมต่อ Google Sheets...")
    
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open('Online Retail Dashboard Data')
    except gspread.exceptions.SpreadsheetNotFound:
        print("สร้าง Google Sheets ใหม่...")
        sheet = client.create('Online Retail Dashboard Data')
        sheet.add_worksheet('Main Data', 1000, 20)
        sheet.add_worksheet('Daily Summary', 1000, 10)
        sheet.add_worksheet('Monthly Summary', 1000, 10)
        sheet.add_worksheet('Product Pairs', 1000, 10)
    
    print("กำลังอัปเดต Main Data...")
    worksheet_main = sheet.worksheet('Main Data')
    worksheet_main.clear()
    worksheet_main.update([df_main.columns.values.tolist()] + df_main.values.tolist())
    
    print("กำลังอัปเดต Daily Summary...")
    worksheet_daily = sheet.worksheet('Daily Summary')
    worksheet_daily.clear()
    worksheet_daily.update([df_daily.columns.values.tolist()] + df_daily.values.tolist())
    
    print("กำลังอัปเดต Monthly Summary...")
    worksheet_monthly = sheet.worksheet('Monthly Summary')
    worksheet_monthly.clear()
    worksheet_monthly.update([df_monthly.columns.values.tolist()] + df_monthly.values.tolist())
    
    print("กำลังอัปเดต Product Pairs...")
    try:
        worksheet_pairs = sheet.worksheet('Product Pairs')
    except gspread.exceptions.WorksheetNotFound:
        worksheet_pairs = sheet.add_worksheet('Product Pairs', 1000, 10)
    worksheet_pairs.clear()
    worksheet_pairs.update([df_product_pairs.columns.values.tolist()] + df_product_pairs.values.tolist())
    
    print("Publication สำเร็จ!")
    print(f"Main Data: {len(df_main):,} แถว")
    print(f"Daily Summary: {len(df_daily):,} แถว")
    print(f"Monthly Summary: {len(df_monthly):,} แถว")
    print(f"Product Pairs: {len(df_product_pairs):,} แถว")
    print("ข้อมูลถูกส่งไป Google Sheets เรียบร้อย")

if __name__ == "__main__":
    main()
