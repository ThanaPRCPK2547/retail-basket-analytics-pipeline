import pandas as pd
from sqlalchemy import create_engine, text

def main():
    # เชื่อมต่อ PostgreSQL
    engine = create_engine('postgresql://root:root@localhost:5432/online_retail_II')
    
    # อ่านข้อมูลจาก raw_data
    print("กำลังอ่านข้อมูลจาก raw_data.online_retail_II...")
    df = pd.read_sql_table('online_retail_II', engine, schema='raw_data')
    
    # Cleaning
    print("กำลังทำ Data Cleaning...")
    # ลบแถวที่มี missing values ในคอลัมน์สำคัญ
    df = df.dropna(subset=['Invoice', 'StockCode', 'Quantity', 'Price'])
    
    # แปลง data types
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    
    # สร้าง Refund column (negative quantity = refund)
    df['Refund'] = df['Quantity'] < 0
    
    # ลบข้อมูลที่ไม่สมเหตุสมผล (เก็บ refund ไว้)
    df = df[(df['Quantity'] != 0) & (df['Price'] > 0)]
    
    # Transformation & Feature Engineering
    print("กำลังสร้าง Features ใหม่...")
    # คำนวณ Total Amount
    df['TotalAmount'] = df['Quantity'] * df['Price']
    
    # สร้างคอลัมน์วันที่
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['Day'] = df['InvoiceDate'].dt.day
    df['DayOfWeek'] = df['InvoiceDate'].dt.dayofweek
    
    # Aggregation - สรุปข้อมูลรายวัน
    print("กำลังสร้างตารางสรุปรายวัน...")
    daily_sales = df.groupby(df['InvoiceDate'].dt.date).agg({
        'TotalAmount': 'sum',
        'Quantity': 'sum',
        'Invoice': 'nunique'
    }).reset_index()
    daily_sales.columns = ['Date', 'DailySales', 'DailyQuantity', 'DailyOrders']
    
    # Aggregation - สรุปข้อมูลรายเดือน
    print("กำลังสร้างตารางสรุปรายเดือน...")
    monthly_sales = df.groupby(['Year', 'Month']).agg({
        'TotalAmount': 'sum',
        'Quantity': 'sum',
        'Invoice': 'nunique'
    }).reset_index()
    monthly_sales.columns = ['Year', 'Month', 'MonthlySales', 'MonthlyQuantity', 'MonthlyOrders']
    
    # Product Pair Analysis - หาสินค้าที่ซื้อคู่กัน
    print("กำลังสร้างตาราง Product Pair Summary...")
    # จัดกลุ่มสินค้าตาม Invoice
    invoice_products = df.groupby('Invoice')['StockCode'].apply(list).reset_index()
    
    # สร้าง product pairs
    product_pairs = []
    for products in invoice_products['StockCode']:
        if len(products) >= 2:
            for i in range(len(products)):
                for j in range(i+1, len(products)):
                    pair = tuple(sorted([products[i], products[j]]))
                    product_pairs.append(pair)
    
    # นับความถี่ของแต่ละ pair
    pair_counts = pd.Series(product_pairs).value_counts().reset_index()
    pair_counts.columns = ['ProductPair', 'Frequency']
    
    # แยก product codes
    pair_counts[['Product1', 'Product2']] = pd.DataFrame(pair_counts['ProductPair'].tolist())
    
    # เพิ่มข้อมูล description
    product_desc = df[['StockCode', 'Description']].drop_duplicates()
    pair_counts = pair_counts.merge(product_desc, left_on='Product1', right_on='StockCode', how='left')
    pair_counts = pair_counts.rename(columns={'Description': 'Product1_Description'})
    pair_counts = pair_counts.merge(product_desc, left_on='Product2', right_on='StockCode', how='left', suffixes=('', '_2'))
    pair_counts = pair_counts.rename(columns={'Description': 'Product2_Description'})

    # เลือกคอลัมน์ที่ต้องการ
    product_pair_summary = pair_counts[['Product1', 'Product1_Description', 'Product2', 'Product2_Description', 'Frequency']].head(1000)
    product_pair_summary = product_pair_summary.fillna('')
    
    # สร้าง schema production
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS production"))
        conn.commit()
    
    # Load ข้อมูลที่แปลงแล้วลง production schema
    print("กำลังโหลดข้อมูลลง production schema...")
    
    # ตารางหลัก
    df.to_sql('online_retail_clean', engine, schema='production', if_exists='replace', index=False)
    
    # ตารางสรุปรายวัน
    daily_sales.to_sql('daily_sales_summary', engine, schema='production', if_exists='replace', index=False)
    
    # ตารางสรุปรายเดือน
    monthly_sales.to_sql('monthly_sales_summary', engine, schema='production', if_exists='replace', index=False)
    
    # ตาราง Product Pair Summary
    product_pair_summary.to_sql('product_pair_summary', engine, schema='production', if_exists='replace', index=False)
    
    print("Data Transformation สำเร็จ!")
    print(f"ข้อมูลหลัก: {len(df):,} แถว → production.online_retail_clean")
    print(f"สรุปรายวัน: {len(daily_sales):,} แถว → production.daily_sales_summary")
    print(f"สรุปรายเดือน: {len(monthly_sales):,} แถว → production.monthly_sales_summary")
    print(f"Product Pairs: {len(product_pair_summary):,} แถว → production.product_pair_summary")

if __name__ == "__main__":
    main()
