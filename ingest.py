import pandas as pd
from sqlalchemy import create_engine, text

def main():
    # เชื่อมต่อ PostgreSQL Docker
    engine = create_engine('postgresql://root:root@localhost:5432/online_retail_II')
    
    # สร้าง schema raw_data
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data"))
        conn.commit()
    
    # Extraction: อ่านข้อมูลจาก CSV
    print("กำลังดึงข้อมูลจาก online_retail_II.csv...")
    df = pd.read_csv('online_retail_II.csv')
    
    # Load: โหลดข้อมูลลง PostgreSQL
    print("กำลังโหลดข้อมูลลง PostgreSQL...")
    df.to_sql('online_retail_II', engine, schema='raw_data', if_exists='replace', index=False)
    
    print(f"Extraction & Load สำเร็จ!")
    print(f"โหลดข้อมูล {len(df):,} แถว ลงตาราง raw_data.online_retail_II")
    print(f"คอลัมน์: {list(df.columns)}")

if __name__ == "__main__":
    main()
