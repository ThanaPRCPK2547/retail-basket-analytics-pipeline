# Online Retail II Data Pipeline
เอกสารนี้อธิบายสถาปัตยกรรมและวิธีรันโปรเจ็กต์ ETL/ELT สำหรับชุดข้อมูล Online Retail II โดยใช้ PostgreSQL, pandas/SQLAlchemy และ Google Sheets เป็นปลายทางการเผยแพร่

## ภาพรวมสถาปัตยกรรม
- แหล่งข้อมูล: `online_retail_II.csv`
- Ingest: `ingest.py` โหลด CSV ลง PostgreSQL schema `raw_data` (ตาราง `raw_data.online_retail_II`)
- Transform: `transform.py` ทำความสะอาด + สร้างฟีเจอร์ + สรุปรายวัน/รายเดือน + วิเคราะห์สินค้าที่ซื้อคู่กัน แล้วบันทึกลง schema `production` (`online_retail_clean`, `daily_sales_summary`, `monthly_sales_summary`, `product_pair_summary`)
- Publish: `publish.py` ดึงตารางจาก `production` ส่งออกไป Google Sheets (“Online Retail Dashboard Data”)
- Orchestration: `run_pipeline.py` รันสามสเตปตามลำดับ พร้อมหยุดเมื่อสเตปล้มเหลว

## สิ่งที่ต้องมี
- Docker & Docker Compose (สำหรับ PostgreSQL + PgAdmin)
- Python 3.10+ และ `pip`
- บัญชี Google Service Account + ไฟล์คีย์ JSON ชื่อ `credentials.json` (อย่า commit ไฟล์นี้)

## การติดตั้งและเตรียมสภาพแวดล้อม
1) ติดตั้งไลบรารี Python ที่ใช้
```bash
python -m venv .venv
source .venv/bin/activate  # บน Windows ใช้ .venv\\Scripts\\activate
pip install pandas sqlalchemy psycopg2-binary gspread google-auth requests
```
2) เริ่มฐานข้อมูล PostgreSQL
```bash
docker-compose up -d
```
ฐานข้อมูลจะรันที่ `localhost:5432` user/pass `root` ชื่อ DB `online_retail_II` (ตาม `docker-compose.yml`)

3) ตั้งค่า Google Sheets
- สร้าง Service Account และดาวน์โหลดไฟล์คีย์เป็น `credentials.json`
- วางไฟล์ไว้ที่รากโปรเจ็กต์ (ไฟล์นี้ถูกใส่ `.gitignore` ไว้แล้ว ห้าม commit)
- ให้สิทธิ์ Editor แก่ Service Account บนสเปรดชีตเป้าหมาย หรือปล่อยให้สคริปต์สร้างชีตใหม่ (“Online Retail Dashboard Data”)

## การรัน Pipeline
- รันครบทุกสเตป: ingest → transform → publish
```bash
python run_pipeline.py
```
- รันเฉพาะบางสเตป
```bash
python ingest.py      # โหลด CSV -> raw_data
python transform.py   # แปลง/สรุป -> production
python publish.py     # ส่งออก Google Sheets
```

## การตรวจสอบผลลัพธ์
- ฐานข้อมูล: ใช้ PgAdmin (เปิดที่ http://localhost:5050) หรือไคลเอนต์ Postgres อื่นๆ ตรวจสอบ schema `raw_data` และ `production`
- Google Sheets: เปิดสเปรดชีต “Online Retail Dashboard Data” ตรวจสอบ worksheets Main Data / Daily Summary / Monthly Summary / Product Pairs

## หมายเหตุเรื่องความปลอดภัย
- อย่า commit `credentials.json` หรือแชร์ไฟล์คีย์สาธารณะ
- หากต้องการรีเซ็ตข้อมูลดิบ/ผลลัพธ์ ให้รันสคริปต์ซ้ำ (ตารางจะถูก replace ทุกครั้ง)
