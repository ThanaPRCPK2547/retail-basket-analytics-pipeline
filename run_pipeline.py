import subprocess
import sys
from datetime import datetime

def run_script(script_name):
    try:
        print(f"กำลังรัน {script_name}...")
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, text=True, check=True)
        print(result.stdout)
        print(f"success {script_name} สำเร็จ")
        return True
    except subprocess.CalledProcessError as e:
        print(f"unsucess {script_name} ล้มเหลว:")
        print(f"Error: {e.stderr}")
        return False

def main():
    start_time = datetime.now()
    print("=" * 50)
    print("เริ่มต้น Data Pipeline")
    print(f"เวลาเริ่มต้น: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    steps = [
        ("ingest.py", "Extraction & Load"),
        ("transform.py", "Data Transformation"),
        ("publish.py", "Publication to Google Sheets")
    ]
    
    success_count = 0
    
    for script, description in steps:
        print(f"\n{description}")
        print("-" * 30)
        
        if run_script(script):
            success_count += 1
        else:
            print(f"Pipeline หยุดที่ขั้นตอน: {description}")
            break
    
    # สรุปผลลัพธ์
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 50)
    print("สรุปผลการรัน Pipeline")
    print("=" * 50)
    print(f"ขั้นตอนที่สำเร็จ: {success_count}/{len(steps)}")
    print(f"เวลาที่ใช้: {duration}")
    print(f"เวลาสิ้นสุด: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success_count == len(steps):
        print("Data Pipeline รันสำเร็จทั้งหมด!")
        return 0
    else:
        print("Data Pipeline รันไม่สำเร็จ")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
