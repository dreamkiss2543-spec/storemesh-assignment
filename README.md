# Shopdata ETL

โปรเจกต์นี้อ่านข้อมูลจาก `shopdata.db` ทำความสะอาดและแปลงข้อมูลด้วย Prefect แล้วบันทึกผลลง `analytics.db`

## วิธีรัน (PowerShell)

เปิด Terminal ในโฟลเดอร์โปรเจกต์ แล้วรันตามลำดับ:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe pipeline.py
.\.venv\Scripts\python.exe test_pipeline.py
```

เมื่อการตรวจผลผ่าน จะเห็น `Tests passed: 10 customers, 17 valid orders`

## ไฟล์ในโปรเจกต์

- `shopdata.db` — ข้อมูลต้นทาง
- `exploration.sql` — SQL ตรวจปัญหาของข้อมูลต้นทาง
- `pipeline.py` — Prefect flow สำหรับอ่าน ทำความสะอาด และบันทึกข้อมูล
- `analytics.db` — ฐานข้อมูลผลลัพธ์ มีตาราง `dim_customers` และ `fct_orders`
- `clv_report.sql` — SQL รายงานมูลค่าลูกค้าตลอดอายุการใช้งาน
- `test_pipeline.py` — ตรวจจำนวนลูกค้า ออเดอร์ และยอดออเดอร์ที่ต้องมากกว่า 0

เปิด `analytics.db` ใน DB Browser for SQLite แล้วนำ SQL ใน `clv_report.sql` ไปรันที่แท็บ Execute SQL เพื่อดูรายงาน

## กติกาการทำความสะอาด

- ลูกค้าที่มี `customer_id` ซ้ำ: เก็บรายการที่มี `signup_date` ล่าสุด
- อีเมลว่าง: ใส่ `unknown@domain.com`
- เบอร์โทรศัพท์: เก็บเฉพาะตัวเลข
- ออเดอร์: เก็บเฉพาะรายการที่ `total_amount > 0`
- การแปลงเป็น USD: จับคู่อัตราแลกเปลี่ยนด้วยสกุลเงินและวันที่ออเดอร์
- ออเดอร์ USD หรือออเดอร์ที่หาอัตราแลกเปลี่ยนไม่พบ: ใช้อัตรา 1.0

ข้อมูลตัวอย่างหลังรันมีลูกค้า 10 คนและออเดอร์ 17 รายการ โดยมี 7 ออเดอร์ที่ไม่พบอัตราแลกเปลี่ยนที่ตรงกัน