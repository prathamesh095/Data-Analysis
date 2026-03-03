# Data Dictionary

## customers

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT | Unique customer identifier |
| signup_date | DATE | Customer registration date |
| acquisition_channel | TEXT | Marketing acquisition source |
| country | TEXT | Customer country |

---

## orders

| Column | Type | Description |
|--------|------|-------------|
| order_id | INT | Unique order identifier |
| customer_id | INT | FK to customers |
| order_date | DATE | Date of order |
| order_amount | NUMERIC | Order revenue |
| order_status | TEXT | Completed / Cancelled |
| ingestion_date | DATE | Simulated late-arriving timestamp |

---

## user_activity

| Column | Type | Description |
|--------|------|-------------|
| activity_id | INT | Unique activity record |
| customer_id | INT | FK to customers |
| activity_date | DATE | Activity timestamp |
| activity_type | TEXT | Login / Browse / etc |

---

## support_interactions

| Column | Type | Description |
|--------|------|-------------|
| ticket_id | INT | Support ticket ID |
| customer_id | INT | FK to customers |
| ticket_date | DATE | Ticket creation date |
| issue_type | TEXT | Category of issue |
| resolution_time_hours | NUMERIC | Resolution duration |

---

## customer_intelligence_mart

**Grain:** One row per customer

Key engineered features used for segmentation and risk scoring.