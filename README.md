# 🛡️ Fraud Detection Analysis

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Fraud Analysis](#fraud-analysis)
- [Customer Behavior](#customer-behavior)
- [Transaction Trends](#transaction-trends)
- [Merchant Activity](#merchant-activity)
- [Fraud Patterns & Indicators](#fraud-patterns--indicators)
- [How to Run](#how-to-run)
- [Notes](#notes)

---

## 📖 Project Overview

This project analyzes customer transactions to identify potential fraudulent activities, high-risk customer behavior, and suspicious merchant practices using a dynamic SQLite database built from CSV data.

Technologies used:
- Python (Pandas, SQLite3)
- SQL Queries
- Anomaly Detection (basic)

---

## 🛡️ Fraud Analysis

- **Number of transactions flagged as fraudulent**: `45`

- **Average Anomaly Score of flagged transactions**: `0.49`

- **Transaction types most prone to fraud**:
  - `Other` — 210 fraud cases
  - `Food` — 204 fraud cases
  - `Travel` — 198 fraud cases

---

## 👤 Customer Behavior

- **Customers with the highest transaction frequency**:
  - `1825` — 6 transactions
  - `1424` — 6 transactions
  - `1618` — 6 transactions

- **Average transaction amount by customer age group**:
  | Age Group | Average Amount (USD) |
  |:---------:|:--------------------:|
  | 25-34     | \$56.48               |
  | 18-24     | \$56.48               |
  | 45-54     | \$56.31               |
  | 35-44     | \$54.47               |
  | 55-64     | \$53.06               |

- **Average account balance of customers with flagged transactions**:
  - `1666` — \$9674.06
  - `1289` — \$9393.91
  - `1912` — \$9262.12

- **Customers with abnormal spending patterns**:
  - `1700` — Anomaly Score: 1.00
  - `1577` — Anomaly Score: 1.00
  - `1625` — Anomaly Score: 1.00

---

## 💳 Transaction Trends

- **Most frequent transaction categories**:
  | Category        | Transaction Count |
  |:---------------:|:------------------:|
  | Other           | 210                 |
  | Food            | 204                 |
  | Travel          | 198                 |
  | Online          | 196                 |
  | Retail          | 192                 |


- **Average transaction amount by category**:
  - Retail: \$53.84
  - Online: \$53.99
  - Travel: \$56.26
  - Food: \$57.47
  - Other: \$57.49


- **Monthly transaction volume**:
  | Month     | Transaction Count |
  |:---------:|:-----------------:|
  | 2022-01   | 744               |
  | 2022-02   | 256               |


- **Transaction amount distribution**:
  - Minimum: \$10.06
  - Maximum: \$99.78
  - Average: \$55.85



---

## 🏪 Merchant Activity

- **Merchant with the highest total transaction value**:
  - `2235` — \$345.61

- **Merchants frequently linked to high anomaly score transactions**:
  - `2901` — 4 high-risk transactions
  - `2479` — 4 high-risk transactions
  - `2154` — 4 high-risk transactions


- **Merchant transaction volume**:
  - `2901` — 4 transactions
  - `2850` — 4 transactions
  - `2736` — 4 transactions

---

## 🧠 Fraud Patterns & Indicators

- **Customer segments more prone to fraud**:
  - Customer 1053 — Age 25-34, Location 2288
  - Customer 1132 — Age 45-54, Location 2922
  - Customer 1165 — Age 45-54, Location 2492

- **Overlap between suspicious activity and fraud indicators**:
  - 3 transactions had overlapping suspicious flags and fraud indicators.

- **Customers with High transaction volume but low account activity customers**:
  - `1009`, `1012`, `1019`

---

## 🛠️ How to Run

1. Install dependencies:
    ```bash
    pip install pandas sqlite3
    ```

2. Create the database:
    ```python
        convert_csv_to_sqlite("AntiFraudData.db", "your_csv_folder_path/")
    ```

3. Run the analysis:
    ```python
        fraud_analysis("AntiFraudData.db")
    ```

---

## 📁 Notes

- Data sources include customer profiles, merchant profiles, transactions, and suspicious activity reports.
- Fraud is flagged based on multiple criteria, including transaction patterns, anomaly scores, and predefined suspicious categories.
- Results are refreshed dynamically based on the latest available transaction data.

---
