import pandas as pd, sqlite3
import os
from pathlib import Path

path_to_data = Path(__file__).parent.resolve() / "data"


def convert_csv_to_sqlite(db_path, base_dir):
    conn = sqlite3.connect("AntiFraudData.db")

    #Function to process each CSV file
    def process_csv(csv_file):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)

        # Get the table name from the CSV file name
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Write the DataFrame to the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Walk through the directory to find all CSV files

    for dirpath, _, filenames in os.walk(base_dir):
        for file in filenames:
            if file.endswith('.csv'):
                csv_file = os.path.join(dirpath, file)
                process_csv(csv_file)

    # Commit changes and close the connection
    conn.commit()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    # Assuming `tables` is a list of tuples containing table names
    table_names = [table[0] for table in tables]

    # Format the table names into a string with commas separating them
    formatted_table_names = ', '.join(table_names)

    # Print the message with a clear format
    print(
        f"All CSV files from '{base_dir}' have been processed and saved to '{db_path}'. \n"
        f"The following tables were created: {formatted_table_names}.")


## Fraud Analysis
# How many transactions were flagged as fraudulent?

def fraud_analysis(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to count the number of fraudulent transactions
    total_fraud_transactions = "SELECT COUNT(*) FROM fraud_indicators WHERE FraudIndicator = 1;"
    cursor.execute(total_fraud_transactions)
    total_fraud_transactions = cursor.fetchone()

    print(
        f"Number of Transactions flagged as fraudaulent: {total_fraud_transactions[0] if total_fraud_transactions else 0}. \n")

    # Which customers had the highest number of suspicious transactions?

    suspicious_customers = ("SELECT DISTINCT CustomerID, COUNT(SuspiciousFlag) AS Suspicious_transactions_count FROM suspicious_activity \
                            WHERE SuspiciousFlag = 1 \
                            GROUP BY CustomerID \
                            ORDER BY Suspicious_transactions_count DESC;")
    cursor.execute(suspicious_customers)
    suspicious_customers = cursor.fetchall()
    formatted_suspicious_customers = [f"Customer ID: {row[0]}, Suspicious Transactions: {row[1]}" for row in
                                      suspicious_customers]
    print(f"Customers with the highest number of suspicious transactions: {formatted_suspicious_customers} \n ")

    #Which merchants are associated with the most fraud indicators?

    merchants_with_fraud = ("SELECT TM.MerchantID, TM.TransactionID, COUNT(FraudIndicator), FI.TransactionID AS fraud_count \
                            FROM fraud_indicators FI, transaction_metadata TM \
                            WHERE FI.TransactionID = TM.TransactionID \
                            GROUP BY MerchantID \
                            ORDER BY fraud_count DESC;")

    cursor.execute(merchants_with_fraud)
    merchants_with_fraud = cursor.fetchall()

    formatted_merchants_with_fraud = [f"Merchant ID: {row[0]}, Transaction ID: {row[1]}, Fraud Count: {row[2]}" for row
                                      in merchants_with_fraud]

    print(f"Merchants associated with the most fraud indicators: {formatted_merchants_with_fraud} \n ")

    #What is the average anomaly score of flagged transactions?

    avg_anomaly_score = ("SELECT AVG(AnomalyScore) FROM anomaly_scores;")

    cursor.execute(avg_anomaly_score)
    avg_anomaly_score = cursor.fetchone()
    print(f"Average Anomaly Score of flagged transactions: {avg_anomaly_score[0]} \n ")

    #Which transaction types (categories) are most prone to fraud?

    transaction_types_fraud = ("WITH Categories AS(SELECT TCL.TransactionID, TCL.Category, FI.TransactionID, COUNT(FraudIndicator) AS FraudCount \
                                FROM fraud_indicators FI, transaction_category_labels TCL \
                                WHERE FI.TransactionID = TCL.TransactionID \
                                GROUP BY Category \
                                ORDER BY FraudCount DESC) \
                                SELECT Category, FraudCount \
                                FROM Categories LIMIT 3;")
    cursor.execute(transaction_types_fraud)
    transaction_types_fraud = cursor.fetchall()
    formatted_transaction_types_fraud = [f"Category: {row[0]}, Fraud Count: {row[1]}" for row
                                         in transaction_types_fraud]
    print(f"Transaction types most prone to fraud: {formatted_transaction_types_fraud} \n ")

    ### 👤 **Customer Behavior**

    #Which customers have the highest transaction frequency?

    transaction_frequency = ("SELECT CustomerID, COUNT(TransactionID) AS TransactionCount \
                            FROM transaction_records \
                            GROUP BY CustomerID \
                            ORDER BY TransactionCount DESC;")

    cursor.execute(transaction_frequency)
    transaction_frequency = cursor.fetchall()
    formatted_transaction_frequency = [f"Customer ID: {row[0]}, Transaction Count: {row[1]}" for row
                                       in transaction_frequency]
    print(f"Customers with the highest transaction frequency: {formatted_transaction_frequency} \n ")

    #How does the average transaction amount vary by customer age group?

    avg_transaction_amount_by_age = ("SELECT \
    CASE \
        WHEN CD.Age BETWEEN 18 AND 24 THEN '18-24' \
        WHEN CD.Age BETWEEN 25 AND 34 THEN '25-34' \
        WHEN CD.Age BETWEEN 35 AND 44 THEN '35-44' \
        WHEN CD.Age BETWEEN 45 AND 54 THEN '45-54'\
        WHEN CD.Age BETWEEN 55 AND 64 THEN '55-64'\
        WHEN CD.Age >= 65 THEN '65+'\
        ELSE 'Unknown'\
    END AS age_group,\
    ROUND(AVG(TR.Amount), 2) AS avg_transaction_amount \
FROM \
    customer_data CD \
JOIN \
    transaction_records TR ON CD.CustomerID = TR.CustomerID \
GROUP BY \
    age_group \
ORDER BY \
    avg_transaction_amount DESC;")
    cursor.execute(avg_transaction_amount_by_age)
    avg_transaction_amount_by_age = cursor.fetchall()
    formatted_avg_transaction_amount_by_age = [f"Age: {row[0]}, Avg Amount: {row[1]}" for row
                                               in avg_transaction_amount_by_age]

    print(f"Average transaction amount by customer age group: {formatted_avg_transaction_amount_by_age} \n ")

    #What is the average account balance of customers with flagged transactions?

    avg_account_balance_flagged = ("SELECT AA.CustomerID, ROUND(AVG(AA.AccountBalance), 2) AS avg_balance, FI.FraudIndicator, FI.TransactionID, TR.TransactionID, TR.CustomerID FROM account_activity AA, fraud_indicators FI, transaction_records TR \
                                    WHERE FI.TransactionID = TR.TransactionID AND AA.CustomerID = TR.CustomerID \
                                    AND FI.FraudIndicator = 1 \
                                   GROUP BY AA.CustomerID ORDER BY avg_balance DESC;")
    cursor.execute(avg_account_balance_flagged)
    avg_account_balance_flagged = cursor.fetchall()

    formatted_avg_account_balance_flagged = [f"Customer ID: {row[0]}, Avg Balance: {row[1]}" for row in
                                             avg_account_balance_flagged]

    print(
        f"Average account balance of customers with flagged transactions: {formatted_avg_account_balance_flagged} \n ")

    #Which customers had abnormal spending patterns based on anomaly scores?

    abnormal_spending_patterns = ("SELECT TR.CustomerID, ROUND(A.AnomalyScore, 2), A.TransactionID, TR.TransactionID FROM anomaly_scores A, transaction_records TR  \
                                  WHERE A.TransactionID = TR.TransactionID AND AnomalyScore <> 0 ORDER BY AnomalyScore DESC;")
    cursor.execute(abnormal_spending_patterns)
    abnormal_spending_patterns = cursor.fetchall()
    formatted_abnormal_spending_patterns = [f"Customer ID: {row[0]}, Anomaly Score: {row[1]}" for row
                                            in abnormal_spending_patterns]
    print(f"Customers with abnormal spending patterns: {formatted_abnormal_spending_patterns} \n ")

    ### 💳 **Transaction Trends**

    #What are the top 10 most frequent transaction categories?

    top_transaction_categories = ("SELECT Category, COUNT(*) AS TransactionCount FROM transaction_category_labels \
                                   GROUP BY Category ORDER BY TransactionCount DESC LIMIT 10;")
    cursor.execute(top_transaction_categories)
    top_transaction_categories = cursor.fetchall()
    formatted_top_transaction_categories = [f"Category: {row[0]}, Transaction Count: {row[1]}" for row
                                            in top_transaction_categories]
    print(f"Top 10 most frequent transaction categories: {formatted_top_transaction_categories} \n ")

    #What is the average transaction amount by category?

    avh_transaction_amount_by_category = ("SELECT TCL.Category, ROUND(AVG(AD.TransactionAmount), 2) AS avg_transaction_amount \
                                          FROM transaction_category_labels TCL \
                                           JOIN amount_data AD ON TCL.TransactionID = AD.TransactionID GROUP BY TCL.Category \
                                            ORDER BY avg_transaction_amount ;")
    cursor.execute(avh_transaction_amount_by_category)
    avh_transaction_amount_by_category = cursor.fetchall()
    formatted_avh_transaction_amount_by_category = [f"Category: {row[0]}, Avg Amount: {row[1]}" for row
                                                    in avh_transaction_amount_by_category]
    print(f"Average transaction amount by category: {formatted_avh_transaction_amount_by_category} \n ")

    #How many transactions occurred per month?

    transactions_per_month = ("SELECT strftime('%Y-%m', Timestamp) AS month, COUNT(*) AS transaction_count \
                              FROM transaction_metadata \
                              GROUP BY month;")
    cursor.execute(transactions_per_month)
    transactions_per_month = cursor.fetchall()
    formatted_transactions_per_month = [f"Month: {row[0]}, Transaction Count: {row[1]}" for row
                                        in transactions_per_month]
    print(f"Transactions per month: {formatted_transactions_per_month} \n ")

    #What is the distribution of transaction amounts (min, max, average)?
    distr_transaction_amounts = ("SELECT ROUND(MIN(TransactionAmount), 2) AS min_amount, \
                                 ROUND(MAX(TransactionAmount), 2) AS max_amount, \
                                ROUND(AVG(TransactionAmount), 2) AS avg_amount \
                                 FROM amount_data;"
                                 )
    cursor.execute(distr_transaction_amounts)
    distr_transaction_amounts = cursor.fetchall()
    formatted_distr_transaction_amounts = [f"Min: {row[0]}, Max: {row[1]}, Avg: {row[2]}" for row
                                           in distr_transaction_amounts]
    print(f"Distribution of transaction amounts: {formatted_distr_transaction_amounts} \n ")

    #Which days/times see the most fraudulent transactions?

    fraud_days_times = ("SELECT strftime('%Y-%m-%d %H:%M', TM.Timestamp) AS day_time, COUNT(*) AS fraud_count \
                        FROM transaction_metadata TM, fraud_indicators FI \
                        WHERE TM.TransactionID = FI.TransactionID AND FI.FraudIndicator = 1 \
                        GROUP BY day_time ORDER BY fraud_count DESC LIMIT 5;")
    cursor.execute(fraud_days_times)
    fraud_days_times = cursor.fetchall()
    formatted_fraud_days_times = [f"Day/Time: {row[0]}, Fraud Count: {row[1]}" for row
                                  in fraud_days_times]
    print(f"Days/times with the most fraudulent transactions: {formatted_fraud_days_times} \n ")

    ### 🏪 **Merchant Activity**
    #Which merchants processed the highest total value of transactions?

    merchant_total_value_transactions = ("with merchant_complete_data as(Select MD.MerchantID, TR.Amount \
                                         FROM merchant_data MD \
                                         join transaction_metadata TM on TM.MerchantID = MD.MerchantID join transaction_records TR on TR.TransactionID = TM.TransactionID)\
                                          Select MerchantID, ROUND(SUM(Amount), 2) as total_transaction_value from merchant_complete_data \
                                         group by MerchantID order by total_transaction_value desc limit 1;")
    cursor.execute(merchant_total_value_transactions)
    merchant_total_value_transactions = cursor.fetchall()

    formatted_merchant_total_value_transactions = [f"Merchant ID: {row[0]}, Total value: {row[1]}" for row in
                                                   merchant_total_value_transactions]

    print(f"Merchant with the highest total value of transactions: {formatted_merchant_total_value_transactions}")

    #Are there merchants frequently linked to high anomaly score transactions?

    merchant_with_high_anomaly_score_trans = ("select MD.MerchantID, COUNT(*) as frequency from transaction_metadata TM \
                                               join anomaly_scores A on A.TransactionID = TM.TransactionID join merchant_data MD on MD.MerchantID = TM.MerchantID \
                                                where A.AnomalyScore >= 0.3 \
                                             group by MD.MerchantID  order by frequency desc;")
    cursor.execute(merchant_with_high_anomaly_score_trans)
    merchant_with_high_anomaly_score_trans = cursor.fetchall()

    formatted_merchant_with_high_anomaly_score_trans = [f"Merchant ID: {row[0]}, Frequency: {row[1]}" for row in
                                                        merchant_with_high_anomaly_score_trans]

    print(
        f"Merchants frequently linked to high anomaly score transactions: {formatted_merchant_with_high_anomaly_score_trans}")

    #Which merchants are most often involved in suspicious activity reports?

    merchants_linked_to_susp_act = ("select TM.MerchantID, Count(*) as susp_count from transaction_metadata TM \
                                    join transaction_records TR on TR.TransactionID = TM.TransactionID join suspicious_activity SA on SA.CustomerID = TR.CustomerID where SA.SuspiciousFlag = 1 group by TM.MerchantID order by susp_count desc ")

    cursor.execute(merchants_linked_to_susp_act)

    merchants_linked_to_susp_act = cursor.fetchall()

    formatted_merchants_linked_to_susp_act = [f"Merchant ID: {row[0]}, Number of Suspicious transactions: {row[1]}" for
                                              row in merchants_linked_to_susp_act]

    print(f"Merchants who are most involve in suspicious activity: {formatted_merchants_linked_to_susp_act}")

    #What is the total number of transactions per merchant?

    total_transactions_per_merchant = (
        "select MerchantID, COUNT(*) as total from transaction_metadata group by MerchantID order by total desc ")
    cursor.execute(total_transactions_per_merchant)
    total_transactions_per_merchant = cursor.fetchall()

    formatted_total_transactions_per_merchant = [f"Merchant ID: {row[0]}, Number of transactions: {row[1]}" for row in
                                                 total_transactions_per_merchant]

    print(f"Total number of transcations per merchant: {formatted_total_transactions_per_merchant}")

    ### 🧠 **Fraud Patterns & Indicators**
    # Which customer segments (age, location) are more susceptible to fraud?

    customer_segments = ("SELECT \
    ca.Name, mt.Location, \
    CASE \
        WHEN ca.Age BETWEEN 18 AND 24 THEN '18-24' \
        WHEN ca.Age BETWEEN 25 AND 34 THEN '25-34'\
        WHEN ca.Age BETWEEN 35 AND 44 THEN '35-44' \
        WHEN ca.Age BETWEEN 45 AND 54 THEN '45-54' \
        WHEN ca.Age BETWEEN 55 AND 64 THEN '55-64'\
        WHEN ca.Age > 64 THEN '65+'\
        ELSE 'Unknown'\
    END AS age_group,\
    COUNT(*) AS fraud_count \
FROM customer_data ca \
JOIN transaction_records tr ON tr.CustomerID = ca.CustomerID \
JOIN transaction_metadata tm ON tr.TransactionID = tm.TransactionID \
JOIN merchant_data mt ON mt.MerchantID = tm.MerchantID \
JOIN fraud_indicators fi ON fi.TransactionID = tr.TransactionID \
WHERE fi.FraudIndicator = 1 \
GROUP BY ca.CustomerID, ca.Name, age_group, mt.Location \
ORDER BY fraud_count DESC; \
 ")
    cursor.execute(customer_segments)
    customer_segments = cursor.fetchall()
    formatted_customer_segments = [f" Name: {row[0]}, Customer Segment by age: {row[2]}, Location: {row[1]}" for row in
                                   customer_segments]

    print(f"customer segments (age, location) more susceptible to fraud: {formatted_customer_segments}")

    #How often do suspicious activity flags overlap with fraud indicators?

    overlap_susp_act_and_fi = ("select count(*) from suspicious_activity sa \
                                join transaction_records tr on tr.CustomerID = sa.CustomerID \
                                join fraud_indicators fi on fi.TransactionID = tr.TransactionID \
                                where sa.SuspiciousFlag = 1 and fi.FraudIndicator = 1; ")

    cursor.execute(overlap_susp_act_and_fi)
    overlap_susp_act_and_fi = cursor.fetchone()

    print(
        f"Overlapping frequency between suspicious activity flags and fraud indicators: {overlap_susp_act_and_fi[0]} ")

    #Find customers with high transaction volume but low account activity (possible fraud).

    possible_fraud = ("with flags as (select a.CustomerID,   \
                        case when sum(tr.Amount) > 100 then 1 else 0 end as high_sum, \
                        case when count(tr.TransactionID) < 3 then 1 else 0 end as low_activity \
                        from account_activity a \
                        join transaction_records tr on tr.CustomerID = a.CustomerID \
                        group by tr.CustomerID) \
                        select * from flags \
                        where high_sum = 1 and low_activity = 1;")

    cursor.execute(possible_fraud)
    possible_fraud = cursor.fetchall()

    formatted_possible_fraud = [f"Customer ID: {row[0]}" for row in possible_fraud]

    print(f"customers with high transaction volume but low account activity: {formatted_possible_fraud}")



    conn.close()




if __name__ == "__main__":
    fraud_analysis("AntiFraudData.db")
