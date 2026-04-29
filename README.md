# 🛒 PySpark E-commerce Analysis

## 📌 Overview

This project demonstrates an end-to-end data analysis pipeline using **PySpark** on an e-commerce dataset.

The pipeline covers data ingestion, cleaning, transformation, and advanced analytics using window functions.

---

## 🔧 Features

* Data ingestion from CSV files
* Data cleaning (handling missing values)
* Data transformation & joins
* Aggregations (total spending per user)
* Window functions (ranking users & products)
* Customer segmentation (VIP vs Normal)

---

## 📊 Key Insights

* 🥇 Top users per city based on total spending
* 📦 Most popular products per city
* 👑 VIP customers distribution across cities

---

## 🛠️ Tech Stack

* Python
* PySpark
* Pandas (for exporting results)

---

## 📂 Project Structure

```
pyspark-ecommerce-analysis/
│
├── data/          # Input datasets (users, orders)
├── output/        # Generated results
├── src/
│   └── main.py    # Main pipeline
├── requirements.txt
└── README.md
```

---

## ▶️ How to Run

```bash
python src/main.py
```

---

## 📈 Sample Output

The project generates:

* Top users per city
* Top products per city
* VIP customers count

Results are saved as CSV files in the `output/` folder.

---

## 💡 Notes

* Spark write is replaced with Pandas export for compatibility with Windows environments.
* The pipeline is designed to simulate real-world data engineering workflows.

---

## 🚀 Future Improvements

* Add Parquet support (production-level format)
* Integrate with a database (PostgreSQL / BigQuery)
* Build a dashboard (Power BI / Streamlit)
