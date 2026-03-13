# 📊 Retail Rocket E-Commerce- Customer Behavior Analysis

## 📝 Overview
This project is an end-to-end data pipeline and interactive dashboard designed to audit the complete user journey of an e-commerce platform. By analyzing over **2.7 million raw interaction events**, this tool shifts the focus from simply reporting traffic to actively identifying where users drop off, enabling data-driven UI/UX and feature prioritization.

## 🛠️ Tech Stack
* **Database & Data Engineering:** MySQL
* **Data Processing & Scripting:** Python
* **Visualization & Analytics:** Power BI, DAX

## ⚙️ The Pipeline
1. **Data Ingestion & Cleaning (Python/MySQL):** Imported 2.7M+ raw user logs (`views`, `add-to-carts`, `transactions`) and product metadata into MySQL.
2. **Data Modeling (MySQL):** Reconstructed category hierarchies and mapped item properties to build a highly-indexed, normalized `master_events` table, optimizing query performance for heavy aggregations.
3. **Dynamic Dashboarding (Power BI & DAX):** Connected Power BI directly to the MySQL database. Built custom DAX measures to calculate on-the-fly metrics like View-to-Cart %, Cart-to-Buy %, Weekly Active Users (WAU), and Monthly Active Users (MAU).

## 💡 Key Product Insights
* **The "Window Shopping" Bottleneck:** Funnel analysis revealed a steep drop-off between product views and cart additions, indicating high top-of-funnel traffic but low immediate intent or potential friction on product detail pages.
* **Pinpointing "Leaky Buckets":** Using a conditional heatmap, I isolated specific high-traffic departments that suffer from severe checkout abandonment. These categories represent the highest-ROI opportunities for A/B testing and flow redesign.
* **Traffic vs. Intent:** Trend analysis showed massive spikes in summer traffic that did not correlate with purchase volume, highlighting the need to evaluate marketing channel quality rather than just raw visitor counts.

## 📈 Interactive Dashboards

### Page 1: Conversion & Friction Analysis
*(Identifies exact bottlenecks in the user journey and department-level drop-offs)*
![Conversion Dashboard]("C:\Users\ayura\Pictures\Screenshots\Screenshot 2026-03-13 121150.png")

### Page 2: Platform Growth & Retention
*(Tracks macro trends, seasonality, and Weekly/Monthly Active Users)*
![Trends Dashboard]("C:\Users\ayura\Pictures\Screenshots\Screenshot 2026-03-13 121038.png")

## 📁 Repository Contents
* `sql_queries.sql`: Complete MySQL script for table creation, ETL processes, and indexing.
* `ecommerce_dashboard.pbix`: The interactive Power BI file.
* `data_dictionary.md`: Definitions of raw tables and metrics.
