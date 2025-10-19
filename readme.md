This project showcases an end-to-end real-time data pipeline that predicts and visualizes insurance risk using a modern data lake/warehouse architecture. It integrates Kafka, Spark, PostgreSQL, Amazon Redshift, and Tableau to simulate a production-grade workflow from raw data ingestion to business intelligence dashboards.

Incoming insurance records are streamed from Kafka, cleaned and transformed in Spark, and scored by a trained machine learning model to predict risk levels. Curated, feature-ready data is stored in PostgreSQL for ML retraining, while aggregated analytics are pushed to Redshift for visualization in Tableau, where key insights such as quarterly charge growth, regional risk distribution, and smoker-based trends are explored interactively.

Diagram displaying the pipeline flowchart.
<img width="1155" height="686" alt="Screenshot 2025-10-19 144701" src="https://github.com/user-attachments/assets/c746a2e2-2508-4b8a-862b-dddf8f7d5f71" />


Dashboard made from the curated data. 
<img width="1562" height="887" alt="dashboard" src="https://github.com/user-attachments/assets/a549ed44-5ae8-4cff-9954-a202554b6b65" />




