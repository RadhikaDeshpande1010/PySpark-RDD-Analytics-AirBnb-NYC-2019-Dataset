<div align="center">

<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Google%20Colab-Ready-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white"/>

# 🏠 PySpark RDD Analytics — AirBnb NYC 2019 Dataset

**Domain:** Short-Term Rental Analytics &nbsp;|&nbsp; **Engine:** Apache Spark (RDD API) &nbsp;|&nbsp; **Language:** Python 3

</div>

---

## 📌 Overview

This notebook applies the **Apache Spark RDD API** to the AirBnb New York City 2019 open dataset, covering **8 analytical exercises** across core Spark primitives — `map`, `filter`, `flatMap`, `reduceByKey`, `sortBy`, `max`, `min`, and `count`. The dataset contains **48,895 listings** across all five NYC boroughs, and all transformations are built using **low-level RDD operations** (no DataFrames or SQL) to build a strong foundational understanding of Spark internals.

---

## 🗂️ Repository Structure

```
pyspark-rdd-airbnb-nyc-analytics/
│
├── notebook/
│   └── AirBnb_rdd_analysis.ipynb          # Main notebook (8 RDD exercises)
├── data/
│   └── AB_NYC_2019.csv                    # Source dataset (48,895 listings)
└── README.md
```

---

## 📋 Dataset Schema

The dataset is a CSV file with a header row. Each subsequent row represents one AirBnb listing in New York City.

| Field | Index | Type | Description |
|---|---|---|---|
| `id` | 0 | int | Unique listing identifier |
| `name` | 1 | str | Listing name / title |
| `host_id` | 2 | int | Unique host identifier |
| `host_name` | 3 | str | Host name |
| `neighbourhood_group` | 4 | str | Borough: Manhattan, Brooklyn, Queens, Bronx, Staten Island |
| `neighbourhood` | 5 | str | Neighbourhood name |
| `latitude` | 6 | float | Latitude coordinate |
| `longitude` | 7 | float | Longitude coordinate |
| `room_type` | 8 | str | `Entire home/apt` / `Private room` / `Shared room` |
| `price` | 9 | int | Price per night in USD |
| `minimum_nights` | 10 | int | Minimum nights required per booking |
| `number_of_reviews` | 11 | int | Total number of reviews |
| `last_review` | 12 | str | Date of last review (`DD-MM-YYYY`) |
| `reviews_per_month` | 13 | float | Average reviews per month (used as rating proxy) |
| `calculated_host_listings_count` | 14 | int | Total active listings by this host |
| `availability_365` | 15 | int | Days per year the listing is available |

**Sample record:**
```
2539,Clean & quiet apt home by the park,2787,John,Brooklyn,Kensington,40.64749,-73.97237,Private room,149,1,9,19-10-2018,0.21,6,365
```

---

## 📊 Dataset Snapshot

| Metric | Value |
|---|---|
| Total listings | 48,895 |
| Entire home / apt | 21,601 |
| Private rooms | 19,079 |
| Shared rooms | 1,027 |
| Boroughs covered | 5 (Manhattan, Brooklyn, Queens, Bronx, Staten Island) |

---

## 📂 Exercises Overview

| Q# | Question | Column Used | Method |
|---|---|---|---|
| Q1 | Which host has the most listed rooms? | `host_id`, `host_name` | `map → reduceByKey → sortBy → first` |
| Q2 | Which host has the highest rating? | `reviews_per_month` | `map → filter → max` |
| Q3 | Which host has the minimum rating? | `reviews_per_month` | `map → filter → min` |
| Q4 | Print first 5 highest rated hosts | `reviews_per_month` | `reduceByKey(max) → sortBy desc → take(5)` |
| Q5 | Print first 5 lowest rated hosts | `reviews_per_month` | `reduceByKey(min) → sortBy asc → take(5)` |
| Q6 | How many rooms are available for 365 days? | `availability_365` | `filter(== 365) → count` |
| Q7 | How many Private rooms are available? | `room_type` | `filter(== 'Private room') → count` |
| Q8 | How many Entire home/apt are available? | `room_type` | `filter(== 'Entire home/apt') → count` |

> **Note:** `reviews_per_month` (index 13) is used as a rating proxy since the dataset does not include an explicit star-rating column.

---

## 🚀 Getting Started

### Prerequisites

- Python 3.x
- PySpark (`pip install pyspark`)
- Google Colab (recommended) or a local Spark environment

### Installation

```bash
pip install pyspark
```

### Running in Google Colab

```python
!pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("AirBnb_NYC_RDD_Analysis") \
    .getOrCreate()

sc = spark.sparkContext
```

Upload `AB_NYC_2019.csv` to `/content/sample_data/` in your Colab session, then run all cells sequentially.

---

## 💡 Key Concepts Demonstrated

**Loading and parsing a CSV with quoted fields using `csv.reader`**
```python
import csv

raw_rdd = sc.textFile("/content/sample_data/AB_NYC_2019.csv")
header  = raw_rdd.first()

data_rdd = raw_rdd \
    .filter(lambda row: row != header) \
    .map(lambda row: next(csv.reader([row])))
```

**Host with the most listings (Q1)**
```python
host_with_most_listings_rdd = data_rdd \
    .filter(lambda col: len(col) > 3) \
    .map(lambda col: ((col[2], col[3]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

top_host_by_listings = host_with_most_listings_rdd.first()
```

**Top 5 highest rated hosts (Q4)**
```python
top_5_highest_rated_hosts = data_rdd \
    .filter(lambda col: len(col) > 13) \
    .filter(lambda col: col[13] not in ('', '0')) \
    .map(lambda col: ((col[2], col[3]), safe_float(col[13]))) \
    .filter(lambda x: x[1] is not None) \
    .reduceByKey(lambda a, b: max(a, b)) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)
```

**Count listings available for 365 days (Q6)**
```python
rooms_available_365_days_count = data_rdd \
    .filter(lambda col: len(col) > 15) \
    .filter(lambda col: col[15].strip() == '365') \
    .count()
```

**Count by room type (Q7 & Q8)**
```python
private_rooms_count = data_rdd \
    .filter(lambda col: col[8].strip() == 'Private room') \
    .count()

entire_home_apt_count = data_rdd \
    .filter(lambda col: col[8].strip() == 'Entire home/apt') \
    .count()
```

---

## 🛠️ Technologies Used

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-RDD%20API-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=flat-square&logo=googlecolab&logoColor=white)

---

<div align="center">

*Built by **Radhika Deshpande** · PySpark RDD exercises on the AirBnb NYC 2019 open dataset*

</div>
