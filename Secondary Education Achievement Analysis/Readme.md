
# Secondary Education Achievement Analysis

This project focuses on analyzing student achievement in secondary education at two Portuguese schools. The dataset includes student grades, demographic information, social and school-related features, and was collected via school reports and questionnaires. The analysis aims to uncover insights into the factors influencing student performance in Mathematics and Portuguese language.

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Dataset Description](#2-dataset-description)
- [3. Project Objectives](#3-project-objectives)
- [4. Data Exploration](#4-data-exploration)
- [5. Feature Selection](#5-feature-selection)
- [6. Conclusion](#6-conclusion)

## 1. Introduction

The data used in this project reflects student achievement in two distinct subjects: Mathematics and Portuguese language. By analyzing various demographic, social, and educational factors, we aim to determine which features most significantly impact student grades.

## 2. Dataset Description

- **Datasets Used:**
  - `student-mat.csv` (Math performance data)
  - `student-por.csv` (Portuguese performance data)

- **Attributes:**
  - The dataset includes features such as student grades, demographic information (age, gender, etc.), social factors (parental education, family size, etc.), and school-related features (study time, school support, etc.).

## 3. Project Objectives

The project covers the following steps:

1. **Data Exploration Using Statistics:**
   - Perform statistical analysis to gain initial insights.

2. **Load the Dataset:**
   - Import the datasets into a DataFrame for analysis.

3. **Explore Datatypes:**
   - Investigate the datatypes of each column to understand the structure of the data.

4. **Explore Ranges and Distinct Values:**
   - Identify the range of values for numeric columns and distinct values for categorical columns.

5. **Explore Data Distribution:**
   - Visualize the distribution of values in each column to understand the data's spread and skewness.

6. **Correlation Analysis:**
   - Explore the relationships between different features using a correlation matrix to identify strong correlations.

7. **Feature Ranking:**
   - Rank all features based on their impact on the overall grade to determine which factors are most influential.

## 4. Data Exploration

### 4.1 Statistical Analysis
- Basic statistical analysis was performed to understand the central tendencies and dispersion of the data.

### 4.2 Datatype Exploration
- The datatypes of each column were examined to identify which columns contain numerical or categorical data.

### 4.3 Range and Distinct Values
- The range for numeric columns was calculated to understand the spread of data.
- Distinct values were identified for categorical columns to see the diversity of responses.

### 4.4 Data Distribution
- Histograms and boxen plots were used to visualize the distribution of data, highlighting skewness in columns such as `G1`, `G2`, `G3`, and others.

### 4.5 Correlation Analysis
- A correlation matrix was generated to explore the relationships between features, with a focus on how these features correlate with student grades.

## 5. Feature Selection

- Features were ranked based on their correlation with the overall grade. 
- Features with strong correlations were identified as critical in determining student performance.
- Columns with low impact were considered for exclusion to streamline the model.

## 6. Conclusion

This analysis provides insights into the factors that most influence student performance in secondary education. The final dataset, after feature selection, is ready for further modeling to predict student grades based on the selected features.

---

**Thank you for exploring this project.**
## 🔗 Links

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/prathamesh095/)

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/prathamesh095)


