from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Create SparkSession
spark = SparkSession.builder.master("local").appName('cancer_diagnosis').getOrCreate()

# Read in the CSV file
cancer_data = spark.read.option('header', True) \
                        .option('delimiter', ',') \
                        .option("inferschema", True) \
                        .csv(r"C:\Users\Chris\PycharmProjects\Projects\DA_Projects\healthcare\cancer_diagnosis_data.csv")

# Register DataFrame as a temporary view
cancer_data.createOrReplaceTempView("cancer")

# Total number of patients in the dataset
count_all_query = """ SELECT COUNT(*) AS total_patients FROM cancer """
count_all_result = spark.sql(count_all_query).collect()[0]['total_patients']

# Number of male patients who survived
male_survived_query = """ SELECT COUNT(*) AS male_survivors FROM cancer WHERE Survival_Status == 'Survived' AND Gender == 'Male' """
male_survived_result = spark.sql(male_survived_query).collect()[0]['male_survivors']

# Number of female patients who survived
female_survived_query = """ SELECT COUNT(*) AS female_survivors FROM cancer WHERE Survival_Status == 'Survived' AND Gender == 'Female' """
female_survived_result = spark.sql(female_survived_query).collect()[0]['female_survivors']

# Number of male patients who did not survive
male_deceased_query = """ SELECT COUNT(*) AS male_deceased FROM cancer WHERE Survival_Status == 'Deceased' AND Gender == 'Male' """
male_deceased_result = spark.sql(male_deceased_query).collect()[0]['male_deceased']

# Number of female patients who did not survive
female_deceased_query = """ SELECT COUNT(*) AS female_deceased FROM cancer WHERE Survival_Status == 'Deceased' AND Gender == 'Female' """
female_deceased_result = spark.sql(female_deceased_query).collect()[0]['female_deceased']

# Number of male patients with benign tumors
male_benign_query = """ SELECT COUNT(*) AS male_benign_cases FROM cancer WHERE Tumor_Type == 'Benign' AND Gender == 'Male' """
male_benign_result = spark.sql(male_benign_query).collect()[0]['male_benign_cases']

# Number of female patients with benign tumors
female_benign_query = """ SELECT COUNT(*) AS female_benign_cases FROM cancer WHERE Tumor_Type == 'Benign' AND Gender == 'Female' """
female_benign_result = spark.sql(female_benign_query).collect()[0]['female_benign_cases']

# Number of male patients with malignant tumors
male_malignant_query = """ SELECT COUNT(*) AS male_malignant_cases FROM cancer WHERE Tumor_Type == 'Malignant' AND Gender == 'Male' """
male_malignant_result = spark.sql(male_malignant_query).collect()[0]['male_malignant_cases']

# Number of female patients with malignant tumors
female_malignant_query = """ SELECT COUNT(*) AS female_malignant_cases FROM cancer WHERE Tumor_Type == 'Malignant' AND Gender == 'Female' """
female_malignant_result = spark.sql(female_malignant_query).collect()[0]['female_malignant_cases']

# Number of male patients treated with chemotherapy
male_chemotherapy_query = """ SELECT COUNT(*) AS male_chemotherapy_cases FROM cancer WHERE Treatment == 'Chemotherapy' AND Gender == 'Male' """
male_chemotherapy_result = spark.sql(male_chemotherapy_query).collect()[0]['male_chemotherapy_cases']

# Number of female patients treated with chemotherapy
female_chemotherapy_query = """ SELECT COUNT(*) AS female_chemotherapy_cases FROM cancer WHERE Treatment == 'Chemotherapy' AND Gender == 'Female' """
female_chemotherapy_result = spark.sql(female_chemotherapy_query).collect()[0]['female_chemotherapy_cases']

# Number of male patients treated with radiation therapy
male_radiation_therapy_query = """ SELECT COUNT(*) AS male_radiation_cases FROM cancer WHERE Treatment == 'Radiation Therapy' AND Gender == 'Male' """
male_radiation_therapy_result = spark.sql(male_radiation_therapy_query).collect()[0]['male_radiation_cases']

# Number of female patients treated with radiation therapy
female_radiation_therapy_query = """ SELECT COUNT(*) AS female_radiation_cases FROM cancer WHERE Treatment == 'Radiation Therapy' AND Gender == 'Female' """
female_radiation_therapy_result = spark.sql(female_radiation_therapy_query).collect()[0]['female_radiation_cases']

# Number of male patients who underwent surgery
male_surgery_query = """ SELECT COUNT(*) AS male_surgery_cases FROM cancer WHERE Treatment == 'Surgery' AND Gender == 'Male' """
male_surgery_result = spark.sql(male_surgery_query).collect()[0]['male_surgery_cases']

# Number of female patients who underwent surgery
female_surgery_query = """ SELECT COUNT(*) AS female_surgery_cases FROM cancer WHERE Treatment == 'Surgery' AND Gender == 'Female' """
female_surgery_result = spark.sql(female_surgery_query).collect()[0]['female_surgery_cases']

# Print the results of each query
# print(f"Total Rows: {count_all_result}")
# print(f"Male Survived: {male_survived_result}")
# print(f"Female Survived: {female_survived_result}")
# print(f"Male Deceased: {male_deceased_result}")
# print(f"Female Deceased: {female_deceased_result}")
# print(f"Male Benign: {male_benign_result}")
# print(f"Female Benign: {female_benign_result}")
# print(f"Male Malignant: {male_malignant_result}")
# print(f"Female Malignant: {female_malignant_result}")
# print(f"Male Chemotherapy: {male_chemotherapy_result}")
# print(f"Female Chemotherapy: {female_chemotherapy_result}")
# print(f"Male Radiation Therapy: {male_radiation_therapy_result}")
# print(f"Female Radiation Therapy: {female_radiation_therapy_result}")
# print(f"Male Surgery: {male_surgery_result}")
# print(f"Female Surgery: {female_surgery_result}")

# Define the CTE for Age Groups and Tumor Sizes
groups = """
WITH age_groups AS (
    SELECT Patient_ID,
        CASE
            WHEN Age >= 20 AND Age <= 29 THEN '20-29'
            WHEN Age >= 30 AND Age <= 39 THEN '30-39'
            WHEN Age >= 40 AND Age <= 49 THEN '40-49'
            WHEN Age >= 50 AND Age <= 59 THEN '50-59'
            WHEN Age >= 60 AND Age <= 69 THEN '60-69'
            ELSE '70+'
        END AS Age_Groups
    FROM cancer
),
tumor_sizes_grouped AS (
    SELECT Patient_ID,
        CASE
            WHEN Tumor_Size <= 2.5 THEN '<= 2.5cm'
            WHEN Tumor_Size > 2.5 AND Tumor_Size <= 5 THEN '2.51cm - 5cm'
            WHEN Tumor_Size > 5 AND Tumor_Size <= 7.5 THEN '5.1cm - 7.5cm'
            ELSE '7.5cm or greater'
        END AS Tumor_Sizes
    FROM cancer
)
"""

# 1. Survival Chances Based on Tumor Type and Age

# a. Calculate the Survival Rate Over Time
survival_over_time = """
SELECT 
    tumor_type,
    age,
    survival_status,
    COUNT(*) OVER (PARTITION BY tumor_type, age ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_patients,
    COUNT(CASE WHEN survival_status = 'Survived' THEN 1 END) OVER (PARTITION BY tumor_type, age ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_survivors,
    ROUND(
        COUNT(CASE WHEN survival_status = 'Survived' THEN 1 END) OVER (PARTITION BY tumor_type, age ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / 
        COUNT(*) OVER (PARTITION BY tumor_type, age ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2
    ) AS cumulative_survival_rate
FROM 
    cancer
WHERE 
    age = 21
    AND tumor_type = 'Malignant'
ORDER BY 
    tumor_type, age;
"""

# b. Best Treatment for Benign Tumors by Age Group
best_treatment_for_benign = """
SELECT 
    c.tumor_type, 
    c.treatment, 
    ag.age_groups,
    ROUND(
        (COUNT(CASE WHEN c.survival_status = 'Survived' THEN 1 END) / COUNT(*)) * 100.0, 
        2
    ) AS survival_chance
FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
WHERE 
    c.tumor_type = 'Benign'
GROUP BY 
    c.tumor_type, c.treatment, ag.age_groups
ORDER BY 
    c.treatment, survival_chance DESC;
"""

# c. Best Treatment for Malignant Tumors by Age Group
best_treatment_for_malignant = """
SELECT 
    c.treatment, 
    ag.age_groups,
    ROUND(
        (COUNT(CASE WHEN c.survival_status = 'Survived' THEN 1 END) / COUNT(*)) * 100.0, 
        2
    ) AS survival_chance
FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
WHERE 
    c.tumor_type = 'Malignant'
GROUP BY 
    c.tumor_type, c.treatment, ag.age_groups
ORDER BY 
    c.treatment, survival_chance DESC;
"""

# d. Calculate the Response to Treatment Over Time
treatment_response_by_age_groups = """
SELECT 
    treatment,
    ag.age_groups,
    COUNT(*) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_patients,

    COUNT(CASE WHEN response_to_treatment = 'Complete Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_complete_responses,
    ROUND(
        COUNT(CASE WHEN response_to_treatment = 'Complete Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / 
        COUNT(*) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2
    ) AS complete_response_rate,

    COUNT(CASE WHEN response_to_treatment = 'Partial Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_partial_responses,
    ROUND(
        COUNT(CASE WHEN response_to_treatment = 'Partial Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / 
        COUNT(*) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2
    ) AS partial_response_rate,

    COUNT(CASE WHEN response_to_treatment = 'No Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_no_responses,
    ROUND(
        COUNT(CASE WHEN response_to_treatment = 'No Response' THEN 1 END) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / 
        COUNT(*) OVER (PARTITION BY treatment ORDER BY ag.age_groups ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2
    ) AS no_response_rate

FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
WHERE 
    treatment = 'Chemotherapy'
ORDER BY 
    treatment, ag.age_groups;
"""

# 2. Tumor Sizes and Detection Based on Gender and Tumor Type

# a. Average Tumor Size by Gender and Tumor Type
avg_tumor_sizes = """
SELECT 
    gender,
    tumor_type,
    ag.age_groups,
    ROUND(AVG(tumor_size), 2) AS avg_tumor_size
FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
GROUP BY 
    gender, tumor_type, ag.age_groups
ORDER BY 
    gender, tumor_type, ag.age_groups;
"""

# b. Tumor Detection Rate for Males with Tumor Size
male_tumor_detection = """
SELECT 
    ag.age_groups, 
    tg.tumor_sizes,
    ROUND(
        (COUNT(CASE WHEN c.biopsy_result = 'Positive' THEN 1 END) / COUNT(*)) * 100.0, 
        2
    ) AS detection_rate
FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
    INNER JOIN tumor_sizes_grouped tg ON c.patient_id = tg.patient_id
WHERE 
    c.gender = 'Male'
GROUP BY 
    ag.age_groups, tg.tumor_sizes
ORDER BY 
    ag.age_groups;
"""

# c. Tumor Detection Rate for Females with Tumor Size
female_tumor_detection = """
SELECT 
    ag.age_groups, 
    tg.tumor_sizes,
    ROUND(
        (COUNT(CASE WHEN c.biopsy_result = 'Positive' THEN 1 END) / COUNT(*)) * 100.0, 
        2
    ) AS detection_rate
FROM 
    cancer c
    INNER JOIN age_groups ag ON c.patient_id = ag.patient_id
    INNER JOIN tumor_sizes_grouped tg ON c.patient_id = tg.patient_id
WHERE 
    c.gender = 'Female'
GROUP BY 
    ag.age_groups, tg.tumor_sizes
ORDER BY 
    ag.age_groups;
"""

# Combine the CTE and the main queries
benign = f"{groups} {best_treatment_for_benign}"
malignant = f"{groups} {best_treatment_for_malignant}"
tumor_sizes = f"{groups} {avg_tumor_sizes}"
treatment_responses = f"{groups} {treatment_response_by_age_groups}"
male_detection = f"{groups} {male_tumor_detection}"
female_detection = f"{groups} {female_tumor_detection}"

# Execute the combined queries
survival_over_time_results = spark.sql(survival_over_time)
benign_results = spark.sql(benign)
malignant_results = spark.sql(malignant)
treatment_response_over_time_results = spark.sql(treatment_responses)
avg_tumor_sizes_results = spark.sql(tumor_sizes)
male_detection_results = spark.sql(male_detection)
female_detection_results = spark.sql(female_detection)

# Show the results
# survival_over_time_results.show(truncate=False)
# benign_results.show(truncate=False)
# malignant_results.show(truncate=False)
# treatment_response_over_time_results.show(truncate=False)
# avg_tumor_sizes_results.show(truncate=False)
# male_detection_results.show(truncate=False)
# female_detection_results.show(truncate=False)

# 3. Convert Spark DataFrames to Pandas DataFrames
df_best_treatment_benign = benign_results.toPandas()
df_best_treatment_malignant = malignant_results.toPandas()
df_male_tumor_detection = male_detection_results.toPandas()
df_female_tumor_detection = female_detection_results.toPandas()

# 4. Create different visualizations to better portray the results

# a.Visualization for Best Treatment for Benign Tumors
plt.figure(figsize=(14, 7))
sns.barplot(data=df_best_treatment_benign, x='age_groups', y='survival_chance', hue='treatment')
plt.title('Survival Chance by Treatment for Benign Tumors')
plt.xlabel('Age Groups')
plt.ylabel('Survival Chance (%)')
plt.legend(title='Treatment')
plt.xticks(rotation=45)
plt.tight_layout()
# plt.show()

# b.Visualization for Best Treatment for Malignant Tumors
plt.figure(figsize=(14, 7))
sns.barplot(data=df_best_treatment_malignant, x='age_groups', y='survival_chance', hue='treatment')
plt.title('Survival Chance by Treatment for Malignant Tumors')
plt.xlabel('Age Groups')
plt.ylabel('Survival Chance (%)')
plt.legend(title='Treatment')
plt.xticks(rotation=45)
plt.tight_layout()
# plt.show()

# c. Heatmap for Male Tumor Detection
pivot_male = df_male_tumor_detection.pivot('age_groups', 'tumor_sizes', 'detection_rate')
plt.figure(figsize=(14, 7))
sns.heatmap(pivot_male, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Tumor Detection Rate - Male')
plt.xlabel('Tumor Sizes')
plt.ylabel('Age Groups')
plt.tight_layout()
# plt.show()

# d. Heatmap for Female Tumor Detection
pivot_female = df_female_tumor_detection.pivot('age_groups', 'tumor_sizes', 'detection_rate')
plt.figure(figsize=(14, 7))
sns.heatmap(pivot_female, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Tumor Detection Rate - Female')
plt.xlabel('Tumor Sizes')
plt.ylabel('Age Groups')
plt.tight_layout()
# plt.show()

# End spark session
spark.stop()
