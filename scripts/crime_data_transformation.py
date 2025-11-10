#aligning age group data to match the live data

import pandas as pd
import matplotlib.pyplot as plt
import os

#reading the source file and selecting relevant sheet
file_path = r'data\Data_Tables_Unique_Alleged_Offenders_Visualisation_Year_Ending_June_2025.xlsx'
sheet_name = 'Table 04'

df = pd.read_excel(file_path,sheet_name=sheet_name)


#removing data by sex as demographic category
df = df[df["Demographic Category"] != "Sex"]

#removing countries with no names
df = df[df["Country of Birth"] != "All other countries"]
df = df[df["Country of Birth"] != "Unspecified"]


# rename the specified age groups to a single label to match with live data
df["Demographic Group"] = df["Demographic Group"].replace(
    {"55_64": "55 years and over", "65_74": "55 years and over", "75_84": "55 years and over", "85ov": "55 years and over",
     "10-17 years":"10_24","18-19 years":"10_24", "20-24 years":"10_24",
     "25-29 years":"25_44","30-34 years":"25_44","35-39 years":"25_44",
        "40-44 years":"25_44","45-49 years":"45_54","50-54 years":"45_54"}
)

#view distinct values in Age group column 
print(df["Demographic Group"].value_counts())

#View unique values - Found some unexpected age groups
print(df["Demographic Group"].unique())

#View unique values - Found some unexpected age groups
print(df["Country of Birth"].unique())

print(df.head())


#Saving the combined dataset
output_file = "data/transformed_crime_data.csv"
df.to_csv(output_file, index=False)