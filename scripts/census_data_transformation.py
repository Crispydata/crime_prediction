#This script transforms census data into required format.

import pandas as pd
import re
import glob
import os


# Folder containing your source CSV files
input_folder = ("data/census_data_by_SAL")

# Get all CSV files in that folder
file_list = glob.glob(os.path.join(input_folder, "*.csv"))

#Creating empty list to store transformed DataFrames
all_data = []

#Function to extract values from column names
def parse_variable(var):
    """
    Example: M_Afghanistan_0_4 -> M, Afghanistan, 0_4
    """
    parts = var.split("_")
    if len(parts) >= 3:
        gender = parts[0]
        country = parts[1]
        age_group = "_".join(parts[2:])
        return gender, country, age_group
    else:
        return None, None, None

#Looping through each file and transform
# Column names have format like M_Afghanistan_0_4, F_India_25_34 etc which contains the gender, country and age group.
# These columns have been transposed into rows after extracting relevant information.
for file_path in file_list:
    print(f"Processing file: {file_path}")
    
    df = pd.read_csv(file_path)
    
    sal_col = df.columns[0]
    df_melted = df.melt(id_vars=[sal_col], var_name="Variable", value_name="Value")

    df_melted[["Gender", "Country", "Age_Group"]] = df_melted["Variable"].apply(
        lambda x: pd.Series(parse_variable(x))
    )

    df_final = df_melted[[sal_col, "Gender", "Country", "Age_Group", "Value"]].copy()
    df_final.rename(columns={sal_col: "SAL_CODE_2021"}, inplace=True)
    df_final.dropna(subset=["Gender", "Country", "Age_Group"], inplace=True)

    # Append to master list
    all_data.append(df_final)

#Combining all transformed files into one DataFrame (Total rows: 4684140)
combined_df = pd.concat(all_data, ignore_index=True) 

#removing rows with zero or negative values (Total rows: 434282)
combined_df = combined_df[combined_df["Value"] > 0]

#removing rows with aggregated values
combined_df = combined_df[(combined_df["Gender"] != "P")]
combined_df = combined_df[(combined_df["Age_Group"] != "Tot")]


#Removing first 3 letters to help with joining another table to get suburb names
print(combined_df["SAL_CODE_2021"].str[:3].unique())
combined_df["SAL_CODE_2021"] = combined_df["SAL_CODE_2021"].str[3:]
combined_df["SAL_CODE_2021"] = combined_df["SAL_CODE_2021"].astype(int)

#removal of unexpected age groups
age_mask = combined_df["Age_Group"].str.lstrip().str.match(r"^\d").fillna(False)
combined_df = combined_df[age_mask]

#removal of additional countries
combined_df = combined_df[combined_df["Country"] != "Elsewhere"]
combined_df = combined_df[combined_df["Country"] != "Tot"]

# rename England and Ireland to UK & Ireland to match training data
combined_df["Country"] = combined_df["Country"].replace(
    {"England": "UK & Ireland", "Ireland": "UK & Ireland"}
)

#Only keeping countries as per Training data
combined_df = combined_df[combined_df["Country"].isin(['Australia', 'India', 'New Zealand', 'South Sudan & Sudan', 'UK & Ireland',
 'Vietnam', 'Afghanistan', 'China', 'Iran', 'Sri Lanka'])]

#Aligning age groups with training data set.
#removal of 0_4 age group   
combined_df = combined_df[combined_df["Age_Group"] != "0_4"]

#removal of age group 5_14 since unable to break the data into 10 to 14 age group
combined_df = combined_df[combined_df["Age_Group"] != "5_14"]

# rename the specified age groups to a single label
combined_df["Age_Group"] = combined_df["Age_Group"].replace(
    {"55_64": "55 years and over", "65_74": "55 years and over", "75_84": "55 years and over", "85ov": "55 years and over",
      "15_24":"10_24" } 
)





#Getting suburb and locality names from a different file
SAL_Name_df = pd.read_excel("data/SAL_2021_AUST.xlsx")
SAL_Name_df = SAL_Name_df[SAL_Name_df["SAL_CODE_2021"] != "ZZZZZ"]
#Only capturing Victoria suburbs
SAL_Name_df = SAL_Name_df[SAL_Name_df["STATE_NAME_2021"] == "Victoria"]
#SAL_Name_df["SAL_CODE_2021"] = SAL_Name_df["SAL_CODE_2021"].astype(int)
SAL_Name_df = SAL_Name_df[["SAL_CODE_2021", "SAL_NAME_2021"]]
#removing duplicates
SAL_Name_df = SAL_Name_df.drop_duplicates()


#converting keys to strings for normalization
combined_df["SAL_CODE_2021_str"] = combined_df["SAL_CODE_2021"].astype(str).str.strip()
SAL_Name_df["SAL_CODE_2021_str"] = SAL_Name_df["SAL_CODE_2021"].astype(str).str.strip()

# Perform merge on normalized string key
combined_df = pd.merge(
    combined_df,
    SAL_Name_df[["SAL_CODE_2021_str", "SAL_NAME_2021"]],
    how="left",
    left_on="SAL_CODE_2021_str",
    right_on="SAL_CODE_2021_str"
)

#Verifying data...
#view distinct values in gender column 
print(combined_df["Gender"].value_counts())

#view distinct values in Age group column 
print(combined_df["Age_Group"].value_counts())

#View unique values - Found some unexpected age groups
print(combined_df["Age_Group"].unique())

#View unique values - Found some unexpected age groups
print(combined_df["Country"].unique())

print(combined_df.info())

#dropping temporary columns
combined_df = combined_df.drop(columns=['SAL_CODE_2021','SAL_CODE_2021_str'])

#Saving the combined dataset
output_file = "data/transformed_SAL_combined.csv"
combined_df.to_csv(output_file, index=False)

print(f"\n Transformation complete! Combined file saved as: {output_file}")
print(f"Total rows: {len(combined_df)}")
print(combined_df.head())
print(combined_df.dtypes)


