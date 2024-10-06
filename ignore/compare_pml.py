import pandas as pd

old_pml_df = pd.read_csv("ignore/2022-2023_pml.csv").reset_index(drop=True)
# change to lower and replace " " with "_"
old_pml_df.columns = old_pml_df.columns.str.lower().str.replace(" ", "_")

old_pml_df = old_pml_df[["code", "grade", "title", "composer", "event_name"]]


new_pml_df = pd.read_csv("pml.csv").reset_index(drop=True)
new_pml_df.columns = new_pml_df.columns.str.lower().str.replace(" ", "_")
new_pml_df = new_pml_df[["code", "grade", "title", "composer", "event_name"]]


# find row differences
print("Row differences:")
diff = old_pml_df.merge(new_pml_df, how="outer", indicator=True).loc[
    lambda x: x["_merge"] != "both"
]

print(diff)
