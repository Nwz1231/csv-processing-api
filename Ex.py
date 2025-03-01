import pandas as pd
import os

def process_csv_file():
    try:
        # Find the file starting with 'mydata_'
        file_name = None
        for file in os.listdir():
            if file.startswith("mydata_") and file.endswith(".csv"):
                file_name = file
                break

        if not file_name:
            print("No file starting with 'mydata_' found in the current directory.")
            return

        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_name)

        # Step 1: Remove rows where "TICKET STATUS" contains "Closed"
        if 'TICKET STATUS' in df.columns:
            df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]

        # Step 2: Remove entire columns "PRIORITY" and "DEPARTMENT"
        for col in ['PRIORITY', 'DEPARTMENT']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        # Step 3: Remove rows where "AGENT NAME" matches specified names
        names_to_remove = {"Laxmi", "Vijay Singh", "Shefali", "Faisal Qureshi", "Harshit Wanwani"}
        if 'AGENT NAME' in df.columns:
            df = df[~df['AGENT NAME'].isin(names_to_remove)]

        # Step 4: Remove rows where "CATEGORY NAME" is "OTHERS"
        if 'CATEGORY NAME' in df.columns:
            df = df[df['CATEGORY NAME'] != "OTHERS"]

        # Save the updated DataFrame back to the same CSV file
        df.to_csv(file_name, index=False)
        print(f"File '{file_name}' has been successfully updated.")

    except Exception as e:
        print(f"An error occurred: {e}")

process_csv_file()
