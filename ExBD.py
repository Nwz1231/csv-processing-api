import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def fetch_data_for_awb(awb):
    url = f"https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=BOM03691&awb=awb&numbers={awb}&format=html&lickey=81a9b858e8646c710d04b36f8e9dc177&verno=1.3f&scan=1"
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        for font in soup.find_all('font'):
            if "Waybill No" in font.text:
                waybill_number = font.find_next('b').text.strip()
                break

        rows = soup.find_all('tr', bgcolor="WHITE")
        for row in rows:
            columns = row.find_all('td')
            if len(columns) >= 4:
                details = columns[1].text.strip()
                return details

        return None
    except requests.RequestException:
        return None

def process_csv_file():
    try:
        print("Step 1: Locating the file.")
        file_name = None
        for file in os.listdir():
            if file.startswith("mydata_") and file.endswith(".csv"):
                file_name = file
                break

        if not file_name:
            print("No file starting with 'mydata_' found in the current directory.")
            return

        print("Step 2: Loading the CSV file.")
        df = pd.read_csv(file_name)

        print("Step 3: Removing rows where 'TICKET STATUS' contains 'Closed'.")
        if 'TICKET STATUS' in df.columns:
            df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]

        print("Step 4: Removing columns 'PRIORITY' and 'DEPARTMENT'.")
        for col in ['PRIORITY', 'DEPARTMENT']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        print("Step 5: Removing rows where 'AGENT NAME' matches specific names.")
        names_to_remove = {"Laxmi", "Vijay Singh", "Shefali", "Faisal Qureshi", "Harshit Wanwani"}
        if 'AGENT NAME' in df.columns:
            df = df[~df['AGENT NAME'].isin(names_to_remove)]

        print("Step 6: Removing rows where 'CATEGORY NAME' is 'OTHERS'.")
        if 'CATEGORY NAME' in df.columns:
            df = df[df['CATEGORY NAME'] != "OTHERS"]

        print("Step 7: Fetching AWB details for filtered data.")
        if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
            filtered_df = df[df['COURIER NAME'].isin(['Bluedart', 'BlueDart Surface'])].copy()
            awb_numbers = filtered_df['TRACKING ID'].dropna().unique()

            if len(awb_numbers) > 0:
                with tqdm(total=len(awb_numbers), desc="Fetching AWB details", unit_scale=True, bar_format="{desc}: {percentage:3.0f}%") as progress_bar:
                    details_results = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        for result in executor.map(fetch_data_for_awb, awb_numbers):
                            details_results.append(result)
                            progress_bar.update(1)

                print("Mapping fetched details to the DataFrame.")
                details_map = {awb: details for awb, details in zip(awb_numbers, details_results) if details}
                filtered_df['Details'] = filtered_df['TRACKING ID'].map(details_map)

                # Add the Details column to the original DataFrame
                df = df.merge(filtered_df[['TRACKING ID', 'Details']], on='TRACKING ID', how='left')

        print("Step 8: Saving the updated DataFrame.")
        df.to_csv(file_name, index=False)
        print(f"File '{file_name}' has been successfully updated.")

    except Exception as e:
        print(f"An error occurred: {e}")

process_csv_file()
