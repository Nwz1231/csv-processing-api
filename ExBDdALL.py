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
                location = columns[0].text.strip()
                details = columns[1].text.strip()
                date = columns[2].text.strip()
                time = columns[3].text.strip()
                return details, f"{date} {time}"

        return None, None
    except requests.RequestException:
        return None, None

def get_last_scan_details(awb):
    url = f'https://selloship.com/vendor/test/track_order/{awb}'
    try:
        response = requests.get(url)
        response.raise_for_status()

        content = response.text
        last_scan_index = content.rfind('[ScanDetail]')

        if last_scan_index != -1:
            last_scan_section = content[last_scan_index:]
            instructions = last_scan_section.split("[Instructions] => ")[1].split("\n")[0].strip()
            status_datetime = last_scan_section.split("[StatusDateTime] => ")[1].split("\n")[0].strip()
            return instructions, status_datetime
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Request failed for AWB {awb}: {e}")
        return None, None

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

        print("Step 5: Removing rows where 'CATEGORY NAME' is 'OTHERS' or 'DISPUTE'.")
        if 'CATEGORY NAME' in df.columns:
            df = df[~df['CATEGORY NAME'].isin(["OTHERS", "DISPUTE"])]

        print("Step 6: Fetching details for Bluedart Data.")
        if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
            filtered_df = df[df['COURIER NAME'].isin(['Bluedart', 'BlueDart Surface'])].copy()
            awb_numbers = filtered_df['TRACKING ID'].dropna().unique()

            if len(awb_numbers) > 0:
                with tqdm(total=len(awb_numbers), desc="Fetching Bluedart details", unit_scale=True, bar_format="{desc}: {percentage:3.0f}%") as progress_bar:
                    details_results = []
                    details_dates = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        for details, date_time in executor.map(fetch_data_for_awb, awb_numbers):
                            details_results.append(details)
                            details_dates.append(date_time)
                            progress_bar.update(1)

                print("Mapping fetched Bluedart details to the DataFrame.")
                details_map = {awb: details for awb, details in zip(awb_numbers, details_results) if details}
                details_date_map = {awb: date_time for awb, date_time in zip(awb_numbers, details_dates) if date_time}
                df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_map))
                df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_date_map))

        print("Step 7: Fetching details for Delhivery Data.")
        if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
            delhivery_filtered_df = df[df['COURIER NAME'].isin(['Delhivery Express', 'Delhivery FR', 'Delhivery FR Surface 10kg'])].copy()
            delhivery_awb_numbers = delhivery_filtered_df['TRACKING ID'].dropna().unique()

            if len(delhivery_awb_numbers) > 0:
                with tqdm(total=len(delhivery_awb_numbers), desc="Fetching Delhivery details", unit_scale=True, bar_format="{desc}: {percentage:3.0f}%") as progress_bar:
                    delhivery_details_results = []
                    delhivery_dates = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        for instructions, status_datetime in executor.map(get_last_scan_details, delhivery_awb_numbers):
                            delhivery_details_results.append(instructions)
                            delhivery_dates.append(status_datetime)
                            progress_bar.update(1)

                print("Mapping fetched Delhivery details to the DataFrame.")
                delhivery_details_map = {awb: details for awb, details in zip(delhivery_awb_numbers, delhivery_details_results) if details}
                delhivery_date_map = {awb: status_datetime for awb, status_datetime in zip(delhivery_awb_numbers, delhivery_dates) if status_datetime}
                df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(delhivery_details_map))
                df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(delhivery_date_map))

        print("Step 8: Saving the updated DataFrame.")
        df.to_csv(file_name, index=False)
        print(f"File '{file_name}' has been successfully updated.")

    except Exception as e:
        print(f"An error occurred: {e}")

process_csv_file()
