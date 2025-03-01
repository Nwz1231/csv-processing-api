from flask import Flask, request, jsonify, Response
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import os

app = Flask(__name__)

def fetch_data_for_awb(awb):
    # Note: Check that the URL formatting is correct (e.g. awb parameter substitution)
    url = f"https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=BOM03691&awb=awb&numbers={awb}&format=html&lickey=81a9b858e8646c710d04b36f8e9dc177&verno=1.3f&scan=1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Loop through font tags to check for "Waybill No" text and then extract details.
        for font in soup.find_all('font'):
            if "Waybill No" in font.text:
                # Example extraction (adjust as needed)
                waybill_number = font.find_next('b').text.strip()
                break

        rows = soup.find_all('tr', bgcolor="WHITE")
        for row in rows:
            columns = row.find_all('td')
            if len(columns) >= 4:
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
    except requests.RequestException as e:
        print(f"Request failed for AWB {awb}: {e}")
        return None, None

def process_csv_data(file_stream):
    df = pd.read_csv(file_stream)

    # Step 3: Remove rows where 'TICKET STATUS' contains 'Closed'.
    if 'TICKET STATUS' in df.columns:
        df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]

    # Step 4: Remove columns 'PRIORITY' and 'DEPARTMENT'.
    for col in ['PRIORITY', 'DEPARTMENT']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Step 5: Remove rows where 'CATEGORY NAME' is 'OTHERS' or 'DISPUTE'.
    if 'CATEGORY NAME' in df.columns:
        df = df[~df['CATEGORY NAME'].isin(["OTHERS", "DISPUTE"])]

    # Step 6: Fetching details for Bluedart Data.
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        filtered_df = df[df['COURIER NAME'].isin(['Bluedart', 'BlueDart Surface'])].copy()
        awb_numbers = filtered_df['TRACKING ID'].dropna().unique()
        if len(awb_numbers) > 0:
            details_results = []
            details_dates = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_data_for_awb, awb_numbers))
            for details, date_time in results:
                details_results.append(details)
                details_dates.append(date_time)
            details_map = {awb: details for awb, details in zip(awb_numbers, details_results) if details}
            details_date_map = {awb: date_time for awb, date_time in zip(awb_numbers, details_dates) if date_time}
            df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_map))
            df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_date_map))

    # Step 7: Fetching details for Delhivery Data.
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        delhivery_filtered_df = df[df['COURIER NAME'].isin(['Delhivery Express', 'Delhivery FR', 'Delhivery FR Surface 10kg'])].copy()
        delhivery_awb_numbers = delhivery_filtered_df['TRACKING ID'].dropna().unique()
        if len(delhivery_awb_numbers) > 0:
            delhivery_details_results = []
            delhivery_dates = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(get_last_scan_details, delhivery_awb_numbers))
            for instructions, status_datetime in results:
                delhivery_details_results.append(instructions)
                delhivery_dates.append(status_datetime)
            delhivery_details_map = {awb: details for awb, details in zip(delhivery_awb_numbers, delhivery_details_results) if details}
            delhivery_date_map = {awb: status_datetime for awb, status_datetime in zip(delhivery_awb_numbers, delhivery_dates) if status_datetime}
            df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(delhivery_details_map))
            df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(delhivery_date_map))

    return df

@app.route('/')
def index():
    return "CSV Processing API is running."

@app.route('/process-csv', methods=['POST'])
def process_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided."}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected."}), 400
    try:
        processed_df = process_csv_data(file)
        csv_output = processed_df.to_csv(index=False)
        return Response(csv_output,
                        mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=processed_{file.filename}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
