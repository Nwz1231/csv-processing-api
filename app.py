from flask import Flask, request, jsonify, Response
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import os
import logging

app = Flask(__name__)

# ✅ Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def fetch_data_for_awb(awb):
    url = f"https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=BOM03691&awb=awb&numbers={awb}&format=html&lickey=81a9b858e8646c710d04b36f8e9dc177&verno=1.3f&scan=1"
    try:
        logging.info(f"Fetching Blueddart data for AWB: {awb}")
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
                date = columns[2].text.strip()
                time = columns[3].text.strip()
                logging.info(f"Fetched details for AWB {awb}: {details}, {date} {time}")
                return details, f"{date} {time}"
        logging.warning(f"No details found for AWB {awb}")
        return None, None
    except requests.RequestException as e:
        logging.error(f"Request error for Blueddart AWB {awb}: {e}")
        return None, None

def get_last_scan_details(awb):
    url = f'https://selloship.com/vendor/test/track_order/{awb}'
    try:
        logging.info(f"Fetching Delhivery scan details for AWB: {awb}")
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
        last_scan_index = content.rfind('[ScanDetail]')
        if last_scan_index != -1:
            last_scan_section = content[last_scan_index:]
            instructions = last_scan_section.split("[Instructions] => ")[1].split("\n")[0].strip()
            status_datetime = last_scan_section.split("[StatusDateTime] => ")[1].split("\n")[0].strip()
            logging.info(f"Fetched last scan for AWB {awb}: {instructions}, {status_datetime}")
            return instructions, status_datetime
        logging.warning(f"No scan details found for Delhivery AWB {awb}")
        return None, None
    except requests.RequestException as e:
        logging.error(f"Request failed for Delhivery AWB {awb}: {e}")
        return None, None

def process_csv_data(file_stream):
    logging.info("Reading CSV file.")
    df = pd.read_csv(file_stream, dtype=str)  # ✅ Read everything as string to prevent scientific notation
    logging.info(f"Initial file shape: {df.shape}")

    # Step 3: Remove rows where 'TICKET STATUS' contains 'Closed'.
    if 'TICKET STATUS' in df.columns:
        df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]
        logging.info(f"After removing 'Closed' tickets, new shape: {df.shape}")

    # Step 4: Remove unnecessary columns
    for col in ['PRIORITY', 'DEPARTMENT']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            logging.info(f"Removed column: {col}")

    # Step 5: Remove rows where 'CATEGORY NAME' is 'OTHERS' or 'DISPUTE'.
    if 'CATEGORY NAME' in df.columns:
        df = df[~df['CATEGORY NAME'].isin(["OTHERS", "DISPUTE"])]
        logging.info(f"After filtering CATEGORY NAME, new shape: {df.shape}")

    # Step 6: Fetch Bluedart Data
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        bluedart_df = df[df['COURIER NAME'].isin(['Bluedart', 'BlueDart Surface'])]
        awb_numbers = bluedart_df['TRACKING ID'].dropna().unique()
        logging.info(f"Fetching Bluedart details for {len(awb_numbers)} AWB numbers.")

        if len(awb_numbers) > 0:
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_data_for_awb, awb_numbers))
            details_map = {awb: details for awb, details in zip(awb_numbers, results) if details}
            df['Details'] = df['TRACKING ID'].map(details_map)

    # Step 7: Fetch Delhivery Data
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        delhivery_df = df[df['COURIER NAME'].isin(['Delhivery Express', 'Delhivery FR', 'Delhivery FR Surface 10kg'])]
        delhivery_awb_numbers = delhivery_df['TRACKING ID'].dropna().unique()
        logging.info(f"Fetching Delhivery details for {len(delhivery_awb_numbers)} AWB numbers.")

        if len(delhivery_awb_numbers) > 0:
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(get_last_scan_details, delhivery_awb_numbers))
            scan_map = {awb: scan for awb, scan in zip(delhivery_awb_numbers, results) if scan}
            df['Details'] = df['TRACKING ID'].map(scan_map)

    logging.info(f"Final processed file shape: {df.shape}")
    return df

@app.route('/')
def index():
    return "CSV Processing API is running."

@app.route('/process-csv', methods=['POST'])
def process_csv():
    logging.info("Received request at '/process-csv' endpoint.")
    if 'file' not in request.files:
        logging.error("No file provided.")
        return jsonify({"error": "No file provided."}), 400
    
    file = request.files['file']
    if file.filename == '':
        logging.error("No file selected.")
        return jsonify({"error": "No file selected."}), 400
    
    try:
        logging.info(f"Processing file: {file.filename}")
        processed_df = process_csv_data(file)
        csv_output = processed_df.to_csv(index=False)

        return Response(csv_output,
                        mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=processed_{file.filename}"})
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logging.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port)
