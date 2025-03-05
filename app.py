from flask import Flask, render_template, request, jsonify, Response, send_file
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import os
import io
import logging

app = Flask(__name__)

# ✅ Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# ✅ Fetch Bluedart Tracking Data
def fetch_data_for_awb(awb):
    url = f"https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=BOM03691&awb=awb&numbers={awb}&format=html&lickey=81a9b858e8646c710d04b36f8e9dc177&verno=1.3f&scan=1"
    try:
        logging.info(f"Fetching Bluedart data for AWB: {awb}")
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
                logging.info(f"AWB {awb}: {details}, {date} {time}")
                return details, f"{date} {time}"
        return None, None
    except requests.RequestException as e:
        logging.error(f"Request error for Bluedart AWB {awb}: {e}")
        return None, None

# ✅ Fetch Delhivery Tracking Data
def get_last_scan_details(awb):
    url = f'https://selloship.com/vendor/test/track_order/{awb}'
    try:
        logging.info(f"Fetching Delhivery data for AWB: {awb}")
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
        last_scan_index = content.rfind('[ScanDetail]')

        if last_scan_index != -1:
            last_scan_section = content[last_scan_index:]
            instructions = last_scan_section.split("[Instructions] => ")[1].split("\n")[0].strip()
            status_datetime = last_scan_section.split("[StatusDateTime] => ")[1].split("\n")[0].strip()
            return instructions, status_datetime
        return None, None
    except requests.RequestException as e:
        logging.error(f"Request error for Delhivery AWB {awb}: {e}")
        return None, None

# ✅ Process CSV Data
def process_csv_data(file_stream):
    logging.info("Reading CSV file.")
    df = pd.read_csv(file_stream, dtype=str)  # Read all as strings to prevent errors
    logging.info(f"Initial file shape: {df.shape}")

    if 'TICKET STATUS' in df.columns:
        df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]
        logging.info(f"Filtered closed tickets. New shape: {df.shape}")

    for col in ['PRIORITY', 'DEPARTMENT']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if 'CATEGORY NAME' in df.columns:
        df = df[~df['CATEGORY NAME'].isin(["OTHERS", "DISPUTE"])]

    # Fetch tracking details
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        awb_numbers = df['TRACKING ID'].dropna().unique()

        with ThreadPoolExecutor(max_workers=10) as executor:
            bluedart_results = dict(zip(awb_numbers, executor.map(fetch_data_for_awb, awb_numbers)))
            delhivery_results = dict(zip(awb_numbers, executor.map(get_last_scan_details, awb_numbers)))

        df['Details'] = df['TRACKING ID'].map(lambda x: bluedart_results.get(x, (None, None))[0])
        df['Details Date'] = df['TRACKING ID'].map(lambda x: bluedart_results.get(x, (None, None))[1])

    logging.info(f"Final processed file shape: {df.shape}")
    return df

# ✅ Home Page (HTML Upload Form)
@app.route('/')
def index():
    return render_template('index.html')

# ✅ Process Uploaded CSV
@app.route('/process-csv', methods=['POST'])
def process_csv():
    logging.info("Received request at '/process-csv' endpoint.")

    if 'file' not in request.files:
        return jsonify({"error": "No file provided."}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected."}), 400

    try:
        logging.info(f"Processing file: {file.filename}")
        processed_df = process_csv_data(file)
        csv_output = io.StringIO()
        processed_df.to_csv(csv_output, index=False)
        csv_output.seek(0)

        return Response(
            csv_output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=processed_{file.filename}"}
        )
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logging.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port)
