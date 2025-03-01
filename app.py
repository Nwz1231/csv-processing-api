from flask import Flask, request, jsonify, Response
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import os
import logging
import asyncio
import aiohttp

app = Flask(__name__)

# Setup logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# ----------------- Asynchronous Functions -----------------

async def async_fetch_data_for_awb(awb, session):
    url = f"https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=BOM03691&awb=awb&numbers={awb}&format=html&lickey=81a9b858e8646c710d04b36f8e9dc177&verno=1.3f&scan=1"
    try:
        async with session.get(url) as response:
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
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
                    logging.info(f"Fetched BD details for AWB {awb}: {details}, {date} {time}")
                    return details, f"{date} {time}"
            logging.info(f"No BD details found for AWB {awb}")
            return None, None
    except Exception as e:
        logging.error(f"Error fetching BD for AWB {awb}: {e}")
        return None, None

async def async_get_last_scan_details(awb, session):
    url = f'https://selloship.com/vendor/test/track_order/{awb}'
    try:
        async with session.get(url) as response:
            text = await response.text()
            last_scan_index = text.rfind('[ScanDetail]')
            if last_scan_index != -1:
                last_scan_section = text[last_scan_index:]
                instructions = last_scan_section.split("[Instructions] => ")[1].split("\n")[0].strip()
                status_datetime = last_scan_section.split("[StatusDateTime] => ")[1].split("\n")[0].strip()
                logging.info(f"Fetched Delhivery details for AWB {awb}: {instructions}, {status_datetime}")
                return instructions, status_datetime
            else:
                logging.info(f"No Delhivery details found for AWB {awb}")
                return None, None
    except Exception as e:
        logging.error(f"Error fetching Delhivery for AWB {awb}: {e}")
        return None, None

async def async_fetch_all_bd(awb_numbers):
    async with aiohttp.ClientSession() as session:
        tasks = [async_fetch_data_for_awb(awb, session) for awb in awb_numbers]
        results = await asyncio.gather(*tasks)
        return results

async def async_fetch_all_del(awb_numbers):
    async with aiohttp.ClientSession() as session:
        tasks = [async_get_last_scan_details(awb, session) for awb in awb_numbers]
        results = await asyncio.gather(*tasks)
        return results

# ----------------- Synchronous CSV Processing -----------------

def process_csv_data(file_stream):
    logging.info("Reading CSV file.")
    df = pd.read_csv(file_stream)
    logging.info(f"CSV file loaded with shape {df.shape}.")

    # Step 3: Remove rows where 'TICKET STATUS' contains 'Closed'.
    if 'TICKET STATUS' in df.columns:
        df = df[~df['TICKET STATUS'].str.contains("Closed", na=False)]
        logging.info("Removed rows with 'Closed' ticket status.")

    # Step 4: Remove columns 'PRIORITY' and 'DEPARTMENT'.
    for col in ['PRIORITY', 'DEPARTMENT']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            logging.info(f"Dropped column: {col}")

    # Step 5: Remove rows where 'CATEGORY NAME' is 'OTHERS' or 'DISPUTE'.
    if 'CATEGORY NAME' in df.columns:
        df = df[~df['CATEGORY NAME'].isin(["OTHERS", "DISPUTE"])]
        logging.info("Removed rows where CATEGORY NAME is 'OTHERS' or 'DISPUTE'.")

    # Step 6: Fetching details for Bluedart Data.
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        bd_df = df[df['COURIER NAME'].isin(['Bluedart', 'BlueDart Surface'])].copy()
        awb_numbers = bd_df['TRACKING ID'].dropna().unique()
        if len(awb_numbers) > 0:
            logging.info(f"Fetching BlueDart details for {len(awb_numbers)} AWB numbers asynchronously.")
            results_bd = asyncio.run(async_fetch_all_bd(awb_numbers))
            details_results = [res[0] for res in results_bd]
            details_dates = [res[1] for res in results_bd]
            details_map = {awb: details for awb, details in zip(awb_numbers, details_results) if details}
            details_date_map = {awb: dt for awb, dt in zip(awb_numbers, details_dates) if dt}
            df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_map))
            df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(details_date_map))
            logging.info("Bluedart details updated in dataframe.")

    # Step 7: Fetching details for Delhivery Data.
    if 'TRACKING ID' in df.columns and 'COURIER NAME' in df.columns:
        del_df = df[df['COURIER NAME'].isin(['Delhivery Express', 'Delhivery FR', 'Delhivery FR Surface 10kg'])].copy()
        del_awb_numbers = del_df['TRACKING ID'].dropna().unique()
        if len(del_awb_numbers) > 0:
            logging.info(f"Fetching Delhivery details for {len(del_awb_numbers)} AWB numbers asynchronously.")
            results_del = asyncio.run(async_fetch_all_del(del_awb_numbers))
            del_details_results = [res[0] for res in results_del]
            del_details_dates = [res[1] for res in results_del]
            del_details_map = {awb: details for awb, details in zip(del_awb_numbers, del_details_results) if details}
            del_details_date_map = {awb: dt for awb, dt in zip(del_awb_numbers, del_details_dates) if dt}
            df['Details'] = df.get('Details', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(del_details_map))
            df['Details Date'] = df.get('Details Date', pd.Series(index=df.index)).combine_first(df['TRACKING ID'].map(del_details_date_map))
            logging.info("Delhivery details updated in dataframe.")

    logging.info("CSV processing complete.")
    return df

# ----------------- Flask Endpoints -----------------

@app.route('/')
def index():
    logging.info("Received request at '/' endpoint.")
    return "CSV Processing API is running."

@app.route('/process-csv', methods=['POST'])
def process_csv():
    logging.info("Received request at '/process-csv' endpoint.")
    if 'file' not in request.files:
        logging.error("No file provided in the request.")
        return jsonify({"error": "No file provided."}), 400
    file = request.files['file']
    if file.filename == '':
        logging.error("No file selected.")
        return jsonify({"error": "No file selected."}), 400
    try:
        logging.info(f"Processing file: {file.filename}")
        processed_df = process_csv_data(file)
        csv_output = processed_df.to_csv(index=False)
        logging.info("CSV file processed successfully.")
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
