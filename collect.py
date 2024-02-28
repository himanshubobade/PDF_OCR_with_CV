import os
import re
import mysql.connector
import json
import pdfplumber
import pytesseract
from PIL import Image
import threading
import multiprocessing
import numpy as np

# Connect to MySQL database
with open('../data/config.json') as config_file:
    config = json.load(config_file)

db = mysql.connector.connect(host=config['host'], user=config['user'], password=config['password'], database=config['database'])
cursor = db.cursor()

# Check if the wells table exists
cursor.execute("SHOW TABLES LIKE 'wells'")
table_exists = cursor.fetchone()

# If the table does not exist, create it
if not table_exists:
    create_table_sql = """
    CREATE TABLE wells (
        id INT AUTO_INCREMENT PRIMARY KEY,
        api VARCHAR(255),
        longitude VARCHAR(255),
        latitude VARCHAR(255),
        well_name VARCHAR(255),
        well_number VARCHAR(255),
        county VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        zip_code VARCHAR(20),
        date_stimulated DATE,
        stimulated_formation VARCHAR(255),
        top_ft INT,
        bottom_ft INT,
        stimulation_stages INT,
        volume INT,
        volume_units VARCHAR(255),
        type_treatment VARCHAR(255),
        acid_percent FLOAT,
        lbs_proppant INT,
        maximum_treatment_pressure INT,
        maximum_treatment_rate FLOAT,
        details VARCHAR(1000)
    );
    """
    cursor.execute(create_table_sql)
    print("Table 'wells' created successfully.")

'''
def is_page_ocr(page):
    return not page.extract_text()

def extract_text_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    return text
'''

def extract_text_from_pdf_threaded(pdf_path):
    # Create a process pool with 4 processes
    pool = multiprocessing.Pool(20)

    # Create a list of tasks to perform OCR on each page in the PDF file
    tasks = []
    with pdfplumber.open(pdf_path) as pdf:
        num_pages = len(pdf.pages)
        batch_size = min(30, num_pages)  # Adjust the batch size as needed

        for i in range(0, num_pages, batch_size):
            batch = range(i, min(i + batch_size, num_pages))
            tasks.append(pool.apply_async(extract_text_from_pages, (pdf_path, batch)))

    # Wait for all of the tasks to finish
    pool.close()
    pool.join()

    # Combine the extracted text from each batch
    text = ""
    for task in tasks:
        text += task.get()

    return text

def is_page_empty(page):
    return not page.extract_text()

def extract_text_from_pages(pdf_path, page_indices):
    with pdfplumber.open(pdf_path) as pdf:
        text = ""
        for i in page_indices:
            page = pdf.pages[i]
            
            # Check if the page is empty (has no text)
            if is_page_empty(page):
                # Perform OCR on empty pages
                img = page.to_image()
                text += pytesseract.image_to_string(img.original)
            else:
                # Extract text directly from pages with text
                text += page.extract_text()

    return text

def extract_information(text):
    print("Extracting information from text...")

    # Make the text lowercase to perform case-insensitive matching
    lower_text = text.lower()

    # Define variations for each field
    field_variations = {
        'api': ['api', 'api number', 'api#'],
        'longitude': ['longitude'],
        'latitude': ['latitude'],
        'well_name': ['well name'],
        'well_number': ['well number'],
        'county': ['county'],
        'city': ['city'],
        'state': ['state'],
        'zip_code': ['zip code', 'zip'],
        'date_stimulated': ['date stimulated'],
        'stimulated_formation': ['stimulated formation'],
        'top_ft': ['top ft', 'top (ft)'],
        'bottom_ft': ['bottom ft', 'bottom (ft)'],
        'stimulation_stages': ['stimulation stages'],
        'volume': ['volume'],
        'volume_units': ['volume units'],
        'type_treatment': ['type treatment'],
        'acid_percent': ['acid percent', 'acid%', 'acid %'],
        'lbs_proppant': ['lbs proppant'],
        'maximum_treatment_pressure': ['maximum treatment pressure (psi)', 'maximum treatment pressure'],
        'maximum_treatment_rate': ['maximum treatment rate (bbls/min)', 'maximum treatment rate'],
        'details': ['details']
    }

    # Initialize extracted_info with fields from the wells table
    extracted_info = {field: None for field in field_variations.keys()}

    # Iterate through variations for each field
    for field, variations in field_variations.items():
        for variation in variations:
            # Update the regular expression to capture values in the specified formats
            if field == 'api':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE)
            elif field == 'longitude':
                pattern = re.compile(fr'{variation}[:\s]+(\d+° \d+\' \d+\.\d+ [WE])', re.IGNORECASE | re.DOTALL)
            elif field == 'latitude':
                pattern = re.compile(fr'{variation}[:\s]+(\d+° \d+\' \d+\.\d+ [WE])', re.IGNORECASE | re.DOTALL)
            elif field == 'well_name':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'well_number':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'county':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'city':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'state':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'zip_code':
                pattern = re.compile(fr'{variation}[:\s]+(\d+)', re.IGNORECASE | re.DOTALL)
            elif field == 'date_stimulated':
                pattern = re.compile(fr'{variation}[:\s]+(\d{2}/\d{2}/\d{4})', re.IGNORECASE | re.DOTALL)
            elif field == 'stimulated_formation':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field in ['top_ft', 'bottom_ft', 'stimulation_stages', 'volume', 'lbs_proppant', 'maximum_treatment_pressure']:
                pattern = re.compile(fr'{variation}[:\s]+(\d+)', re.IGNORECASE | re.DOTALL)
            elif field in ['maximum_treatment_rate', 'acid_percent']:
                pattern = re.compile(fr'{variation}[:\s]+([\d.]+)', re.IGNORECASE | re.DOTALL)
            elif field in ['volume_units', 'type_treatment']:
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
            elif field == 'details':
                pattern = re.compile(fr'{variation}[:\s]+(.+?)\n', re.IGNORECASE | re.DOTALL)
                # You may need a more complex pattern for details field based on your actual data

            match = pattern.search(lower_text)
            if match:
                # Strip leading and trailing whitespaces
                extracted_info[field] = match.group(1).strip()
                print(f"{field}: {extracted_info[field]}")
                break  # Break the loop if a match is found

    return extracted_info

# Iterate over downloaded PDFs
folder_path = "/home/kanchi/Desktop/lab5/data/pdfs"
for filename in os.listdir(folder_path):
    if filename.endswith(".pdf"):
        pdf_path = os.path.join(folder_path, filename)
        print(f"\nProcessing PDF: {pdf_path}")

        # Extract text from the PDF
        pdf_text = extract_text_from_pdf_threaded(pdf_path)

        # Extract information from text
        extracted_info = extract_information(pdf_text)

        # Store information in the database
        # Modify your SQL query to include placeholders for all fields, with default values
        sql = """
            INSERT INTO wells (
                api, longitude, latitude, well_name, well_number, county, city, state, zip_code,
                date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages,
                volume, volume_units, type_treatment, acid_percent, lbs_proppant,
                maximum_treatment_pressure, maximum_treatment_rate, details
            ) 
            VALUES (
                %(api)s, %(longitude)s, %(latitude)s, %(well_name)s, %(well_number)s, 
                %(county)s, %(city)s, %(state)s, %(zip_code)s,
                %(date_stimulated)s, %(stimulated_formation)s, %(top_ft)s, %(bottom_ft)s, 
                %(stimulation_stages)s, %(volume)s, %(volume_units)s, %(type_treatment)s, 
                %(acid_percent)s, %(lbs_proppant)s, %(maximum_treatment_pressure)s, 
                %(maximum_treatment_rate)s, %(details)s
            )
        """

        # Create a dictionary with default values for missing fields
        default_values = {
            'api': None,
            'longitude': None,
            'latitude': None,
            'well_name': None,
            'well_number': None,
            'county': None,
            'city': None,
            'state': None,
            'zip_code': None,
            'date_stimulated': None,
            'stimulated_formation': None,
            'top_ft': None,
            'bottom_ft': None,
            'stimulation_stages': None,
            'volume': None,
            'volume_units': None,
            'type_treatment': None,
            'acid_percent': None,
            'lbs_proppant': None,
            'maximum_treatment_pressure': None,
            'maximum_treatment_rate': None,
            'details': None
        }

        # Update the default values with the actual values from extracted_info
        default_values.update(extracted_info)

        # Execute the SQL query with the updated values
        cursor.execute(sql, default_values)
        db.commit()

# Close database connection
cursor.close()
db.close()

