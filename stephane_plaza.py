import requests
from bs4 import BeautifulSoup
import pandas as pd
from twilio.rest import Client
import re
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

"""
PARAMETERS
"""
job_adress = "Booking.com, Oosterdokskade, Amsterdam, Pays-Bas"

TWILLIO_PHONE_NUMBER = "+12052094890"
MY_PHONE_NUMBER = "+32489694032"

PARARIUS_BASE_URL = "https://www.pararius.com"

"""
Constants
"""
load_dotenv()

FUNDA_URL = os.getenv("FUNDA_URL")
FUNDA_PAGE_PARAM = os.getenv("FUNDA_PAGE_PARAM")

PARARIUS_URL = os.getenv("PARARIUS_URL")
PARARIUS_PAGE_PARAM = os.getenv("PARARIUS_PAGE_PARAM")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_DIRECTIONS_API_ENDPOINT = os.getenv("GOOGLE_DIRECTIONS_API_ENDPOINT")

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

TWILLIO_ACCOUNT_SID = os.getenv("TWILLIO_ACCOUNT_SID")
TWILLIO_AUTH_TOKEN = os.getenv("TWILLIO_AUTH_TOKEN")
twillio_client = Client(TWILLIO_ACCOUNT_SID, TWILLIO_AUTH_TOKEN)

headers = {
    "Accept-Language": "nl-NL,nl;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

"""
Google Maps
"""
def generate_google_maps_link(address):
    base_url = "https://www.google.com/maps/search/?api=1&query="
    return base_url + address.replace(" ", "+")

def get_travel_duration(start_address, end_address=job_adress):
    params = {
        "origin": start_address,
        "destination": end_address,
        "mode": "transit",
        "key": GOOGLE_API_KEY
    }

    response = requests.get(GOOGLE_DIRECTIONS_API_ENDPOINT, params=params)
    data = response.json()

    if data.get("routes"):
        duration = data["routes"][0]["legs"][0]["duration"]["text"]
        return duration
    else:
        return None

"""
Excel sheet local
"""
def load_existing_apartments(filepath):
    if not os.path.exists(filepath):
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            pd.DataFrame().to_excel(writer, sheet_name="Funda")
            pd.DataFrame().to_excel(writer, sheet_name="Pararius")

    df_funda = pd.read_excel(filepath, sheet_name="Funda", engine='openpyxl')
    data_funda = df_funda.to_dict('records')
    
    df_pararius = pd.read_excel(filepath, sheet_name="Pararius", engine='openpyxl')
    data_pararius = df_pararius.to_dict('records')
    
    return data_funda, data_pararius

"""
Google sheet on google drive
"""
def authenticate_with_gspread(json_keyfile):
    """
    Authenticate with Google Sheets API using a service account key.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    client = gspread.authorize(credentials)
    
    return client

def load_existing_apartments_from_gsheet(client, spreadsheet_name, sheet_name):
    """
    Load existing apartment listings from a Google Sheet.
    """
    spreadsheet = client.open(spreadsheet_name)
    worksheet = spreadsheet.worksheet(sheet_name)
    
    # Get all records as a list of dictionaries
    records = worksheet.get_all_records()
    
    return records

def save_apartments_to_gsheet(client, spreadsheet_name, sheet_name, records):
    """
    Save apartment listings to a Google Sheet.
    """
    spreadsheet = client.open(spreadsheet_name)
    worksheet = spreadsheet.worksheet(sheet_name)
    
    # Clear existing content
    worksheet.clear()
    
    # Set the headers and the records
    headers = ["Street Name", "Min to Booking", "Price", "Size (m²)", "Rooms", "Agent", "Detail Link", "Google Maps Link"]
    all_records = [headers] + records
    worksheet.append_rows(all_records)

"""
Scraping
"""
def remove_first_word(s):
    words = s.split()
    return ' '.join(words[1:])

def extract_from_pararius_card(tag):   
    # Street
    street_tag = tag.select_one(".listing-search-item__title a")
    street_name = remove_first_word(street_tag.get_text(strip=True)) if street_tag else None
    
    # Postal code & city
    postal_code_tags = tag.select("[class^='listing-search-item__sub-title']")
    postal_code_city = None
    for st_tag in postal_code_tags:
        if st_tag.attrs.get("class", [])[0] == "listing-search-item__sub-title'":
            postal_code_city = st_tag.get_text(strip=True)
            break
    
    # Price
    price_tag = tag.select_one(".listing-search-item__price")
    price = price_tag.get_text(strip=True) if price_tag else None
    
    # Features
    features_tag = tag.select(".illustrated-features__item")
    features = [feature.get_text(strip=True) for feature in features_tag] if features_tag else []
    size = features[0] if len(features) > 0 else None
    rooms = features[1] if len(features) > 1 else None
    
    # Agent Information
    agent_tag = tag.select_one(".listing-search-item__info a")
    agent = agent_tag.get_text(strip=True) if agent_tag else None
    
    # Link
    link = PARARIUS_BASE_URL + street_tag["href"] if street_tag and "href" in street_tag.attrs else None

    return street_name, postal_code_city, price, size, rooms, agent, link

def extract_from_funda_card(tag):
    # Street
    street_name = tag.text.strip()

    # Postal code & city
    postal_div = tag.find_next('div', {'data-test-id': 'postal-code-city'})
    postal_code_city = postal_div.text.strip() if postal_div else None

    # Price
    price_tag = tag.find_next('p', {'data-test-id': 'price-rent'})
    price = price_tag.text.strip() if price_tag else None

    # Size
    size_tag = tag.find_next('li')
    size = size_tag.text.strip() if size_tag else None
    
    # Number of rooms
    if size_tag:
        room_tag = size_tag.find_next_sibling('li')
        rooms = room_tag.text.strip() if room_tag else None
    else:
        rooms = None

    # Agent Information
    agent_tag = tag.find_next("a", href=re.compile("makelaars"))
    agent = agent_tag.get_text(strip=True) if agent_tag else None

    # Link
    link_tag = tag.find_parent("a")
    link = link_tag['href'] if link_tag else None

    return street_name, postal_code_city, price, size, rooms, agent, link

def scrape_page(url, existing_apartments, website):
    existing_addresses = [apartment['Street Name'] for apartment in existing_apartments]
    new_apartment_details = []

    if website == "funda":
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        tags = soup.find_all(['div', 'h2'])
        for tag in tags:
            if 'data-test-id' in tag.attrs and tag['data-test-id'] == 'street-name-house-number':
                street_name, postal_code_city, price, size, rooms, agent, link = extract_from_funda_card(tag)
                address = f"{street_name}, {postal_code_city}, Amsterdam"
                # duration_to_booking = get_travel_duration(address)
                duration_to_booking = 10
                google_maps_link = generate_google_maps_link(address)
                
                if street_name in existing_addresses:
                    return new_apartment_details

                infos = [street_name, duration_to_booking, price, size, rooms, agent, link, google_maps_link]
                new_apartment_details.append(infos)

                message = "Funda: " + link
                print(message)
                # send_message(message)

    elif website == "pararius":
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")
        cards = soup.select(".listing-search-item")
        for card in cards:
            street_name, postal_code_city, price, size, rooms, agent, link = extract_from_pararius_card(card)
            address = f"{street_name}, {postal_code_city}, Amsterdam"
            duration_to_booking = get_travel_duration(address)
            google_maps_link = generate_google_maps_link(address)

            if street_name in existing_addresses:
                return new_apartment_details

            infos = [street_name, duration_to_booking, price, size, rooms, agent, link, google_maps_link]
            new_apartment_details.append(infos)

            message = "Pararius: " + link
            print(message)
            # send_message(message)

    return new_apartment_details

def scrape_and_save(existing_apartments, website):
    if website == "funda":
        current_url = FUNDA_URL
    elif website == "pararius":
        current_url = PARARIUS_URL
    
    apartments_on_page = scrape_page(current_url, existing_apartments, website)
    columns = ["Street Name", "Min to Booking", "Price", "Size (m²)", "Rooms", "Agent", "Detail Link", "Google Maps Link"]
    df_new_apartments = pd.DataFrame(apartments_on_page, columns=columns)

    if existing_apartments:
        df_existing = pd.DataFrame(existing_apartments)
        df_combined = pd.concat([df_new_apartments, df_existing], axis=0, ignore_index=True)
    else:
        df_combined = df_new_apartments

    return df_combined

def scrape_all_pages(existing_apartments, website):
    if website == "funda":
        URL = FUNDA_URL
        PAGE_PARAM = FUNDA_PAGE_PARAM
    elif website == "pararius":
        URL = PARARIUS_URL
        PAGE_PARAM = PARARIUS_PAGE_PARAM

    new_apartments = []
    current_page = 1
    continue_scraping = True

    while continue_scraping:
        current_url = URL + PAGE_PARAM + str(current_page)
        apartments_on_page, continue_scraping = scrape_page(current_url, existing_apartments, website)

        if not apartments_on_page:
            break

        new_apartments.extend(apartments_on_page)
        current_page += 1

    columns = ["Street Name", "Min to Booking", "Price", "Size (m²)", "Rooms", "Agent", "Detail Link", "Google Maps Link"]
    df_new_apartments = pd.DataFrame(new_apartments, columns=columns)

    if existing_apartments:
        df_existing = pd.DataFrame(existing_apartments)
        df_combined = pd.concat([df_new_apartments, df_existing], axis=0, ignore_index=True)
    else:
        df_combined = df_new_apartments

    return df_combined

def send_message(message):
    message = twillio_client.messages.create(
        from_=TWILLIO_PHONE_NUMBER,
        body=message,
        to=MY_PHONE_NUMBER
    )

"""
Main
"""
def run_program():
    # Authenticate with Google Sheets API
    client = authenticate_with_gspread(GOOGLE_CREDENTIALS)
    
    spreadsheet_name = GOOGLE_SHEET_NAME

    # Load existing apartments from Google Sheets
    existing_funda = load_existing_apartments_from_gsheet(client, spreadsheet_name, "Funda")
    existing_pararius = load_existing_apartments_from_gsheet(client, spreadsheet_name, "Pararius")
    
    df_funda = scrape_and_save(existing_funda, "funda")
    df_pararius = scrape_and_save(existing_pararius, "pararius")

    # Convert DataFrames to lists of records for saving to Google Sheets
    funda_records = df_funda.values.tolist()
    pararius_records = df_pararius.values.tolist()

    # Save to Google Sheets
    save_apartments_to_gsheet(client, spreadsheet_name, "Funda", funda_records)
    save_apartments_to_gsheet(client, spreadsheet_name, "Pararius", pararius_records)

if __name__ == "__main__":
    run_program()