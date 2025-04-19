from DatabaseConnector import DatabaseConnector
from PropertyLinkUpdater import PropertyLinkUpdater
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
from datetime import datetime
import time
import requests

class PropertyDetailsUpdater:
    def __init__(self):
        self.db_connector = DatabaseConnector()

    def get_page(self, url, retries=3, backoff_factor=0.5):
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                return BeautifulSoup(response.text, 'html.parser')
            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
                    continue
                else:
                    print(f"Failed to retrieve {url} after {retries} attempts.")
                    return None

    def update_property_details(self, links):
        for index, (url, status, data) in enumerate(links):
            soup = self.get_page(url)
            if not soup:
                self.db_connector.execute_query("UPDATE property_link SET available_data = %s WHERE url = %s", (9, url))
                continue
            
            property_detil = self.extract_property_details(soup, url)
            if property_detil is None:
                continue
                
            property_detil = tuple(property_detil.values())
            self.db_connector.execute_query('''
            INSERT INTO property_detil (
                propertyid, propertylistingid, propertytipe, title, currency, 
                price_real, price_short, price_monthly, price_psm, address, 
                contact_person, phone_number, agency, kondisi_bangunan, luas_bangunan, 
                luas_tanah, jumlah_lantai, floor_loc, certificate, interior, main_bedroom, 
                bathroom, secondary_bedroom, saluran_telepon, listrik, air_pam, air_tanah, 
                jalur_mobil, garasi, carport, direction, about, domain, url, page_created_at, 
                created_at)
                VALUES (%s, %s, %s, %s, %s, %s * 1.0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', property_detil)
            self.db_connector.execute_query("UPDATE property_link SET available_data = %s WHERE url = %s", (1, url))
            self.db_connector.execute_query('CALL public.ad_update_pagedate();')
            
            print(f'Processed {index + 1}/{len(links)}: {url}')
            if (index + 1) % 50 == 0:
                print('Cooldown for 60 seconds...')
                time.sleep(60)

    def extract_property_details(self, soup, url):
        if len(soup.find_all('div', 'category')) == 0:
            self.db_connector.execute_query("UPDATE property_link SET available_data = %s WHERE url = %s", (8, url))
            return None
            
        property_detil = {}
        try:
            script_content = soup.find_all('script')[5].contents[0]
            price_data = json.loads(script_content)
            property_info_divs = soup.find_all('div', 'property-info')
            property_detil.update({
                "ID": soup.find_all('div', 'category')[0].text.strip(),
                "ID_Listing": int(property_info_divs[0].select_one('div.type:-soup-contains("ID Listing") + div.value').get_text(strip=True)),
                "Type": property_info_divs[0].select_one('div.type:-soup-contains("Tipe") + div.value').get_text(strip=True),
                "Property_Name": price_data['name'],
                "Currency": price_data['offers']['priceCurrency'],
                "Real_Price": float(price_data['offers']['price']),
                "Short_Price": soup.find_all('div', 'price')[0].contents[1].text.strip(),
                "Monthly_Price": soup.find_all('div', 'price')[0].contents[3].text.strip(),
                "Price_per_sq": property_info_divs[0].select_one('div.type:-soup-contains("PSM") + div.value').get_text(strip=True),
                "Address": soup.find_all('div', 'address')[0].text.strip(),
                "Contact_Person": soup.find_all('div', 'name')[0].text.strip(),
                "Phone_Number": soup.find_all('div', 'owner')[0].contents[5].text.strip(),
                "Agency": soup.find_all('div', 'name')[1].text.strip() if len(soup.find_all('div', 'name')) > 1 else "Not Found!"
            })

            additional_details = ["Kondisi Bangunan", "Luas Bangunan", "Luas Tanah", "Jumlah Lantai", "Floor Location", "Sertifikat",
                                  "Interior", "Kamar Tidur", "Kamar Mandi", "Kamar Pembantu", "Saluran Telepon", "Listrik", "Air Pam",
                                  "Air Tanah", "Jalur Mobil", "Garasi", "Carport", "Menghadap"]
            for detail in additional_details:
                try:
                    property_detil[detail.replace(" ", "_")] = property_info_divs[0].select_one(f'div.type:-soup-contains("{detail}") + div.value').get_text(strip=True)
                except:
                    property_detil[detail.replace(" ", "_")] = "Not Found!"

            property_detil.update({
                "About": soup.find_all('div', 'about no-border')[0].contents[3].text.strip(),
                "Domain": urlparse(url).netloc,
                "URL": url,
                "Registered_On": property_info_divs[0].select_one('div.type:-soup-contains("Terdaftar pada") + div.value').get_text(strip=True),
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception as e:
            self.db_connector.execute_query("UPDATE property_link SET available_data = %s WHERE url = %s", (7, url))
            return None
        return property_detil


# if __name__ == "__main__":
#     updater = PropertyDetailsUpdater()
#     link_updater = PropertyLinkUpdater()
#     links = link_updater.fetch_new_links(10)
#     updater.update_property_details(links)