from DatabaseConnector import DatabaseConnector
from bs4 import BeautifulSoup
import time
import requests

class PropertyLinkUpdater:
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

    def fetch_new_links(self, page_length):
        new_links = []
        for page_number in range(page_length):
            url = f'https://properti123.com/properti-jual?post_date=DESC&listing_type=SALE&page={page_number+1}'
            soup = self.get_page(url)
            if not soup:
                continue
            links = [link.get('href') for link in soup.find_all('a') if link.get('href') and 'https://properti123.com/properti-jual/' in link.get('href')]
            formatted_links = tuple(links)
            existing_links = self.db_connector.fetch_data("SELECT url FROM public.property_link WHERE status = 'JUAL' AND url IN %s", (formatted_links,))
            if len(existing_links) >= len(links):
                print('All links are already available.')
                break
            new_links.extend([(link, 'JUAL', 0) for link in links if (link,) not in existing_links])
            self.db_connector.executemany_query("INSERT INTO public.property_link (url, status, available_data) VALUES (%s, %s, %s) ON CONFLICT (url) DO NOTHING;", new_links)
            print(f'Committed {len(links)-len(existing_links)}/{len(links)} new link!')
        return new_links

# if __name__ == "__main__":
#     updater = PropertyLinkUpdater()
#     updater.fetch_new_links(5)

