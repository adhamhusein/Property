import psycopg2
from thefuzz import fuzz, process
import time
from DatabaseConnector import DatabaseConnector

class PropertyAddressProcessor:
    def __init__(self):
        self.connect_to_postgres()

    def connect_to_postgres(self):
        self.db_connector = DatabaseConnector()

    def fetch_property_address(self):
        query = """SELECT t1.propertyid, t1.address FROM public.property_detil t1
        LEFT JOIN property_address t2 ON t1.propertyid = t2.propertyid WHERE t2.score IS NULL;"""
        return self.db_connector.fetch_data(query)
    
    def fetch_province_list(self):
        query = 'SELECT DISTINCT provinsi FROM public.ref_address;'
        return [prov[0] for prov in self.db_connector.fetch_data(query)]

    def fetch_kabupatenkota_list(self, province):
        query = f"SELECT distinct kabupatenkota FROM public.ref_address WHERE provinsi = '{province}';"
        return [add[0] for add in self.db_connector.fetch_data(query)]

    def fetch_kecamatan_list(self, province, kabupatenkota):
        query = f"""SELECT distinct kecamatan FROM public.ref_address 
                                    WHERE provinsi = '{province}' and kabupatenkota = '{kabupatenkota}';"""
        return [add[0] for add in self.db_connector.fetch_data(query)]

    def match_province(self, address, province_list):
        province_matching, province_matching_score = process.extractOne(address, province_list, scorer=fuzz.WRatio)
        if province_matching_score < 90:
            province_matching, province_matching_score = process.extractOne(address, province_list, scorer=fuzz.token_set_ratio)
        return province_matching, province_matching_score

    def match_kabupatenkota(self, address, kabupatenkota_list):
        kabupatenkota_matching, kabupatenkota_matching_score = process.extractOne(address, kabupatenkota_list, scorer=fuzz.WRatio)
        if kabupatenkota_matching_score < 90:
            kabupatenkota_matching, kabupatenkota_matching_score = process.extractOne(address, kabupatenkota_list, scorer=fuzz.token_set_ratio)
        return kabupatenkota_matching, kabupatenkota_matching_score

    def match_kecamatan(self, address, kecamatan_list):
        kecamatan_matching, kecamatan_matching_score = process.extractOne(address, kecamatan_list, scorer=fuzz.WRatio)
        if kecamatan_matching_score < 90:
            kecamatan_matching, kecamatan_matching_score = process.extractOne(address, kecamatan_list, scorer=fuzz.token_set_ratio)
        return kecamatan_matching, kecamatan_matching_score

    def calculate_score(self, province_score, kabupatenkota_score, kecamatan_score):
        return int((province_score + kabupatenkota_score + kecamatan_score) / 3)

    def insert_address(self, propertyid, address_nonstandard, province, kabupatenkota, kecamatan, score):
        query = f"""INSERT INTO public.property_address(
            propertyid, address_nonstandard, province, kabupatenkota, kecamatan, score)
            VALUES ('{propertyid}', '{address_nonstandard}', '{province}', '{kabupatenkota}', '{kecamatan}', 
            {score}) ON CONFLICT (propertyid) DO NOTHING;"""
        self.db_connector.execute_query(query)

    def process_address(self, property_address):
        province_list = self.fetch_province_list()
        total_addresses = len(property_address)
        for index, item in enumerate(property_address):
            propertyid, address_nonstandard = item
            address_nonstandard = address_nonstandard.replace("'", "")
            province_matching, province_matching_score = self.match_province(address_nonstandard, province_list)
            kabupatenkota_list = self.fetch_kabupatenkota_list(province_matching)
            kabupatenkota_matching, kabupatenkota_matching_score = self.match_kabupatenkota(address_nonstandard, kabupatenkota_list)
            kecamatan_list = self.fetch_kecamatan_list(province_matching, kabupatenkota_matching)
            kecamatan_matching, kecamatan_matching_score = self.match_kecamatan(address_nonstandard, kecamatan_list)
            score = self.calculate_score(province_matching_score, kabupatenkota_matching_score, kecamatan_matching_score)
            self.insert_address(propertyid, address_nonstandard, province_matching, kabupatenkota_matching, kecamatan_matching, score)
            percentage = (index + 1) / total_addresses * 100
            print(f"Processed {index + 1}/{total_addresses} addresses. ({percentage:.2f}%)")

    def run(self):
        while True:
            try:
                property_address = self.fetch_property_address()
                if not property_address:
                    print('All Address Processed!')
                    break
                self.process_address(property_address)
            except psycopg2.Error as e:
                print(f"Lost connection or error: {str(e)}")
                time.sleep(60)
                self.connect_to_postgres() 
                self.run()

# if __name__ == "__main__":
#     processor = PropertyAddressProcessor()
#     processor.run()