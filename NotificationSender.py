from DatabaseConnector import DatabaseConnector
import requests
import json

class LineNotificationSender:
    def __init__(self):
        self.channel_access_token = '2myumQFenLuBDlype6Eih6pXCeQrAUvxAP/5zcbmceh2aehSQznmwEGORK4XRDvkNyES/3JFMBLUUbHex3mt6JMTozhgvY6vqTrITLXcBXxT2ODTXv8yP2op3PiOw/HheuaR3SvEtiVsbyFksIthLgdB04t89/1O/w1cDnyilFU='

    def LgRumahIdaman(self, message):
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.channel_access_token}"}
        data = {"to": 'Cdb7f0d779c204eba97653c8a0364367d', "messages": [{"type": "text", "text": message}]}
        
        response = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print(f'Failed to send message: {response.status_code}\n{response.text}')

class TelegramNotificationSender:
    def __init__(self):
        self.bot_token = '7744183695:AAFE542XI3BkBNd9P6JWFjMA6FLVZ3-tl3c'  # <-- your real token
        self.chat_id = "-4788488501"  # <-- your group chat ID

    def send_message(self, message):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"Failed to send message: {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print("Message sent to Telegram group!")

class PropertyNotification:
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.telegram_notification_sender = TelegramNotificationSender()

    def notification_message(self, new_link):
        cursor = self.db_connector
        if cursor is None:
            return

        list_link = [link[0] for link in new_link]
        n_new_link = len(list_link)
        formatted_links = ', '.join(f"'{url}'" for url in list_link) if list_link else "'No Link Available!'"
        # formatted_links = "'https://properti123.com/properti-jual/42911-rumah-baru-2-lantai-modern-minimalis-pondok-ungu-permai-bekasi'"

        query = f"""
            SELECT propertyid, kondisi_bangunan, title, address, price_short, 
            price_monthly, luas_bangunan, luas_tanah, url, page_created_at
            FROM public.property_detil 
                
            WHERE url IN ({formatted_links}) AND price_real < 700000000
            AND propertytipe IN ('APARTEMEN DIJUAL', 'KONDOMINIUM DIJUAL', 
            'RUKO DIJUAL', 'RUMAH DIJUAL', 'TANAH DIJUAL', 'VILA DIJUAL')
            AND (address ILIKE '%JAKARTA%' OR address ILIKE '%JKT%' 
            OR address ILIKE '%DKI%' OR about ILIKE '%JAKARTA%' 
            OR address ILIKE '%BOGOR%' OR address ILIKE '%DEPOK%' 
            OR address ILIKE '%TANGERANG%' OR address ILIKE '%BEKASI%')
            """

        link_detil = cursor.fetch_data(query)
        result = ''
        for property_details in link_detil:
            property_id, condition, title, address, price_short, price_monthly, area_building, area_land, url, page_created_at = property_details
            result += f'''
[{property_id} - {condition.upper()}]

Judul: {title}
Lokasi: {address}

Harga: {price_short} ({price_monthly})
Luas Bgn-Tanah: {area_building} - {area_land}

Link:
{url}

{page_created_at}
==============================================================
'''
        text_message = f'===============================\n<b>TODAY WE HAVE {n_new_link} NEW PROPERTIES FOR SALE AND {len(link_detil)} MEET YOUR REQUIREMENT!</b>\n===============================\n{result}'
        # self.line_notification_sender.LgRumahIdaman(text_message)
        self.telegram_notification_sender.send_message(text_message)
        print('Message sent!')
        self.db_connector.close_connection()

# if __name__ == "__main__":
#     property_notification = PropertyNotification()
#     new_links = []
#     property_notification.notification_message(new_links)

