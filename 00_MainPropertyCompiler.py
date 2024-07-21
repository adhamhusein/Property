from PropertyLinkUpdater import PropertyLinkUpdater
from PropertyDetilUpdater import PropertyDetailsUpdater
from AddressMatching import PropertyAddressProcessor
from LineNotificationSender import PropertyNotification

def main():
    link_updater = PropertyLinkUpdater()
    details_updater = PropertyDetailsUpdater()
    notification_sender = PropertyNotification()
    address_processor = PropertyAddressProcessor()

    new_links = link_updater.fetch_new_links(5)
    details_updater.update_property_details(new_links)
    notification_sender.notification_message(new_links)
    address_processor.run()

if __name__ == "__main__":
    main()
