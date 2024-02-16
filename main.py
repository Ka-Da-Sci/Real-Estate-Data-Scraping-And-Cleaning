# Check the readme file.

from bs4 import BeautifulSoup
import requests
from datetime import datetime
from lxml import etree
from socket import gaierror
from http.client import IncompleteRead
from requests.exceptions import ConnectionError, ReadTimeout, ChunkedEncodingError
from urllib3.exceptions import MaxRetryError, NameResolutionError, ReadTimeoutError, TimeoutError, ProtocolError
from time import sleep
import pandas as pd
from json import loads, dump
from multiprocessing import Pool, cpu_count

main_listings_url = "https://www.abujaproperties.com/properties-list/page/"
land_listings_url = "https://www.abujaproperties.com/land-for-sale/"


def prep(page):
    network_connectivity_confirmed = False
    while not network_connectivity_confirmed:
        try:
            webpage = requests.get(page)
        except (ConnectionError, gaierror, MaxRetryError, NameResolutionError,
                TimeoutError, ReadTimeout, ReadTimeoutError, IncompleteRead, ProtocolError, ChunkedEncodingError) \
                as NetworkError:
            print("Poor Network! Check Your Network Connectivity!")
            sleep(3)
            continue
        else:
            xpath_soup = BeautifulSoup(webpage.content, "html.parser")
        return xpath_soup


def main(url, idx):
    page_data = []
    miscellaneous_details_dict_unique_keys = set()
    page_url = url
    dom = etree.HTML(str(prep(page=page_url)))
    page_listings_container_div = dom.xpath('//*[@id="listing_ajax_container"]')[0].findall("div")

    for listing in page_listings_container_div:
        # Scrape Property Name/Title And Page Advert Link
        property_link_obj = dom.xpath(f'//*[@id="listing_ajax_container"]/div['
                                      f'{page_listings_container_div.index(listing) + 1}]/div/h4/a')
        property_link = property_link_obj[0].attrib["href"].replace('\n', '')
        print(property_link_obj[0])

        # Scrape Location
        try:
            location_tag_obj = dom.xpath(f'//*[@id="listing_ajax_container"]/div['
                                         f'{page_listings_container_div.index(listing) + 1}'
                                         f']/div/div[1]/div[1]/div[3]')[0].findall("a")
        except IndexError:
            location_tag_obj = dom.xpath(f'//*[@id="listing_ajax_container"]/div['
                                         f'{page_listings_container_div.index(listing)}'
                                         f']/div/div[1]/div[1]/div[3]')[0].findall("a")
            location_tags = [location_tag_obj[tag_num].attrib["href"].split("/")[-2].title() for tag_num in
                             range(len(location_tag_obj))]
        else:
            location_tags = [location_tag_obj[tag_num].attrib["href"].split("/")[-2].title() for tag_num in
                             range(len(location_tag_obj))]

        listing_type = dom.xpath('//*[@id="listing_ajax_container"]/div[2]/div/div[1]/div[2]/div/div[1]')[0].text

        # Enter The Property Page
        x_dom = etree.HTML(str(prep(page=property_link)))

        # Scrape Property Title
        property_title = x_dom.xpath(f'//*[@id="all_wrapper"]/div[1]/div[3]/div/div[2]/h1')[0].text

        # Scrape Property Description
        try:
            property_desc = x_dom.xpath(f'//*[@id="all_wrapper"]/div[1]/div[3]/div/div[2]/div[3]/div[3]/p')[0].text
        except IndexError:
            property_desc = dom.xpath(f'//*[@id="listing_ajax_container"]/div['
                                      f'{page_listings_container_div.index(listing) + 1}]/div/div[3]'
                                      )[0].text.replace("\n", "")
        else:
            pass

        # Scrape Property Address Info
        property_add_dict = {'address': "Not Specified", 'city': "Not Specified", 'area': "Not Specified",
                             'state': "Not Specified", 'country': "Not Specified", 'zip': "Not Specified"}
        property_add_div = x_dom.xpath(f'//*[@id="collapseTwo"]/div')[0].findall('div')
        for div in property_add_div:
            add_obj_lst = ', '.join([item.text for item in div]).replace(',', '', 1).split(":")
            if add_obj_lst[-1].strip() == "":
                add_obj = str(x_dom.xpath(f'//*[@id="collapseTwo"]/div/div[{property_add_div.index(div) + 1}]/text()'
                                          )[0]).strip()
                add_obj_lst = ', '.join([item.text + f' {add_obj}' for item in div]).replace(',', '', 1).split(":")
            property_add_dict[add_obj_lst[0].lower()] = add_obj_lst[-1].strip()
            if add_obj_lst[0] == 'State/County':
                property_add_dict['state'] = add_obj_lst[-1].strip()

        # Scrape Property Details
        detail_dict = {"property_id": 'Not Specified', "property_size": 'Not Specified', "price": 'Not Specified'}
        miscellaneous_details = []
        property_details_div = x_dom.xpath(f'//*[@id="collapseOne"]/div')[0].findall('div')
        for detail in property_details_div:
            detail_soup = BeautifulSoup(etree.tostring(detail), 'html.parser')
            detail_text = detail_soup.text
            if detail_text.split(":")[0].strip() == "Price" or detail_text.split(":")[0].strip() == "Property Size" \
                    or detail_text.split(":")[0].strip() == "Property Id":
                detail_dict[detail_text.split(":")[0].strip().replace(" ", "_").lower()] = \
                    detail_text.split(":")[-1].strip()

            else:
                miscellaneous_details.append(detail_text)
                miscellaneous_details_dict_unique_keys.add(detail_text.split(":")[0].strip().replace(" ", "_").lower())
        if detail_dict["price"] == "Not Specified":
            detail_dict["price"] = x_dom.xpath('//*[@id="all_wrapper"]/div[1]/div[3]/div/div[2]/span[2]/span[1]'
                                               )[0].text.strip()
        if len(miscellaneous_details) == 0:
            miscellaneous_details = "None"
        if miscellaneous_details != "None":
            miscellaneous_details = {item.split(":")[0].replace(" ", "_").lower(): item.split(":")[-1].strip()
                                     for item in miscellaneous_details}

        if detail_dict["property_id"] in idx:
            property_type = "Land"
        else:
            property_type = "Housing"

        # Scrape Contact Info
        contact_info = []
        try:
            c_soup = BeautifulSoup(etree.tostring(x_dom.xpath(f'//*[@id="custom_html-2"]/div')[0]), 'html.parser')
        except IndexError as e:
            contact_info = "None"
        else:
            c_info_text = c_soup.text
            c_info_text_initial_split = c_info_text.split("Phone")
            contact_info.append("contact_address: " + c_info_text_initial_split[0].strip())
            c_info_text_2nd_split = c_info_text_initial_split[1].split("Email")
            contact_info.append("phone" + c_info_text_2nd_split[0].replace("\n", ", ").replace(",", "", 1).strip())
            contact_info.append("email" + c_info_text_2nd_split[1].replace("\n", ", ").replace(",", "", 1).strip())

        if contact_info != "None":
            contact_info = {item.split(":")[0]: item.split(":")[-1].strip() for item in contact_info}

        page_num = int(url.split('/')[-2].strip())

        # Scrape Images URL
        scraping_img_urls = True
        div_num = 0
        property_img_url = "None"
        while scraping_img_urls and div_num <= 100:
            div_num += 1
            try:
                img_urls = x_dom.xpath(f'//*[@id="carousel-listing"]/div[{div_num}]')[0].findall("div")
            except IndexError:
                # print("IndexError: ", page_num, " : ", div_num, " : ", property_link)
                continue
            else:
                try:
                    property_img_url = [img_url.find("a").find("img").attrib['src'] for img_url in img_urls]
                    check_var = property_img_url[0]
                except (AttributeError, IndexError):
                    property_img_url = "None"
                    # print("AttributeError: ", page_num, " : ", div_num, " : ", property_link)
                    continue
                else:
                    scraping_img_urls = False

        # Extract The Number Of Page Views
        num_of_page_views = int(x_dom.xpath('//*[@id="all_wrapper"]/div[1]/di'
                                            'v[3]/div/div[2]/div[3]/div[1]/div[4]/div/text()')[0])

        time_last_updated = datetime.now()

        # # Add Scraped Data To The Data Pool.
        property_info = {"property_id": detail_dict["property_id"], "title": property_title,
                         "description": property_desc, "listing_type": listing_type, "property_type": property_type,
                         "status": "Available", "number_of_page_views": num_of_page_views,
                         "price": detail_dict["price"].replace("₦", "NGN"), "property_size": detail_dict[
                "property_size"].replace("ft2", "sq. ft"), "miscellaneous_details": miscellaneous_details,
                         "address": property_add_dict["address"], "city": property_add_dict["city"],
                         "area": property_add_dict["area"], "state": property_add_dict["state"],
                         "country": property_add_dict["country"], 'zip': property_add_dict["zip"],
                         "location_tags": location_tags, "contact_info": contact_info,
                         "images_urls": property_img_url, "advert_page_link": property_link, "page": page_num,
                         "time_last_updated": time_last_updated}

        page_data.append(property_info)

    # Save Data For Save Mode Option 1
    if len(page_data) != 0:
        df = pd.DataFrame(data=page_data, columns=list(page_data[0].keys()))

        # Save In JSON Format
        data = df.to_json(orient="records", force_ascii=False)
        parsed_data = loads(data)
        path = f"./Scraped Data Files/Abuja Properties Data By Pages/Abuja Properties JSON Data By Pages/Abuja" \
               f" Properties Page {url.split('/')[-2].strip()} Data.json"
        with open(path, 'w') as json_file:
            dump(parsed_data, json_file, indent=4)

        # Save In CSV Format
        df.to_csv(f"./Scraped Data Files/Abuja Properties Data By Pages/Abuja Properties CSV Data By Pages/Abuja "
                  f"Properties Page {url.split('/')[-2].strip()} Data.csv", index=False)

        print(f"Page {url.split('/')[-2].strip()} Data Scraped Successfully.")
        return page_data, miscellaneous_details_dict_unique_keys


def result_wrapper(uri, idx, main_func):
    result = main_func(uri, idx)
    return result,


if __name__ == "__main__":
    land_property_ids = []
    page_links = []
    all_pages_data = []
    misc_details_dict_unique_keys = []
    is_scraping_page_urls = True
    listing_page_num = 0
    is_land_properties_scraping_on = True
    land_property_listing_page_num = 0
    # Scraping Listing Page Links
    print("Scraping Listing Pages Links")
    while is_scraping_page_urls:
        listing_page_num += 1
        listing_page_url = f"{main_listings_url}{listing_page_num}/"
        l_dom = etree.HTML(str(prep(page=listing_page_url)))
        try:
            trigger_text = l_dom.xpath('//*[@id="listing_ajax_container"]/h4/text()')[0].strip()
            # print(trigger_text)
        except IndexError:
            page_links.append(listing_page_url)
        else:
            listing_page_num = 0
            is_scraping_page_urls = False
    print("Done Scraping Listing Pages Links")

    print("Scraping Land Property IDS")
    # Extracting Property ID of Land Listings Used For Sorting Between Lands And Houses In The Main Data
    while is_land_properties_scraping_on:
        is_land_properties_scraping_on = False
        land_property_listing_page_num += 1
        land_pages_url = f"{land_listings_url}/page/{land_property_listing_page_num}/"
        land_dom = etree.HTML(str(prep(page=land_pages_url)))
        land_listings_container_div = land_dom.xpath('//*[@id="listing_ajax_container"]')[0].findall("div")
        for land_listing in land_listings_container_div:
            land_property_link_obj = land_dom.xpath(f'//*[@id="listing_ajax_container"]/div['
                                                    f'{land_listings_container_div.index(land_listing) + 1}]/div/h4/a')
            land_property_link = land_property_link_obj[0].attrib["href"].replace('\n', '')
            land_property_page_dom = etree.HTML(str(prep(page=land_property_link)))
            land_property_id = land_property_page_dom.xpath('//*[@id="propertyid_display"]/text()')[0].strip()
            land_property_ids.append(land_property_id)
            is_land_properties_scraping_on = True
    print("Done Scraping Land Property IDS")

    print("Commencing Main Scraping")
    # Incorporating Multiprocessing And Commencing Main Scraping
    pool = Pool(round(cpu_count() * 1.25))
    args = [(page_links[links_index], land_property_ids, main) for links_index in range(len(page_links))]
    results = pool.starmap(result_wrapper, args)
    pool.close()
    pool.join()

    for result_num in range(len(results)):
        for property_listing in results[result_num][0][0]:
            all_pages_data.append(property_listing)

    for result_num in range(len(results)):
        for dict_unique_keys in results[result_num][0][1]:
            misc_details_dict_unique_keys.append(dict_unique_keys)
    misc_details_dict_unique_keys = set(misc_details_dict_unique_keys)

    if len(all_pages_data) != 0:
        all_df = pd.DataFrame(data=all_pages_data, columns=list(all_pages_data[0].keys()))

        # Save In JSON Format
        all_data = all_df.to_json(orient="records", force_ascii=False)
        all_parsed_data = loads(all_data)
        all_path = f"./Scraped Data Files/Abuja Properties All Pages Data Combined/Abuja Properties " \
                   f"All Pages JSON Data Combined.json"
        with open(all_path, 'w') as all_json_file:
            dump(all_parsed_data, all_json_file, indent=4)

        # Save In CSV Format
        all_df.to_csv(f"./Scraped Data Files/Abuja Properties All Pages Data Combined/Abuja Properties All"
                      f" Pages CSV Data Combined.csv", index=False)

    # Save miscellaneous_details_dict_unique_keys In JSON Format
    if len(list(misc_details_dict_unique_keys)) != 0:
        misc_df = pd.Series(data=list(misc_details_dict_unique_keys))
        msc_data = misc_df.to_json(orient="records", force_ascii=False)
        msc_parsed_data = loads(msc_data)
        msc_path = f"./Scraped Data Files/miscellaneous_details_dict_unique_keys.json"
        with open(msc_path, 'w') as msc_json_file:
            dump(msc_parsed_data, msc_json_file, indent=4)
