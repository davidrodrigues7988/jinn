from bs4 import BeautifulSoup
import pandas as pd
import os 
import time
import urllib
import requests
import re
import random
from urllib.parse import urlparse,urlunparse
import json
import pprint
from typing import List
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from fake_useragent import UserAgent
from datetime import datetime

"""
Globals
"""
DATE = datetime.now().strftime("%Y%m%d")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Config file settings 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~
with open('CONFIG', 'r') as f:
    CONFIG = json.load(f)

user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"]        
# Crawlera proxy settings 
# Create fake headers
def random_headers():
    # return {'User-Agent': UserAgent().random}
    return {'User-Agent': random.choice(user_agents)}


# Setup the proxy authorization
proxy_host = "proxy.crawlera.com"
proxy_port = "8010"
proxy_auth = f"{CONFIG.get('YOUR_API_KEY')}:" 

if CONFIG.get('YOUR_API_KEY') and CONFIG.get('USE_PROXY'):    
    proxies = {
        "https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
        "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)
        }
else: 
    proxies = {
        'no': 'pass',
    }

#~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setup the Flipkart scraper 
#~~~~~~~~~~~~~~~~~~~~~~~~~    
class FkScraper():
    def __init__(self):
        
        self.vendor = 'flipkart'
        self.vendor_url = "https://www.flipkart.com"
        self.jinn_path = u"\\\\?\\" + os.getcwd(); print(self.jinn_path)

    def SearchURL(self, search_term:str):
        url_query = "+".join(search_term.split())
        return f"https://www.flipkart.com/search?q={url_query}"

    def ExtractNumbers(self, text):
        numbers = re.findall(pattern='\d+\.?\d*',string=text)
        return "".join(numbers) if len(numbers) > 0 else 'None' 

    # Retry code
    def GetSoup(self, url:str,
                proxies=proxies,
                headers=random_headers(),
                parser:str='lxml'):
        while True:
            try:
                if CONFIG.get('PROXY_SERVICE') == 'scraperapi':
                    response = requests.get(
                    f"http://api.scraperapi.com?api_key={CONFIG.get('YOUR_API_KEY')}&url={url}", 
                    headers=random_headers(), verify=False)
                else:
                    response = requests.get(url,proxies=proxies, headers=random_headers(), verify=False)
                    
                if response.status_code == 200:
                    print(f'Success!{response.status_code}')
                    # with open(f"fk_page_{url.split('=')[-1]}.html",'wb') as f:
                        # f.write(response.content)
                    break
            except:
                print('Retrying')
                time.sleep(3)
                
        return BeautifulSoup(response.content, parser)
    
    # Scrape List Layout pages 
    def ListBoxInfo(self, box):
        
        box_info = {}
        
        sku_id = box['data-id']
        sku_name = box.find('div', {'class':'_3wU53n'})
        sku_desc = box.find_all('li')
        sku_rating = box.find('span',{'id':re.compile('productrating', flags=re.IGNORECASE)})
        sku_ratingcount = box.find('span', string=re.compile('ratings', flags=re.IGNORECASE))
        sku_reviewcount = box.find('span', string=re.compile('reviews', flags=re.IGNORECASE))
        sku_link = box.a['href']
        original_price = box.find('div', attrs={'class':'_3auQ3N'})
        vendor_price = box.find('div',attrs={'class':'_1vC4OE'})
        discount_perc = box.find('div',attrs={'class':'VGWI6T'})
        img_link = box.find(attrs={'src':re.compile(r".+")})

        # Handle None text info
        box_info['sku_id'] = sku_id if sku_id else 'None'
        box_info['sku_name'] = sku_name.text if sku_name else 'None'
        box_info['sku_desc'] = "|".join([i.text for i in sku_desc]) if sku_desc else 'None'
        box_info['sku_rating'] = self.ExtractNumbers(sku_rating.text) if sku_rating else 'None'
        box_info['sku_ratingcount'] = self.ExtractNumbers(sku_ratingcount.text) if sku_ratingcount else 'None'
        box_info['sku_reviewcount'] = self.ExtractNumbers(sku_reviewcount.text) if sku_reviewcount else 'None'
        box_info['sku_link'] = "https://www.flipkart.com" + sku_link if sku_link else 'None'
        box_info['original_price'] = self.ExtractNumbers(original_price.text) if original_price else 'None'
        box_info['vendor_price'] = self.ExtractNumbers(vendor_price.text) if vendor_price else 'None'
        box_info['discount_perc'] = self.ExtractNumbers(discount_perc.text) if discount_perc else 'None'
        box_info['img_link'] = img_link['src'] if img_link else 'None'
        
        return box_info
        
    # Scrape Grid layout pages
    def GridBoxInfo(self, box):
        
        box_info = {}
        string_list = [s for s in box.find_all(string=True) if s.lower() not in ['ad','out of stock','trending']]
        
        sku_id = box['data-id']
        sku_name = string_list[0]
        sku_desc = string_list[1]
        sku_rating = box.find('span',{'id':re.compile('productrating', flags=re.IGNORECASE)})
        sku_ratingcount = sku_rating.parent.find('span',string = re.compile(r"\(\d+.*\)")) if sku_rating else 'None'
        sku_reviewcount = box.find('span', string=re.compile('reviews', flags=re.IGNORECASE))
        sku_link = box.a['href']
        original_price = box.find('div', attrs={'class':'_3auQ3N'})
        vendor_price = box.find('div',attrs={'class':'_1vC4OE'})
        discount_perc = box.find('div',attrs={'class':'VGWI6T'})
        img_link = box.find(attrs={'src':re.compile(r".+")})#['src']

        # Handle None text info
        box_info['sku_id'] = sku_id if sku_id else 'None'
        box_info['sku_name'] = sku_name if sku_name else 'None'
        box_info['sku_desc'] = sku_desc if sku_desc else 'None'
        box_info['sku_rating'] = self.ExtractNumbers(sku_rating.text) if sku_rating else 'None'
        box_info['sku_ratingcount'] = self.ExtractNumbers(sku_ratingcount.text) if sku_rating else 'None'
        box_info['sku_reviewcount'] = self.ExtractNumbers(sku_reviewcount.text) if sku_reviewcount else 'None'
        box_info['sku_link'] = "https://www.flipkart.com" + sku_link if sku_link else 'None'
        box_info['original_price'] = self.ExtractNumbers(original_price.text) if original_price else 'None'
        box_info['vendor_price'] = self.ExtractNumbers(vendor_price.text) if vendor_price else 'None'
        box_info['discount_perc'] = self.ExtractNumbers(discount_perc.text) if discount_perc else 'None'
        box_info['img_link'] = img_link['src'] if img_link else 'None'
        
        return box_info
    
    def FkPageInfo(self, URL, PAGE_NO=1):
    
        # Make the soup object
        soup = self.GetSoup(URL, parser='html.parser')
                
        # Get the result list
        box_list = soup.find_all(attrs={'data-id':re.compile(".+")})
        print(len(box_list))

        page_info = []

        for box in box_list:

            # Identify if the Page Layout is Grid or List style using box width
            box_width = re.findall('\d+', box['style'])[0]

            if int(box_width) == 100: 
                box_info = self.ListBoxInfo(box)    

            else:
                box_info = self.GridBoxInfo(box)
            
            # Adding the page number
            box_info['page_no'] = PAGE_NO

            # Adding the scraping date & time 
            box_info['date'] = datetime.now().strftime("%d/%m/%Y")
            box_info['time'] = datetime.now().strftime("%H:%M:%S")

            # Add the box information to the page list
            page_info.append(box_info)
            
        return page_info, soup
        
        
        
    ## MAIN FUNCTION ## 

    def Scrape(self, search_term, save_images=False):
    
        self.save_images = save_images
        self.search_term = search_term
        
        # Make the output directory
        output_dir = f'output\\{search_term}_{DATE}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)        
            
        # # Make the image dirs (Commented out for now)
        # img_dir = f'output\\{search_term}/{self.vendor}_images_{self.search_term}'
        # if not os.path.exists(img_dir):
            # os.makedirs(img_dir)
        
        self.output_dir = output_dir
        # self.img_dir = img_dir
        
        SEARCH_URL = self.SearchURL(search_term)
        print(SEARCH_URL)

        page_no = 1
        page_info_list = []

        while True:
            
            # Random time delay
            if CONFIG['WAIT'] > 0:
                time.sleep(random.randint(CONFIG['WAIT'], CONFIG['WAIT']+3))
            
            if page_no==1:
                URL = SEARCH_URL
            print(URL, 'Page Number:', page_no)
            

            # Scrape page info
            page_info, soup = self.FkPageInfo(URL, page_no)
            page_info_list.extend(page_info)

            # Get the Nav bar 
            nav_bar = soup.select_one('nav')

            if nav_bar and nav_bar.find('span', string="Next") and len(page_info)>0:
                page_no += 1        
                # Construct the URL to the next page
                URL = SEARCH_URL + f"&page={page_no}"
                
            else:
                print('No more pages')
                break
                

            
        # with open(f'{output_dir}\\{self.vendor}_{search_term}.json', 'w') as fp:
            # json.dump(page_info_list, fp)
            
        df = pd.DataFrame(page_info_list)
        df.to_csv(f'{output_dir}\\{self.vendor}_{search_term}.csv', index=False)