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
# Setup the Amazon scraper 
#~~~~~~~~~~~~~~~~~~~~~~~~~    
class AmScraper():
    def __init__(self):

        self.vendor = 'amazon'
        self.vendor_url = "https://www.amazon.in"
        # self.search_term = ''
        self.jinn_path = u"\\\\?\\" + os.getcwd(); print(self.jinn_path)

    def SearchURL(self, search_term:str):
        url_query = "+".join(search_term.split())
        return f"https://www.amazon.in/s?k={url_query}"

    def ExtractNumbers(self, text):
        numbers = re.findall(pattern='\d+\.?\d*',string=text)
        return "".join(numbers) if len(numbers) > 0 else 'None' 

    def GetRatings(self, rating_box):
        ratings = rating_box.find('span', string=re.compile('stars'))
        rating_count = rating_box.find('span', string=re.compile('^(?!.*stars).*\d+\.*\d*$'))
        ratings = re.findall(r"\d+.?\d*", ratings.text)[0] if ratings else 'None'
        rating_count = rating_count.text if rating_count else 'None'
        return ratings, rating_count
    
    def ImageFilename(self, box_info):    
        # Remove Illegal characters in Windows
        name_chars = re.findall(r"[A-Za-z0-9 \.-]", box_info['sku_name'])
        desc_chars = re.findall(r"[A-Za-z0-9 \.-]", box_info['sku_desc'])
        norm_name = "".join(name_chars)
        norm_desc = "".join(desc_chars)[:100]
        
        # Create a filename with vendor and ID 
        image_filename = "{}_{}_{}_{}{}".format(
                    self.vendor, 
                    box_info['sku_id'],
                    norm_name,
                    norm_desc,
                    os.path.splitext(box_info['img_link'])[1]
                )
        return image_filename
        
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
                    # with open(f"amz_page_{url.split('=')[-1]}.html",'wb') as f:
                        # f.write(response.content)
                    break
            except:
                print('Retrying')
                time.sleep(3)
        
        return BeautifulSoup(response.content, parser)
    
    # Scrape List Layout pages 
    def ListBoxInfo(self, box):
    
        box_info = {}

        sku_id = box['data-asin']
        sku_name = box.select_one('h2')
        sku_desc = box.select_one('h2')
        sku_rating = box.select_one('div[class*="a-row a-size-small"]')
        sku_ratingcount = box.select_one('div[class*="a-row a-size-small"]')
        sku_link = box.select_one('a[class="a-link-normal a-text-normal"]')
        vendor_price = box.select_one('span[class*="a-price-whole"]')
        original_price = box.select_one('span[class*="a-price a-text-price"] > span')
        discount_perc = box.find('span', string=re.compile("\(\d+.*%\)"))
        alt_price = box.find('span', {'class':re.compile("[a\-color\-price]")},string=re.compile("₹"))
        img_link = box.select_one('img[class="s-image"]')

        # Handle None text info
        box_info['sku_id'] = box['data-asin'] if box else None
        box_info['sku_name'] = 'None' #sku_name.text.strip() if sku_name else 'None'
        box_info['sku_desc'] = sku_desc.text.strip() if sku_desc else 'None'
        box_info['sku_rating'] = self.GetRatings(sku_rating)[0] if sku_rating else 'None'
        box_info['sku_ratingcount'] = self.GetRatings(sku_ratingcount)[1] if sku_ratingcount else 'None'
        box_info['sku_link'] = "https://www.amazon.in" + sku_link['href'] if sku_link else 'None'
        box_info['vendor_price'] = self.ExtractNumbers(vendor_price.text) if vendor_price else 'None'
        box_info['original_price'] = self.ExtractNumbers(original_price.text) if original_price else 'None'
        box_info['discount_perc'] = re.findall("\((\d+).*%\)", discount_perc.text)[0] if discount_perc else 'None'
        box_info['alt_price'] = self.ExtractNumbers(alt_price.text) if alt_price else 'None'
        box_info['img_link'] = img_link['src']  if img_link else 'None'

        # Download the image to disk
        if img_link and self.save_images:
            try:
                print(self.ImageFilename(box_info))
                
                img_request = requests.get(box_info['img_link'],
                                           proxies=proxies,
                                           headers=random_headers(),
                                           verify=False)
                                           
                img_path = os.path.join(self.img_dir, self.ImageFilename(box_info))
                print(img_path)
                print(len(img_path))
                with open(img_path , 'wb') as fp:
                    fp.write(img_request.content)  
            except:
                print('Cant write image')
                pass

        return box_info

    # Scrape Grid layout pages
    #  
    def GridBoxInfo(self, box):
        box_info = {}

        sku_id = box['data-asin']
        sku_name = box.select_one('span.a-size-base-plus.a-color-base')
        sku_desc = box.select_one('h2')
        sku_rating = box.find('div',{'class':"a-row a-size-small"})
        sku_ratingcount = box.find('div',{'class':"a-row a-size-small"})
        sku_link = box.select_one('a[class="a-link-normal a-text-normal"]')
        vendor_price = box.select_one('span[class="a-price-whole"]')
        original_price = box.select_one('span[class="a-price a-text-price"] > span')
        discount_perc = box.find('span', string=re.compile("\(\d+.*%\)"))
        alt_price = box.find('span', {'class':re.compile("[a\-color\-price]")},string=re.compile("₹"))
        img_link = box.select_one('img[class="s-image"]')

        # Handle None text info
        box_info['sku_id'] = box['data-asin'] if box else None
        box_info['sku_name'] = sku_name.text.strip() if sku_name else 'None'
        box_info['sku_desc'] = sku_desc.text.strip() if sku_desc else 'None'
        box_info['sku_rating'] = self.GetRatings(sku_rating)[0] if sku_rating else 'None'
        box_info['sku_ratingcount'] = self.GetRatings(sku_ratingcount)[1] if sku_ratingcount else 'None'
        box_info['sku_link'] = "https://www.amazon.in" + sku_link['href'] if sku_link else 'None'
        box_info['vendor_price'] = self.ExtractNumbers(vendor_price.text) if vendor_price else 'None'
        box_info['original_price'] = self.ExtractNumbers(original_price.text) if original_price else 'None'
        box_info['discount_perc'] = re.findall(pattern="\((\d+).*%\)", string=discount_perc.text)[0] if discount_perc else 'None'
        box_info['alt_price'] = self.ExtractNumbers(alt_price.text) if alt_price else 'None'
        box_info['img_link'] = img_link['src']  if img_link else 'None'

        # Download the image to disk
        if img_link and self.save_images:
            try:
                print(self.ImageFilename(box_info))
                
                img_request = requests.get(box_info['img_link'],
                                           proxies=proxies,
                                           headers=random_headers(),
                                           verify=False)
                
                with open(os.path.join(self.img_dir, self.ImageFilename(box_info)) , 'wb') as fp:
                    fp.write(img_request.content)  
            except:
                print('Cant write image')
                pass

        return box_info

        
    def AmPageInfo(self, URL, PAGE_NO=1):
    

    
        # Make the soup object
        soup = self.GetSoup(URL, parser='html.parser')
        
        # Get the result list
        box_list = soup.find_all(attrs={'data-asin':re.compile(".+"),
                                      'data-index':re.compile("\d+")})
        print(len(box_list))

        page_info = []

        for box in box_list:

            # Identify if the Page Layout is Grid or List style using box width
            if box['class'][0] == 'sg-col-20-of-24': 
                box_info = self.ListBoxInfo(box)

            else:
                box_info = self.GridBoxInfo(box)
            
            # Adding the page number
            box_info['page_no'] = PAGE_NO

            # Adding the scraping date & time 
            box_info['date'] = datetime.now().strftime("%d/%m/%Y")
            box_info['time'] = datetime.now().strftime("%H:%M:%S")
            
            # Remove the queryid from the links
            box_info['sku_link'] =  re.sub(r"&qid=\d+","",box_info['sku_link']) 

            # Add the box information to the page list
            page_info.append(box_info)
            
        return  page_info, soup
        
        
        
    ## MAIN FUNCTION ## 

    def Scrape(self, search_term, save_images=False):
        
        self.save_images = save_images
        self.search_term = search_term
        
        # Make the output directory
        output_dir = f'{self.jinn_path}\\output\\{search_term}_{DATE}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)        
        self.output_dir = output_dir
        
        # Make the image dirs
        if save_images:
            img_dir = f'{output_dir}\\{self.vendor}_images_{self.search_term}'
            if not os.path.exists(img_dir):
                os.makedirs(img_dir)
            self.img_dir = img_dir
        
        
        
        
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
            page_info, soup  = self.AmPageInfo(URL, page_no)
            page_info_list.extend(page_info)

            # Get the Nav bar 
            next_page = soup.select_one('ul.a-pagination > li.a-last > a')

            if next_page and len(page_info)>0:
                page_no +=1 
                
                # Construct the URL to the next page
                URL = SEARCH_URL + f"&page={page_no}"               

            else:
                print('No more pages')
                break
                
        
            
        # with open(f'{output_dir}\\{self.vendor}_{search_term}.json', 'w') as fp:
            # json.dump(page_info_list, fp)
            
        df = pd.DataFrame(page_info_list)
        df.to_csv(f'{output_dir}\\{self.vendor}_{search_term}.csv', index=False)