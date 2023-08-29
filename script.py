from  urllib.request import urlretrieve, urlopen
import urllib.parse as urlencode
import json
import os

import requests
import bs4 

folder="output"
text_folder="extract"
path = '{}/{}/{}/{}/{}/'
target_report_type = ".pdf"

url_get_all_filter = 'https://www.vietcombank.com.vn/sxa/InvestmentApi/InvestmentFacets/?l=vi-VN&s={3B4CF33A-7B38-431C-B2C5-42EBBE48896A}&itemid={158CFC95-E771-4FC2-B6EA-1D93BCD69E70}&sig=investment-detail&investmentFacetSource={2B981AA6-1CC7-4C36-8A64-85D2F82E21A5}&yearFacetSource={4CFAD77C-53F8-4409-B385-6ACE5BE66429}&f=investmentdocumentchip%7Cinvestmentdocumentmenu%7Cinvestmentdocumentyear'
res = urlopen(url_get_all_filter)

_, list_filter, list_year = json.loads(res.read()).values()


list_year = list(map(lambda x: int(x['Key'].split(' ')[1]), list_year))
# schema [int] 

list_filter = list(map(lambda x: {"Key": x["Key"], "Value": list(map(lambda y: y["Key"],x["InvestmentMenuFacetSortOrders"]))}, list_filter))
# schema [{'Key': string, 'Value': [string]}]

def fetch_report_on_section(    
    year: int,
    filter_key: str,
    filter_value: str
) -> list[(str,str)]:
    
    year = 'Năm ' + str(year)

    _previous = {
        "l": "vi-VN",
        "s": "{3B4CF33A-7B38-431C-B2C5-42EBBE48896A}",
        "itemid": "{158CFC95-E771-4FC2-B6EA-1D93BCD69E70}",
        "sig": "investment-detail",
        "o": "SortOrder,Ascending",
        "v": "{93B61FD8-B8A6-48CA-B2B0-1C9494F79C93}",
        "investmentFacetSource": "{2B981AA6-1CC7-4C36-8A64-85D2F82E21A5}",
        "investmentdocumentmenu": filter_value,
        "investmentdocumentchip": filter_key,
        "investmentdocumentyear": year,
        "p": "200",
    }

    payload = {}
    for key, val in _previous.items():
        value = val
        if key in ['investmentdocumentmenu', 'investmentdocumentchip', 'investmentdocumentyear']:
            val = urlencode.quote(val)
        payload[key] = value

    res = requests.get(
        url="https://www.vietcombank.com.vn/sxa/InvestmentApi/InvestmentDetailResults/",
        params=payload,
        headers={
            "Sec-Ch-Ua": 'Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116',
            "Sec-Ch-Ua-Arch": "x86",
            "Sec-Ch-Ua-Bitness": "64",
            "Sec-Ch-Ua-Full-Version-List": 'Chromium";v="116.0.5845.111", "Not)A;Brand";v="24.0.0.0", "Google Chrome";v="116.0.5845.111',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Model":  "",
            "Sec-Ch-Ua-Platform":   "Windows",
            "Sec-Ch-Ua-Platform-Version":  "10.0.0",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        }
    )

    data = res.json()

    element = []
    for section in data.get("SectionResults"):
        
        metadata = section.get("Results")

        for record in metadata:

            html= bs4.BeautifulSoup(record.get("Html"))
            _download =  "https://www.vietcombank.com.vn" + html.find("a").attrs.get("href")
            _name = html.find("p").text.replace('\r\n', '').strip()
            # 
            element.append((_download, _name))


    return element


def retrive_report(download_url,target_path) -> None:
    # download
    file, header = urlretrieve(download_url, target_path) 


url_get_list_pdf = 'https://www.vietcombank.com.vn/sxa/InvestmentApi/InvestmentDetailResults/?l=vi-VN&s={3B4CF33A-7B38-431C-B2C5-42EBBE48896A}&itemid={158CFC95-E771-4FC2-B6EA-1D93BCD69E70}&sig=investment-detail&v={93B61FD8-B8A6-48CA-B2B0-1C9494F79C93}&investmentFacetSource={2B981AA6-1CC7-4C36-8A64-85D2F82E21A5}&investmentdocumentmenu=%s&investmentdocumentchip=%s&investmentdocumentyear=%s'
# file, h = urlretrieve(url_get_list_pdf % (urlencode.quote(str), urlencode.quote('Kết quả Kinh doanh Quý'), urlencode.quote('Năm 2023')), './t.txt')

tickers = ["VCB"]

#REQUIREMENTS 1
all_path_folder = []
for tick in tickers:
    for year in list_year:
       for filter in list_filter:
            for filter_value in filter.get('Value'):
                download_tuple = fetch_report_on_section(year=year, 
                                                  filter_key=filter.get('Key'),
                                                  filter_value=filter_value)
                folder_path = path.format(folder,tick,year,filter.get('Key'),filter_value)
                if len(download_tuple) != 0:
                    all_path_folder.append(folder_path)
                print('Start downloading for folder ' + folder_path)
                if not os.path.exists(folder_path) and len(download_tuple) != 0:
                    print('Make dir ' + folder_path)
                    os.makedirs(folder_path)
                for (download_url, file_name) in download_tuple:
                    status = retrive_report(download_url=download_url,
                                            target_path=folder_path + file_name + target_report_type)
                print('End downloading for folder ' + folder_path)


#REQUIREMENT2
# from fsspec.implementations.local import LocalFileSystem
import fsspec
def get_number_report_inyear(ticker,year):
    dir = '{}/{}/{}/'.format(folder, ticker, year)
    list_subdirs = ['Kết quả Kinh doanh Quý', 'Báo cáo định kỳ']
    res = 0
    for subdir in list_subdirs:
        refix_BC = fsspec.open_files(dir + subdir + '/*/BC*')
        other_refix = fsspec.open_files(dir + subdir + '/*/Báo cáo*')
        res += (len(refix_BC) + len(other_refix))
    return res

for ticker in tickers:
    statistic = {}
    for year in list_year:
        statistic[year] = get_number_report_inyear(ticker=ticker,year=year)

    for year, report_number in statistic.items():
        print('For ticker {}, having {} report in {}'.format(ticker, report_number, year))

#REQUIREMENT3

from pdf2image import convert_from_path

def convert_pdf_to_png(pdf_path): 
    tmp_path = pdf_path.replace(folder, 'tmp').replace(target_report_type, '')
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    print(pdf_path)
    pages = convert_from_path(pdf_path)
    num = 1
    for page in pages:
        page.save(tmp_path + '/' + str(num).zfill(3) + '.png', 'PNG')
        num += 1
    return tmp_path
# convert_pdf_to_png('output/VCB/2023/Kết quả Kinh doanh Quý/Quý 1/BCTC Hợp nhất Q1.2023.pdf')
# list_png = list(os.listdir('tmp/VCB/2023/Kết quả Kinh doanh Quý/Quý 1/BCTC Hợp nhất Q1.2023'))
# list_png.sort()
# text = ''
# for file in list_png:
#     text = text + pytesseract.image_to_string('tmp/VCB/2023/Kết quả Kinh doanh Quý/Quý 1/BCTC Hợp nhất Q1.2023' + '/' + file, lang='vie') + '\n'

# print(text)
def convert_png_to_text(png_path):
    list_png = list(os.listdir(png_path))
    list_png.sort()
    text = ''
    for file in list_png:
        text = text + pytesseract.image_to_string(png_path + '/' + file, lang='vie') + '\n'
    return text

def fetch_content(target_path) -> str:
    # Extract
    tmp_path = convert_pdf_to_png(target_path)
    content = convert_png_to_text(tmp_path)
    return content

for p in ['output/VCB/2023/Kết quả Kinh doanh Quý/Quý 1/']:#all_path_folder:
    for file in ['BCTC Hợp nhất Q1.2023.pdf']:#os.listdir(p):
        content = fetch_content(target_path=p + file)
        target_folder = p.replace(folder, text_folder)
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        fp = open(target_folder + file.replace(target_report_type, '.txt'), 'w', encoding = 'utf8')
        fp.write(content)










