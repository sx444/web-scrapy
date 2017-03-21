#####################################
##get zip codes from city-data page##
#####################################
from bs4 import BeautifulSoup
import urllib2
import re

def get_zip(web_addr, dict_zip):
	'''use each group-zips page (e.g. 10010-10200) to get each zip page'''
	request = urllib2.Request(web_addr)
	request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
	response = urllib2.urlopen(request)
	content = response.read()
	soup = BeautifulSoup(content, 'html.parser')
	zip_part = soup.find_all('a',class_="list-group-item col-lg-3 col-sm-4 col-xs-6")
	for zip_a in zip_part:
		zip_page = 'http://www.city-data.com' + zip_a['href']
		zip_code = re.findall(r'\d+', zip_a.string)[0]
		dict_zip[zip_code] = zip_page
	return dict_zip



url = 'http://www.city-data.com/zipDir.html'
request = urllib2.Request(url)
request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
response = urllib2.urlopen(request)
content = response.read()
soup = BeautifulSoup(content, 'html.parser')
# get all group-zips pages 
group_zips_pages = soup.find_all('a',class_="list-group-item col-lg-3 col-sm-4 col-xs-6")
all_zip_codes = []
# loop each group-zips pages to get all individual zip pages
dict_zip = {}
for page in group_zips_pages:
	web_addr = 'http://www.city-data.com' + page['href']
	dict_zip = get_zip(web_addr, dict_zip)
	#all_zip_codes = all_zip_codes + zip_codes

print dict_zip


'''get cost of living index using re'''
cost_living_index = {}
#for zip_code in all_zip_codes:
for zip_code, zip_page in dict_zip.iteritems():
	#zip_page = 'http://www.city-data.com/zips/%s.html' % str(zip_code)
	request = urllib2.Request(zip_page)
	request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
	response = urllib2.urlopen(request)
	pageSourceCode = response.read()
	expr = re.compile(r"cost of living index in .*?:</b>\s*(\d+(\.\d+)?)\s*<b>")
	pop_density[zip_code] = expr.findall(pageSourceCode)[0][0]

'''get population density'''
pop_density = {}
#for zip_code in all_zip_codes:
for zip_code, zip_page in dict_zip.iteritems():
	#zip_page = 'http://www.city-data.com/zips/%s.html' % str(zip_code)
	request = urllib2.Request(zip_page)
	request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
	response = urllib2.urlopen(request)
	pageSourceCode = response.read()
	expr = re.compile(r"cost of living index in .*?:</b>\s*(\d+(\.\d+)?)\s*<b>")
	pop_density[zip_code] = expr.findall(pageSourceCode)[0][0]





'''get education info'''



