#coding:UTF-8
import re
import requests
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')

def get_data(zip_code):
    url = "http://www.city-data.com/zips/{0}.html".format(zip_code)
    req = requests.get(url)
    html = req.text
    soup = BeautifulSoup(html, 'html.parser')
    # start to get and save data
    data = {'zip code': zip_code}
    # get cost of living
    #names.append('cost of living index')
    expr = re.compile(r"cost of living index in .*?:</b>\s*(\d+(\.\d+)?)\s*<b>")
    cl = float(expr.findall(html)[0][0])
    #data.append(cl)
    # create dictionary to record data
    data['cost of living index'] = cl
    # get population density
    #names.append('population density')
    tables = re.findall('<table border="0" cellpadding="0" cellspacing="0">(.*?)</table>', html)
    tr = re.findall('<tr>(.*?)</tr>',str(tables))
    td = re.findall('<td>(.*?)</td>',tr[0])
    pd = int(re.findall('</b> (.*?) <b>',td[0])[0].replace(',',''))
    #data.append(pd)
    # add data to dic
    data['population density'] = pd
    # get sex 
    td = re.findall('<td>(.*?)</td>',tr[1])
    male = td[-1][td[-1].find("(")+1:td[-1].find(")")]
    male_pct = float(male.replace("%",""))*0.01
    data['male_pct'] = male_pct
    # get education
    li = re.findall('<li>(.*?)</li>',html)
    for i in li[11:15]:
        name = str(re.findall('<b>(.*?):</b>',i)[0])
        value = float(re.findall("\d+\.\d+", i)[0]) * 0.01
        data[name] = value
    # get marriage status
    for i in li[16:21]:
        name = str(re.findall('<b>(.*?):</b>',i)[0])
        value = float(re.findall("\d+\.\d+", i)[0]) * 0.01
        data[name] = value
    # get race percentage
    race = re.findall("<span class='badge'>(.*?)\r", html)[:8]
    total = 0
    for i in race:
        i = i.replace(",","")
        number = int(re.findall(r'[0-9]+',i)[0])
        total += number
    for i in race:
        i = i.replace(",","")
        number = int(re.findall(r'[0-9]+',i)[0])
        name = str(re.findall("</span>(.*?) population</li>", i)[0])
        pct = number/float(total)
        data[str(name+'_pct')] = pct
    # house value, age, household size
    parts = re.findall("<div class='hgraph'><b>(.*?)</div>", html)[:3]
    # house value
    name = str(re.findall("(.*?):</b> .*?", parts[0]))
    value = re.findall("</b> (.*?)<table>",parts[0])[0]
    value = value.replace(",","")
    number = float(re.findall(r'[0-9]+',value)[0])
    data[name] = number
    # age and household size
    for i in range(1,3):
        name = str(re.findall("(.*?):</b> .*?", parts[i]))
        value = re.findall("</p>(.*?)</td>",parts[i])[0]
        number = float(re.findall("\d+\.\d+", value)[0])
        data[name] = number
    # hoursehold info
    parts = re.findall("<div class='hgraph'><b>(.*?)</div>", html)[3:]
    # hoursehold median income
    income = parts[0]
    name = re.findall("<b>(.*?): </b>", income)[0]
    value = re.findall("</p>(.*?)</td>",income)[0].replace(",","")
    number = int(re.findall(r'[0-9]+',value)[0])
    data[name] = number
    # household others
    for i in parts[1:3]:
        name = re.findall("(.*?):</b>", i)[0]
        value = re.findall("</p>(.*?)</td>",i)[0]
        number = str(value).replace("%","")
        number = 0.01 * float(number)
        data[name] = number
    # les and gay
    parts = re.findall("<li><b>(.*?)</b></li>", html)[-3:-1]
    for i in parts:
        name = re.findall("(.*?):</b>",i)[0]
        value = re.findall("</b> (.*?) <b>",i)[0]
        number = str(value).replace("%","")
        number = float(number) * 0.01
        data[name] = number
    # food stamp pct
    parts = re.findall("SNAP(.*?)<br/>\r",html)
    snap = float(re.findall(r'[0-9]+',parts[0])[1])
    no_snap = float(re.findall(r'[0-9]+',parts[1].replace(",",""))[1])
    data['pct_receive_SNAP'] = snap/(snap+no_snap)
    data['pct_no_SNAP'] = no_snap/(snap+no_snap)
    # birth by unmarried women
    part = str(re.findall('\r\n<b>(.*?)</b><br>\r\n',html)[2]).replace(",","")
    total = re.findall(r'[0-9]+',part)
    data['birth_by_unmarried_women'] = float(total[-1])/float(total[1])
    # mortgage price
    part = str(re.findall('<b>(.*?)<br/>\r\n',html)[-2]).replace(",","")
    name = re.findall('(.*?):</b>',part)[0]
    value = int(re.findall(r'[0-9]+',part)[0])
    data[name] = value
    # poverty
    parts = re.findall("<b>(.*?)Whole state",html)[:2]
    value = re.findall("</p>(.*?)</td>",parts[0])[0]
    number = float(value.replace("%",""))*0.01
    data['pct below poverty level in 2013'] = number
    value = re.findall("</p>(.*?)</td>",parts[1])[0]
    number = float(value.replace("%",""))*0.01
    data['pct below half poverty level in 2013'] = number
    # number of rooms
    parts = re.findall("<div class='hgraph'><b>(.*?)</div>",html)[-2:]
    for i in parts:
        name = str(re.findall("(.*?):</b><table><tr>",i)[0])
        value = float(re.findall("</p>(.*?)</td></tr>",i)[0])
        data[name] = value
    # housing price
    parts = re.findall("\r\n<b>(.*?).\r\n</p>",html)[-2:]
    for i in parts:
        name = str(re.findall("(.*?):</b>",i)[0])
        value = int(re.findall(r'[0-9]+',i.replace(",",""))[-1])
        data[name] = value
    # 






def  get_zip():
    # get a list of all zip codes in the us.
    zip_codes = pd.read_excel("./web/USA-Zip.xls")['ZIP code'].tolist()
    zip_codes = zip_codes + pd.read_excel("./web/USA-Zip.xls")['ZIP code.1'].dropna().tolist()

    for zip_code in zip_codes:
        get_data(str(zip_code))


if __name__=='__main__':
    get_zip()