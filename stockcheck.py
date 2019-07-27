import requests
from selenium import webdriver
from bs4 import BeautifulSoup
import time
from PIL import Image
from io import BytesIO
from multiprocessing import Pool
import os


def checkStock(stock):
    stats = []
    statsGetters = [
        getConsistentEarningsGrowth,
        getReturnOnEquity,
        getFreeCashFlow,
        getDebtToEquityRatio,
        getRating,
        getChart,
        getLiquidity,

        # getLongLastingCompetitiveAdvantage,
        # getNews,
        # getOptionable,
        # getUndervalue,
        # getPE,
    ]

    for getStats in statsGetters:
        stats.extend(getStats(stock))

    for stat in stats:
        if 'EPS Estimate > Year Ago EPS' in stat['label']:
            stat['check'] = f"{stat['averageEPSEstimate']:,} > {stat['yearAgoEPS']:,}"
            stat['result'] = stat['averageEPSEstimate'] > stat['yearAgoEPS']

        elif stat['label'] == 'EPS Estimate > 90 Days Ago EPS Estimate':
            stat['check'] = f"{stat['currentEPSEstimate']:,} > {stat['90DaysAgoEPSEstimate']:,}"
            stat['result'] = stat['currentEPSEstimate'] > stat['90DaysAgoEPSEstimate']

        elif 'Actual EPS > EPS Estimate' in stat['label']:
            stat['check'] = f"{stat['epsActual']:,} > {stat['epsEstimate']:,}"
            stat['result'] = stat['epsActual'] > stat['epsEstimate']

        elif stat['label'] == 'Return on Equity (ROE) > 0.15':
            stat['check'] = f"{stat['returnOnEquity']:,} > {stat['returnOnEquityTarget']:,}"
            stat['result'] = stat['returnOnEquity'] > stat['returnOnEquityTarget']

        elif stat['label'] == 'Return on Equity (ROE) > Sector (Top 20 Market Cap) Return on Equity':
            stat['check'] = f"{stat['returnOnEquity']:,} > {stat['sectorReturnOnEquity']:,}"
            stat['result'] = stat['returnOnEquity'] > stat['sectorReturnOnEquity']

        elif stat['label'] == '(Free Cash Flow / Revenue) > 0.15':
            stat['check'] = f"{stat['freeCashFlowOverRevenue']:,} > {stat['freeCashFlowOverRevenueTarget']:,}"
            stat['result'] = stat['freeCashFlowOverRevenue'] > stat['freeCashFlowOverRevenueTarget']

        elif stat['label'] == 'Debt to Equity Ratio < 2':
            stat['check'] = f"{stat['debtToEquityRatio']:,} < {stat['debtToEquityRatioTarget']:,}"
            stat['result'] = stat['debtToEquityRatio'] < stat['debtToEquityRatioTarget']

        elif stat['label'] == 'Stock Rating out of 5 \n (1 = strong buy, 2 = buy, 3 = hold, 4 = sell, 5 = strong sell)':
            stat['check'] = stat['weightedAverageRatingOutOf5']
            stat['result'] = None

        elif stat['label'] == 'Chart > 50 Day Moving Average Line':
            stat['image'] = stat['chart']
            stat['check'] = None
            stat['result'] = None

        elif stat['label'] == '3 Month Average Share Volume > 500,000':
            stat['check'] = f"{stat['3MonthAverageShareVolume']:,} > {stat['3MonthAverageShareVolumeTarget']:,}"
            stat['result'] = stat['3MonthAverageShareVolume'] > stat['3MonthAverageShareVolumeTarget']

    return stats
    # printStats(stock, stats)


def periodCodeToStr(period):
    codeToStr = {
        '0q': 'Current Quarter',
        '+1q': 'Next Quarter',
        '0y': 'Current Year',
        '+1y': 'Next Year',
        '-4q': 'Last Year',
        '-3q': '3 Quarters Ago',
        '-2q': '2 Quarters Ago',
        '-1q': 'Last Quarter'
    }
    return codeToStr[period]


def getConsistentEarningsGrowth(stock):
    stats = []

    data = getYahooFinanceData(stock, 'analysis')

    epsTrends = data['earningsTrend']['trend']

    for epsTrend in epsTrends:
        if epsTrend['period'] not in ['0q', '+1q', '0y', '+1y']:
            continue

        statsItem = {
            'label': f"{periodCodeToStr(epsTrend['period'])} EPS Estimate > Year Ago EPS",
            'period': epsTrend['period'],
            'averageEPSEstimate': epsTrend['earningsEstimate']['avg']['raw'],
            'yearAgoEPS': epsTrend['earningsEstimate']['yearAgoEps']['raw'],
        }
        stats.append(statsItem)

        if epsTrend['period'] == '0q':
            statsItem = {
                'label': 'EPS Estimate > 90 Days Ago EPS Estimate',
                'currentEPSEstimate': epsTrend['epsTrend']['current']['raw'],
                '90DaysAgoEPSEstimate': epsTrend['epsTrend']['90daysAgo']['raw']
            }
            stats.append(statsItem)

    epsEarningsHistory = data['earningsHistory']['history']

    for epsEarningsQuarter in epsEarningsHistory:
        statsItem = {
            'label': f"{periodCodeToStr(epsEarningsQuarter['period'])} Actual EPS > EPS Estimate",
            'period': epsEarningsQuarter['period'],
            'epsEstimate': epsEarningsQuarter['epsEstimate']['raw'],
            'epsActual': epsEarningsQuarter['epsActual']['raw']
        }
        stats.append(statsItem)

    return stats


def getReturnOnEquity(stock):
    stats = []

    statistics = getYahooFinanceData(stock, 'statistics')

    if statistics['financialData']['returnOnEquity'] == {}:
        return stats

    returnOnEquity = statistics['financialData']['returnOnEquity']['raw']
    stats.append({
        'label': 'Return on Equity (ROE) > 0.15',
        'returnOnEquity': returnOnEquity,
        'returnOnEquityTarget': 0.15
    })

    profile = getYahooFinanceData(stock, 'profile')
    sector = profile['assetProfile']['sector']

    sectorStocks = getSectorStocks(sector)
    sectorStocks = chunk(sectorStocks, 5)
    
    p = Pool(len(sectorStocks))
    sectorROEDependents = p.map(getSectorROEDependents, sectorStocks)
    p.close()
    
    sectorROEDependents = [elem for arr in sectorROEDependents for elem in arr] # flatten

    # sectorROEDependents = getSectorROEDependents(sectorStocks)
    totalMarketCap = sum([s['marketCap'] for s in sectorROEDependents])
    sectorROE = sum([s['returnOnEquity'] * s['marketCap'] for s in sectorROEDependents]) / totalMarketCap

    stats.append({
        'label': 'Return on Equity (ROE) > Sector (Top 20 Market Cap) Return on Equity',
        'returnOnEquity': round(returnOnEquity, 4),
        'sectorReturnOnEquity': round(sectorROE, 4)
    })
    return stats


'''
Get 25 largest stocks in the sector by market cap
'''
def getSectorStocks(sector):
    sectorURL = '_'.join(sector.lower().split(' '))

    r = getWithRetries(f'https://finance.yahoo.com/sector/ms_{sectorURL}?count=100&offset=0', {})

    html = BeautifulSoup(r.content, 'html.parser')
    table = html.find(id='scr-res-table')
    stocks = [stockLabel.text for stockLabel in table.find_all('a')][:20]

    return stocks


def extractROE(html):
    roeStr = html.find('span', string='Return on Equity').parent.nextSibling.text
    if roeStr == 'N/A':
        return None

    return float(roeStr[:-1])/ 100


def extractMarketCap(html):
    marketCapStr = html.find('span', string='Market Cap (intraday)').parent.nextSibling.text
    if marketCapStr[-1] == 'M':
        return float(marketCapStr[:-1]) * 1_000_000
    elif marketCapStr[-1] == 'B':
        return float(marketCapStr[:-1]) * 1_000_000_000
    elif marketCapStr[-1] == 'T':
        return float(marketCapStr[:-1]) * 1_000_000_000_000


'''
Get sector return on equity using weighted average (by market cap) of 100 largest stocks in sector
'''
def getSectorROEDependents(sectorStocks):
    sectorROEDependents = []
    idx = 0
    for stock in sectorStocks:
        r = getWithRetries(f'https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}', {})
        html = BeautifulSoup(r.content, 'html.parser')

        roe = extractROE(html)
        marketCap = extractMarketCap(html)
        if roe == None:
            continue

        sectorROEDependents.append({'returnOnEquity': roe, 'marketCap': marketCap})

        idx += 1

    return sectorROEDependents


def getFreeCashFlow(stock):
    financialsData = getYahooFinanceData(stock, 'financials')
    statisticsData = getYahooFinanceData(stock, 'statistics')

    cashFlowData = financialsData['cashflowStatementHistory']['cashflowStatements'][0]

    item = {
        'label': '(Free Cash Flow / Revenue) > 0.15',
        'totalCashFlowFromOperatingActivities': cashFlowData['totalCashFromOperatingActivities']['raw'],
        'capitalExpenditures': cashFlowData['capitalExpenditures']['raw'],
        'revenue': statisticsData['financialData']['totalRevenue']['raw'],
        'freeCashFlowOverRevenueTarget': 0.15
    }
    item['freeCashFlow'] = item['totalCashFlowFromOperatingActivities'] - item['capitalExpenditures']
    item['freeCashFlowOverRevenue'] = round(item['freeCashFlow'] / item['revenue'], 4)

    return [item]


def getDebtToEquityRatio(stock):
    data = getYahooFinanceData(stock, 'statistics')

    item = {
        'label': 'Debt to Equity Ratio < 2',
        'debtToEquityRatio': data['financialData']['currentRatio']['raw'],
        'debtToEquityRatioTarget': 2
    }

    return [item]


'''
Get rating using weighted average of Yahoo Finance recommendations
'''
def getRating(stock):
    data = getYahooFinanceData(stock, 'analysis')
    recs = data['recommendationTrend']['trend'][0]

    total = recs['strongBuy'] + recs['buy'] + recs['hold'] + recs['sell'] + recs['strongSell']
    weightedTotal = 1 * recs['strongBuy'] + 2 * recs['buy'] + 3 * recs['hold'] + 4 * recs['sell'] + 5 * recs['strongSell']


    return [{
        'label': 'Stock Rating out of 5 \n (1 = strong buy, 2 = buy, 3 = hold, 4 = sell, 5 = strong sell)',
        'strongBuyRecs': recs['strongBuy'],
        'buyRecs': recs['buy'],
        'holdRecs': recs['hold'],
        'sellRecs': recs['sell'],
        'strongSellRecs': recs['strongSell'],
        'weightedAverageRatingOutOf5': round(weightedTotal / total, 2)
    }]


def getChart(stock):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')

    chrome = webdriver.Chrome()
    chrome.set_window_position(0, 0)
    chrome.set_window_size(2400, 768)

    chrome.get(
        'https://finance.yahoo.com/chart/' + stock + '#eyJpbnRlcnZhbCI6ImRheSIsInBlcmlvZGljaXR5IjoxLCJjYW5kbGVXaWR0aCI6OCwidm9sdW1lVW5kZXJsYXkiOnRydWUsImFkaiI6dHJ1ZSwiY3Jvc3NoYWlyIjp0cnVlLCJjaGFydFR5cGUiOiJjYW5kbGUiLCJleHRlbmRlZCI6ZmFsc2UsIm1hcmtldFNlc3Npb25zIjp7fSwiYWdncmVnYXRpb25UeXBlIjoib2hsYyIsImNoYXJ0U2NhbGUiOiJsb2ciLCJwYW5lbHMiOnsiY2hhcnQiOnsicGVyY2VudCI6MSwiZGlzcGxheSI6IlpVTyIsImNoYXJ0TmFtZSI6ImNoYXJ0IiwidG9wIjowfX0sInNldFNwYW4iOnt9LCJsaW5lV2lkdGgiOjIsInN0cmlwZWRCYWNrZ3JvdWQiOnRydWUsImV2ZW50cyI6dHJ1ZSwiY29sb3IiOiIjMDA4MWYyIiwiZXZlbnRNYXAiOnsiY29ycG9yYXRlIjp7ImRpdnMiOnRydWUsInNwbGl0cyI6dHJ1ZX0sInNpZ0RldiI6e319LCJjdXN0b21SYW5nZSI6bnVsbCwic3ltYm9scyI6W3sic3ltYm9sIjoiWlVPIiwic3ltYm9sT2JqZWN0Ijp7InN5bWJvbCI6IlpVTyJ9LCJwZXJpb2RpY2l0eSI6MSwiaW50ZXJ2YWwiOiJkYXkiLCJzZXRTcGFuIjp7fX1dLCJzdHVkaWVzIjp7InZvbCB1bmRyIjp7InR5cGUiOiJ2b2wgdW5kciIsImlucHV0cyI6eyJpZCI6InZvbCB1bmRyIiwiZGlzcGxheSI6InZvbCB1bmRyIn0sIm91dHB1dHMiOnsiVXAgVm9sdW1lIjoiIzAwYjA2MSIsIkRvd24gVm9sdW1lIjoiI0ZGMzMzQSJ9LCJwYW5lbCI6ImNoYXJ0IiwicGFyYW1ldGVycyI6eyJ3aWR0aEZhY3RvciI6MC40NSwiY2hhcnROYW1lIjoiY2hhcnQifX0sIuKAjG1h4oCMICg1MCxDLG1hLDApIjp7InR5cGUiOiJtYSIsImlucHV0cyI6eyJQZXJpb2QiOjUwLCJGaWVsZCI6IkNsb3NlIiwiVHlwZSI6InNpbXBsZSIsIk9mZnNldCI6MCwiaWQiOiLigIxtYeKAjCAoNTAsQyxtYSwwKSIsImRpc3BsYXkiOiLigIxtYeKAjCAoNTAsQyxtYSwwKSJ9LCJvdXRwdXRzIjp7Ik1BIjoiI2FkNmVmZiJ9LCJwYW5lbCI6ImNoYXJ0IiwicGFyYW1ldGVycyI6eyJjaGFydE5hbWUiOiJjaGFydCJ9fX19'
    )

    time.sleep(3) # yes, this is janky and *theoretically* non-deterministic

    png = chrome.get_screenshot_as_png()
    chrome.quit()

    filename = f'chart-{stock}.png'
    im = Image.open(BytesIO(png))
    im.save(f'{os.getcwd()}/static/{filename}')

    return [{
        'label': 'Chart > 50 Day Moving Average Line',
        'chart': filename
    }]


def getLiquidity(stock):
    data = getYahooFinanceData(stock, 'statistics')
    item = {
        'label': '3 Month Average Share Volume > 500,000',
        '3MonthAverageShareVolume': data['summaryDetail']['averageVolume']['raw'],
        '3MonthAverageShareVolumeTarget': 500_000
    }
    return [item]


def getLongLastingCompetitiveAdvantage(stock):
    data = getYahooFinanceData(stock, 'profile')
    return [{'profile':data['assetProfile']['longBusinessSummary']}]


def getNews(stock):
    raise NotImplementedError


def getOptionable(stock):
    raise NotImplementedError


def getUndervalue(stock):
    raise NotImplementedError


def getPE(stock):
    raise NotImplementedError


def getWithRetries(url, headers):
    tries = 0
    while tries < 5:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r
        else:
            print(f'{r.status_code} received from {url}')
            tries += 1

    return {}


DATA_CACHE = {
    'financials': None,
    'statistics': None,
    'analysis': None,
    'profile': None
}

def getYahooFinanceData(stock, dataType):
    if DATA_CACHE[dataType] != None:
        return DATA_CACHE[dataType]

    headers = {
        'X-RapidAPI-Host': 'apidojo-yahoo-finance-v1.p.rapidapi.com',
        'X-RapidAPI-Key': '5f59c51c7fmshc71b4e7898ed7a4p17ea69jsnd8f001589868'
    }

    url = 'https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-' + dataType + '?region=US&symbol=' + stock
    data = getWithRetries(url, headers=headers).json()
    DATA_CACHE[dataType] = data
    return data


def chunk(arr, size):
    return [arr[i*size:(i+1)*size] for i in range(int(len(arr) / size))]


bcolors = {
    'HEADER': '\033[95m',
    'BLUE': '\033[94m',
    'GREEN': '\033[92m',
    'WARNING': '\033[93m',
    'RED': '\033[91m',
    'ENDC': '\033[0m',
    'BOLD': '\033[1m',
    'UNDERLINE': '\033[4m'
}

def printColor(color, str):
    print(f"{bcolors[color]} {str} {bcolors['ENDC']}")

def printStats(stock, stats):
    printColor('BOLD', f"{stock}\n-----------------")

    for idx, stat in enumerate(stats):
        printColor('BOLD', f"{idx}. {stat['label']}")
        printColor('BLUE', stat['check'])
        if stat['result'] == True:
            printColor('GREEN', stat['result'])
        elif stat['result'] == False:
            printColor('RED', stat['result'])
        print()
