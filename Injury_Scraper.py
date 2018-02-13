import bs4 as bs
from urllib.request import urlopen
import time
import pandas as pd


class Injury_Scraper(object):
    global url, teams, years, cols
    url = 'http://www.pro-football-reference.com/teams/'

    # all team tickers
    teams = ['mia','nyj','nwe','buf','clt','jax','htx','oti'
        ,'rav','pit','cle','cin','sdg','rai','kan','den'
        ,'phi','was','dal','nyg','tam','car','nor','atl'
        ,'chi','min','gnb','det','ram','sea','sfo','crd']

    # range of years (might make non-constant in the future)
    years = list(range(2009,2017))

    # scrapes pro football reference injuries from site
    # url format: http://www.pro-football-reference.com/teams/TICKER/YEAR_injuries.htm'
    # output: list of list: [player name, date, opposing team, injury, chance of playing, played(bool)]
    @staticmethod
    def scrape(url, sleep_time=2):

        # url request delay time
        time.sleep(sleep_time)

        # url request
        source = urlopen(url)
        soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")

        # get current year
        year = soup.find('title').text.split()[0]

        # table headers
        thead = soup.find('thead').findAll('th')
        del thead[0] # useless row header 'Player'

        # dates and opponents from table header
        dates = [year+'/'+game.text.split('vs. ')[0] for game in thead]

#[unicode(x.strip()) if x is not None else '' for x in row]
        print(dates)
        # format dates
        dates = [w.replace('/', '-') for w in dates]

        # get opponents
        verses = [game.text.split('vs. ')[1] for game in thead]

        table = soup.find('tbody').findAll('tr')
        # get all players in table
        players = [t.find('th').text for t in table]

        table = [t.findAll('td') for t in table]

        # corresponds to the shading on table, not sure if this means they actually played.
        played = [[x.get('class', '') for x in t] for t in table]

        # flatten list
        played = [val for sublist in played for val in sublist]
        played = [False if 'dnp' in x else True for x in played]

        # gets player injury status on given day
        injuries = [[x.get('data-tip', 'Healthy') for x in t] for t in table]

        # flatten list
        injuries = [val for sublist in injuries for val in sublist]

        # split injury and chance of playing
        chance_of_playing = [x.split(': ')[0] if len(x.split(': ')) == 2 else 'Good' for x in injuries]
        injuries = [x.split(': ')[1] if len(x.split(': ')) == 2 else x.split(': ')[0] for x in injuries]

        # putting it all together
        temp = [[player,date,vs] for player in players for date,vs in zip(dates,verses)]
        output = [(t+[i]+[c]+[p]) for t,i,c,p in zip(temp,injuries,chance_of_playing,played)]

        return output

    # scrapes all injuries from the list of teams and years above
    @staticmethod
    def scrape_all(sleep_time=2):
        # loops through every team for every year
        fails = 0
        injuries = []
        starttime = time.time()
        for team in teams:
            for year in years:
                current_url = url+team+'/'+str(year)+'_injuries.htm'
                current_urls_injuries = []
                try:
                    print('Scraping:',team,year)
                    current_urls_injuries = Injury_Scraper().scrape(current_url,sleep_time)
                except:
                    fails+=1
                    print('Scrape failed on:',team,year,current_url)
                    continue

                injuries = injuries + current_urls_injuries

        print('Scrape completed. Scrapes failed:',fails)
        print('Elapsed time',time.time()-starttime)

        return injuries

"""
df = pd.read_csv('injuries.csv')

#print(df.iloc[2010:2035][['Player','Date','Injury','Chance_of_playing', 'Played']])
print(df.loc[df['Player'] == 'Benny Cunningham'])
#print((df.loc[df['Player'] == 'Bruce Miller']).iloc[15:])
#print(df[(df['Player'] == 'Bruce Miller') & (df['Date'] == '2012-01-12' )])
"""

# get all injuries
inj = Injury_Scraper().scrape_all(0)

# create df & save to csv
cols = ['Player','Date','Opp','Injury','Chance_of_playing','Played']
df = pd.DataFrame(inj, columns=cols)
df.to_csv('injuries.csv')

print(df.head())
print(len(inj))


