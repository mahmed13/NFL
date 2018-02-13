# http://www.pro-football-reference.com/players/
import bs4 as bs
from urllib.request import urlopen
import time
from datetime import datetime
import pickle
import os
import sys
import re
import multiprocessing
import os
import glob
import pandas as pd
from functools import partial

global DATA_DIRECTORY_NAME

# directory and file name constants
DATA_DIRECTORY_NAME = 'Player Scraper Data/'
ERROR_LOG_DIRECTORY_NAME = 'errorlog/'
PLAYER_DATA_FILENAME = 'Player_Data_'
URL_DICT_FILENAME = 'url_dict_'


class Players_Scraper(object):

    # scrapes pro football reference index of letters from site
    # url: http://www.pro-football-reference.com/players/
    # output: list of letter URLs
    @staticmethod
    def get_letters():

        url = 'http://www.pro-football-reference.com/players/'
        # url request
        source = urlopen(url)
        soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")

        # get all possible last name letters from http://www.pro-football-reference.com/players/
        letters = soup.find('ul', {'class':'page_index'}).findAll('li')
        letters = [li.find('a') for li in letters]
        letters = [l.text for l in letters if l is not None]

        return letters

    # scrapes pro football reference for all player's names and gamestat urls for a given letter
    # url: http://www.pro-football-reference.com/players/{LETTER}
    # output: dict{player names : player gamelog urls}
    # * will return an empty dict if failed
    def get_player_urls(letter, sleep_time=0):

        player_urls = []
        player_names = []
        url = 'http://www.pro-football-reference.com/players/'

        # url request delay time
        time.sleep(sleep_time)

        try:
            print('- Scraping player urls:', letter)

            # open url
            source = urlopen(url + letter)
            soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")

            # find players section
            players = soup.find('div', {'class': 'section_content', 'id': 'div_players'}).findAll('a')

            # if statement probably not needed. Added as a safe guard
            new_player_names = [name.text for name in players if name is not None]

            # append new player names
            player_names += new_player_names

            # ['/players/A/AaitIs00.htm', '/players/A/AbbeJo20.htm',...
            new_player_urls = [player_url.get('href', '') for player_url in players]

            # ['AaitIs00.htm', 'AbbeJo20.htm',...
            new_player_urls = [player_url.split('/')[-1] for player_url in new_player_urls]

            # ['AaitIs00', 'AbbeJo20',...
            new_player_urls = [player_url.split('.htm')[0] for player_url in new_player_urls]

            # ['http://www.pro-football-reference.com/players/A/AaitIs00/gamelog/',...
            new_player_urls = [url + letter + '/' + player_url + '/gamelog/' for player_url in new_player_urls]

            # append new player urls
            player_urls += new_player_urls

        except:
            print('*! Scrape failed on', url + letter)
            print('Empty dict returned.')
            return dict()

        # safe test
        for player in player_names:
            if player == '' or player is None:
                print('Creating url_dict failed. One or more player names is None. Empty dict returned.')
                return dict()

        # safe test
        for player in player_urls:
            if player == '' or player is None:
                print('Creating url_dict failed. One or more player urls is None. Empty dict returned.')
                return dict()

        # safe test
        if len(player_names) == len(player_urls):
            # unique player nameS
            for i in range(len(player_names)):
                player_names[i] = player_names[i]+' (' + str(i+1) + ' of ' + str(len(player_names)) + ')'

            # create dict
            url_dict = dict(zip(player_names, player_urls))
            return url_dict
        else:
            print('Creating url_dict failed. Players to urls is not one-to-one. Empty dict returned.')
            return dict()


    # saves {Player name : URL} dictionaries for each letter (only needs to be ran once)
    # output: url_dict{}.pkl
    @staticmethod
    def create_url_dicts(letters):
        n_players = 0
        for letter in letters:
            letter_urls = Players_Scraper.get_player_urls(letter)

            if letter_urls is not dict():
                save_obj(letter_urls,DATA_DIRECTORY_NAME + URL_DICT_FILENAME+'{}'.format(letter))
                n_players += len(letter_urls)
        print('Successfully scraped',n_players,'player urls')

    # Used in scrape_player(); searches list of list for index of search string
    @staticmethod
    def indexOf(attribute_name, list_of_list):
        for i in range(len(list_of_list)):
            if list_of_list[i][0] == attribute_name:
                return i
        return -1

    # Used in scrape_player() to substitute values in list of list
    @staticmethod
    def substitute(attribute_name, list_of_list, substitution):
        index = None
        try:
            # search step
            for i in range(len(list_of_list)):
                if list_of_list[i][0] == attribute_name:
                    index = i
                    break

            # substitution step
            if index is not None:
                list_of_list[i:i+1] = substitution
            return list_of_list
        except:
            print('*! Error occurred while substituting list values')
            return list_of_list

    # scrapes player gamelog data given player's gamelog url
    # url example: http://www.pro-football-reference.com/players/G/GabbBl00/gamelog/
    # output:   SUCCESS(-): gamelog list of dictionaries
    #           NONCRITICAL ERROR(~!): return 0  -- no gamelog table exist for player
    #           CRITICAL ERROR(*!): return -1  -- player data had an unexpected format and cannot be added to database
    def scrape_player(url, sleep_time=1):
        error_message = None

        # url request delay
        time.sleep(sleep_time)

        # open url
        try:
            source = urlopen(url)
            soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
        except:
            print('~! Contacting url failed. Trying again in 3 seconds...')
            # sleep then try again
            time.sleep(3)
            try:
                source = urlopen(url)
                soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
            except:
                error_message = ('*! Failed contacting url: '+ url)
                print(error_message)
                return -1, error_message

        # skips a player if no gamelog table exists
        if soup.find('thead') is None:
            # there may be a better way to handle this
            error_message = ('~! No gamelog table exists: '+ url)
            print(error_message)
            return 0, error_message
        # Biography scrape
        try:
            # temporary variable to put soup in scope
            temp = soup.find('div', {'id': 'info'}).find('div',{'itemtype':'http://schema.org/Person'})
            if temp is None: # the itemtype argument occasionally switches from http to https
                temp = soup.find('div', {'id': 'info'}).find('div', {'itemtype': 'https://schema.org/Person'})

            # raw biography info: [Name, Position, Height, Weight...]
            bio = temp.findAll('p')

            bio = [t.text.replace(u'\xa0', ' ').replace('\n','').replace('\t','').strip() for t in bio]

            # splitting attributes and values: 'Position: WR' -> ['Position', 'WR']
            bio_data = [b.split(':') if ':' in b else b for b in bio]

            # manual manipulations for Full Name, Name, Height, and Weight
            bio_data[0] = ['Full_Name',bio_data[0]]

            # player name
            name = temp.find('h1', {'itemprop':'name'}).text
            bio_data.insert(0,['Name',name])

            # height & weight (ft/in & lbs)
            span_data =[temp.find('span',{'itemprop':'height'}),
                        temp.find('span',{'itemprop':'weight'}),
                        temp.find('span',{'itemprop':'birthPlace'})]

            # if player doesnt have a height or a weight https://www.pro-football-reference.com/players/C/CoxxMi00/gamelog/

            if span_data[0] is not None or span_data[1] is not None:
                span_data = [t.text.replace(u'\xa0', ' ').replace('\n', '').replace('\t', '').strip() for t in
                                 span_data]

                # height & weight (cm & kg)
                bio_data[3] = bio_data[3].split('lb (')[1].strip(')').split(',') # <- huge assumption may cause future issues

                # update list with Height and Weight
                bio_data[3:4] = (['Height_cm',bio_data[3][0][:-2]],['Weight_kg',bio_data[3][1][:-2]],['Height_ft/in',span_data[0]], ['Weight_lb',span_data[1][:-2]])
            else:
                span_data[-1] = span_data[-1].text.replace(u'\xa0', ' ').replace('\n', '').replace('\t', '').strip()


            # manual manipulations for QBs -- QBs have an extra stat 'Throws: Left/Right'
            if (len(bio_data[2]) == 3) and 'Throws' in bio_data[2][1]:
                bio_data[2:3] = ([bio_data[2][0], bio_data[2][1].split('Throws')[0]],['Throws',bio_data[2][2]])

            # remove empty indexes http://www.pro-football-reference.com/players/B/BrowVi00/gamelog/ Position
            bio_data = [x for x in bio_data if x != '']

            # manual manipulations for Birth Date, Age, and Location
            # html attribute name changes occasionally from bio_data-birth to data-birth
            if temp.find('span',{'itemprop':'birthDate'}) is not None: # <- for condition where player doesnt have Birthdate (very rare)
                if temp.find('span',{'itemprop':'birthDate'}).has_attr('data-birth'):
                    birth = temp.find('span',{'itemprop':'birthDate'}).get('data-birth','') # <-
                else:
                    birth = temp.find('span',{'itemprop':'birthDate'}).get('bio_data-birth','') # <-

                # update list with Birth Date and Location
                bio_data = Players_Scraper().substitute('Born',bio_data,(['Birth_Date',birth],['Location',span_data[-1].strip(' in')]))
            else: # <- condition for missing birthdate case: https://www.pro-football-reference.com/players/L/LambKe00/gamelog/
                bio_data = [['Location',bd.strip('in')] if len(bd) != 2 and 'in ' in bd else bd for bd in bio_data ]

            # manual manipulation for College
            bio_data = [['College', college[1].rpartition(' (College Stats)')[0]] if ' (College Stats)' in college[1] else college for college in bio_data]

            # manual manipulation for Weighted Career AV
            # index of Weight Career AV in list
            WCAV_i = Players_Scraper().indexOf('Weighted Career AV (100-95-...)', bio_data)

            if WCAV_i != -1:
                bio_data[WCAV_i:WCAV_i + 1] = (['Weighted_Career_AV', bio_data[WCAV_i][1].rpartition(' (')[0]],
                                                 ['Weighted_Career_Overall', bio_data[WCAV_i][1].partition('(')[-1].rpartition(' overall')[0][:-2]])
            # manual manipulations for Draft
            # index of draft in list
            draft_i = Players_Scraper().indexOf('Draft',bio_data)

            # using partition and rpartition to get the desired string between substrings
            if draft_i != -1:
                # condition for multiple drafts NFL & AFL
                if '.,' in bio_data[draft_i][1]:
                    bio_data[draft_i][1] = bio_data[draft_i][1].split('.,')[0]

                bio_data[draft_i:draft_i+1] = (['Draft_Team',bio_data[draft_i][1].rpartition(' in the')[0]],
                           ['Draft_Round',bio_data[draft_i][1].partition('in the ')[-1].rpartition(' round')[0][:-2]],
                           ['Draft_Overall',bio_data[draft_i][1].partition('(')[-1].rpartition(' overall)')[0][:-2]],
                           ['Draft_Year',bio_data[draft_i][1].partition('overall) of the ')[-1].rpartition(' NFL Draft.')[0]])


            # manual manipulations for Hall of Fame
            # index of Hall of Fame in list
            HOF_i = Players_Scraper().indexOf('Hall of Fame', bio_data)
            if HOF_i != -1:
                bio_data[HOF_i] = ['Hall_of_Fame_Year',bio_data[HOF_i][1].partition('Player in ')[-1].rpartition('(')[0]]

            # removing whitespaces
            bio_data = [list(map(lambda x: re.sub(' +',' ',x).strip(), sublist)) for sublist in bio_data]


        except Exception as ex:
            template = "An exception of type {0} occurred. \nArguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            error_message = ('*! Critical Error: on player biography '+ str(message)+' '+ url)
            print(error_message)
            return -1, error_message

        # Gamelog table scrape
        try:
            tables = soup.find('div', ({'id': 'content', 'role':'main'})).findAll('table',{'class':'row_summable'})

            # check if game log table exist
            if len(tables) < 1:
                error_message = ('~! No gamelog table exists: '+ url)
                print(error_message)
                return 0, error_message

            # all game log data will be stored in this list
            all_game_data = []

            # loop through Regular (and Playoffs) season tables
            for table in tables:

                season_name = table.find('caption').text.split()[0]

                # skip tables that aren't gamelogs
                if season_name != 'Regular' and season_name != 'Playoffs':
                    continue

                tr = table.find('tr', {'class' : None})

                # pulls raw data from table
                game_data = table.find('tbody').findAll('tr')

                # pulls over table header names
                over_header = [th.get('data-over-header', '') for th in tr.findAll('th')][1:]

                # pull all table header names
                table_headers = [d.get('data-stat','') for d in game_data[0].findAll('td')]

                # safe test for table header == over header
                if len(table_headers) != len(over_header):
                    error_message = ('*! Critical Error: table headers != over headers ' + url)
                    print(error_message)
                    return -1, error_message

                # combines over headers and table headers
                table_headers = ['rank']+[over_header[i]+'-'+table_headers[i] for i in range(len(over_header))]
                table_headers = [th[1:] if th[0] == '-' else th for th in table_headers]

                # safe test for table header name overlap
                if (len(table_headers) != len(set(table_headers))):
                    print('*! Critical Error: 2 or more table headers contain the same name ', url)
                    # output repeated header name
                    for i in range(len(table_headers)):
                        for j in range(len(table_headers)):
                            if table_headers[i] == table_headers[j] and i != j:
                                error_message = '*! 2 or more occurances of header: ' + str(table_headers[i])
                                return -1, error_message

                # cleans up raw data from table
                game_data = [([tr.find('th').text] + [td.text for td in tr.findAll('td')])
                             for tr in game_data if [tr.find('th').text] != ['Rk']] # <-

                # safe test for data to column header mismatch
                if max([len(gd) for gd in game_data]) != len(table_headers):
                    error_message = ('*! Critical Error: game data != table headers', url)
                    print(error_message)
                    print('len(game_data[0]) =',len(game_data[0]),' len(table_headers) =',len(table_headers))
                    return -1, error_message

                # # format player age column
                age_i = table_headers.index('age') if 'age' in table_headers else -1
                if age_i != -1:
                    # insert new columns into table headers
                    table_headers[age_i:age_i + 1] = ('age','age_years', 'age_days') # <- not sure if all players have age_days
                    # insert split data into game data
                    for game in game_data:
                        if len(game[age_i].split('-')) == 2:
                            game[age_i:age_i + 1] = (game[age_i], game[age_i].split('-')[0], game[age_i].split('-')[1])
                        else:
                            game[age_i:age_i + 1] = (game[age_i], '', '')


                # # format Home/Away column
                hw_i = table_headers.index('game_location') if 'game_location' in table_headers else -1
                if hw_i != -1:
                    for game in game_data:
                        if game[hw_i] == '':
                            game[hw_i] = 'Home'
                        else:
                            game[hw_i] = 'Away'

                # # format game_result column: 'W 24-10' -> 'W', '24', '10'
                result_i = table_headers.index('game_result') if 'game_result' in table_headers else -1
                if result_i != -1 and len(game_data[0][result_i].split()) == 2 and len(game[result_i].split()[1].split('-')) == 2: # <- possible issue
                    # insert new columns into table headers
                    table_headers[result_i:result_i+1] = ('result','team_score','opp_team_score')
                    # insert split data into game data
                    for game in game_data:
                        game[result_i:result_i+1] = (game[result_i].split()[0], game[result_i].split()[1].split('-')[0], game[result_i].split()[1].split('-')[1])

                # insert season header and data
                table_headers = ['season'] + table_headers
                game_data = [[season_name]+gd for gd in game_data]

                # create [table header, data] tuple
                for game in game_data:
                    for i in range(len(game)):
                        game[i] = [table_headers[i], game[i]]

                # append to list of list, each game_data variable represents a row in the table
                all_game_data = all_game_data + game_data

            # skip if no game log table existed
            if len(all_game_data) == 0:
                error_message = ('~! No gamelog table exists: '+ url)
                print(error_message)
                return 0, error_message

            # append player bio info to each row
            all_game_data = [bio_data + agd for agd in all_game_data]

            # convert to list of dictionaries
            all_game_data = [dict(agd) for agd in all_game_data]

            return all_game_data, error_message

        except Exception as ex:
            template = "An exception of type {0} occurred. \nArguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            error_message = ('*! Critical Error: on gamelog table '+ str(message)+' '+ url)
            print(error_message)
            return -1, error_message


    # use this version of player scrape when multiprocessing
    # args = [letters to scrape, number of players to scrape per letter]
    @staticmethod
    def scrape_player_multiprocessing(num_players, letter):

        # gets player urls for a particular letter
        letter_urls = load_obj(DATA_DIRECTORY_NAME + URL_DICT_FILENAME +'{}'.format(letter))

        non_critical_errors = []
        critical_errors = []
        df = pd.DataFrame()
        print('- Scraping players with lastname:',letter)

        # for i in range(len(letter_urls)):
        # debugging range
        ran = min(len(letter_urls), num_players)
        for i in range(ran):
            print('-', list(letter_urls.keys())[i], ':', list(letter_urls.values())[i])
            new_data, error_message = Players_Scraper.scrape_player(list(letter_urls.values())[i])


            if new_data != 0 and new_data != -1:
                df = df.append(new_data)
            elif new_data == 0:
                non_critical_errors.append([list(letter_urls.keys())[i], list(letter_urls.values())[i], error_message])
            elif new_data == -1:
                critical_errors.append([list(letter_urls.keys())[i], list(letter_urls.values())[i], error_message])

        df.to_csv(DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME+'{}.csv'.format(letter),index=False)
        return critical_errors



# combines all dataframes  Player_Data_A.csv, Player_Data_B.csv... Player_Data_Z.csv -> Player_Data_All.csv
# saves combined df to a csv and returns df
def combine_dataframes():

    # remove old file if exist
    try:
        os.remove(DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME + 'All' + '.csv', dir_fd=None)
    except OSError:
        pass
        #print('Error occured while removing file',DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv')

    # list of all Player Data csv filenames in directory
    csv_files = [i for i in glob.glob(DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME + '?.csv')]

    if len(csv_files) == 0:
        print('Error: No files with naming format', PLAYER_DATA_FILENAME + '?.csv',
              'exist in',os.getcwd()+ DATA_DIRECTORY_NAME)
        return

    # dataframe for all players data
    df = pd.DataFrame()

    # merge all dfs read from csv
    for csv in csv_files:
        # skips player data csv if empty
        if (os.stat(os.getcwd() +'/'+ csv).st_size == 0) or (os.stat(os.getcwd() +'/'+ csv).st_size == 1):
            print('- Skipped:', csv)
            continue

        print('- Appending:',csv)
        df = df.append(pd.read_csv(csv,low_memory=False), ignore_index=True)
    df = df.reset_index(drop=True)

    # save merged df to csv
    df.to_csv(DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME + 'All' + '.csv',index=False)
    print('-',PLAYER_DATA_FILENAME + 'All' + '.csv','saved.')
    #df.set_index('Name', inplace=True)
    return df

# performs various checks on dataset
def data_checks():
    # read in Player_Data_All.csv to dataframe
    try:
        df = pd.read_csv(DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME + 'All' + '.csv', low_memory=False)
    except:
        print('*! File not found',DATA_DIRECTORY_NAME+PLAYER_DATA_FILENAME + 'All' + '.csv')
        return

    # Checks None/NaN values
    if not (df.loc[df['Name'].isnull()]['Full_Name'].empty and \
            df.loc[df['Full_Name'].isnull()]['Name'].empty and \
            df.loc[df['Weighted_Career_AV'].isnull()]['Name'].empty and \
            df.loc[df['season'].isnull()]['Name'].empty and \
            df.loc[df['Weighted_Career_Overall'].isnull()]['Name'].empty and\
            df.loc[df['game_date'].isnull()]['Name'].empty and\
            df.loc[df['game_location'].isnull()]['Name'].empty and\
            df.loc[df['game_num'].isnull()]['Name'].empty and\
            df.loc[df['opp'].isnull()]['Name'].empty and\
            df.loc[df['opp_team_score'].isnull()]['Name'].empty and\
            df.loc[df['rank'].isnull()]['Name'].empty and\
            df.loc[df['result'].isnull()]['Name'].empty and\
            df.loc[df['team_score'].isnull()]['Name'].empty and\
            df.loc[df['year_id'].isnull()]['Name'].empty):

        print('Name:', df.loc[df['Name'].isnull()]['Full_Name'])
        print('Full_Name:', df.loc[df['Full_Name'].isnull()]['Name'])
        # print('Birth_date:',df.loc[df['Birth_Date'].isnull()]['Name']) # <- George Fields, Bob Garner, Keenan Lambert
        #print('Height_cm:', df.loc[df['Height_cm'].isnull()]['Name']) # http://www.pro-football-reference.com/players/C/CoxxMi00/gamelog/
        #print('Height_ft/in:', df.loc[df['Height_ft/in'].isnull()]['Name'])  # http://www.pro-football-reference.com/players/C/CoxxMi00/gamelog/
        #print('Weight_kg:', df.loc[df['Weight_kg'].isnull()]['Name']) # http://www.pro-football-reference.com/players/C/CoxxMi00/gamelog/
        #print('Weight_lb:', df.loc[df['Weight_lb'].isnull()]['Name']) # http://www.pro-football-reference.com/players/C/CoxxMi00/gamelog/
        #print('Position:', df.loc[df['Position'].isnull()]['Name']) # http://www.pro-football-reference.com/players/B/BrowVi00/gamelog/
        print('Weighted_Career_AV:',df.loc[df['Weighted_Career_AV'].isnull()]['Name'])  # <- should work fine now
        # print('age:',df.loc[df['age'].isnull()]['Name']) # <- George Fields, Bob Garner, Keenan Lambert
        print('season:', df.loc[df['season'].isnull()]['Name'])
        print('WCO:', df.loc[df['Weighted_Career_Overall'].isnull()]['Name'])
        print('game_date:', df.loc[df['game_date'].isnull()]['Name'])
        print('game_location:', df.loc[df['game_location'].isnull()]['Name'])
        print('game_num:', df.loc[df['game_num'].isnull()]['Name'])
        print('opp:', df.loc[df['opp'].isnull()]['Name'])
        print('opp_team_score', df.loc[df['opp_team_score'].isnull()]['Name'])
        print('rank', df.loc[df['rank'].isnull()]['Name'])
        print('result', df.loc[df['result'].isnull()]['Name'])
        print('team_score', df.loc[df['team_score'].isnull()]['Name'])
        print('year_id', df.loc[df['year_id'].isnull()]['Name'])

        print('*! Critical Error in dataset: empty columns exist where they should not')
        return -1

    # checks for data inconsistencies
    if not df.loc[(df['opp_team_score'] > df['team_score']) & (df['result'] == 'W')]['Name'].empty and \
            df.loc[(df['opp_team_score'] < df['team_score']) & (df['result'] == 'L')]['Name'].empty and \
            df.loc[(df['opp_team_score'] == df['team_score']) & (df['result'] != 'T')]['Name'].empty and \
            df.loc[(df['result'] != 'W') & (df['result'] != 'T') & (df['result'] != 'L')]['Name'].empty:

        # all should be empty
        print(df.loc[(df['opp_team_score'] > df['team_score']) & (df['result'] == 'W')]['Name'])
        print(df.loc[(df['opp_team_score'] < df['team_score']) & (df['result'] == 'L')]['Name'])
        print(df.loc[(df['opp_team_score'] == df['team_score']) & (df['result'] != 'T')]['Name'])
        print(df.loc[(df['result'] != 'W') & (df['result'] != 'T') & (df['result'] != 'L')]['Name'])

        print('*! Critical Error in dataset: game stat W/L/T results are inconsistent')
        return -1


    # range statistics
    print('Tallest Player:',df.iloc[df['Height_cm'].idxmax()]['Name'],'at',df.iloc[df['Height_cm'].idxmax()]['Height_cm'],'cm,',df.iloc[df['Height_cm'].idxmax()]['Height_ft/in'])
    print('Shortest Player:',df.iloc[df['Height_cm'].idxmin()]['Name'],'at',df.iloc[df['Height_cm'].idxmin()]['Height_cm'],'cm,',df.iloc[df['Height_cm'].idxmin()]['Height_ft/in'])

    print('Heaviest Player:',df.iloc[df['Weight_kg'].idxmax()]['Name'],'at',df.iloc[df['Weight_kg'].idxmax()]['Weight_kg'],'kg,',df.iloc[df['Weight_kg'].idxmax()]['Weight_lb'],'lb')
    print('Lightest Player:',df.iloc[df['Weight_kg'].idxmin()]['Name'],'at',df.iloc[df['Weight_kg'].idxmin()]['Weight_kg'],'kg,',df.iloc[df['Weight_kg'].idxmin()]['Weight_lb'],'lb')

    print('Oldest Player(playing in a game, year only):',df.iloc[df['age_years'].idxmax()]['Name'],'at',df.iloc[df['age_years'].idxmax()]['age_years'],'years old')
    print('Youngest Player(playing in a game, year only):',df.iloc[df['age_years'].idxmin()]['Name'],'at',df.iloc[df['age_years'].idxmin()]['age_years'],'years old')

    return 1

# saving pickles
def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

# loading pickles
def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def console_input():
    # inputs: letters to scrape, number of players to scrape in each letter
    letters = []
    num_players = 0
    if input('Console: Press Enter to select a range of letters or Enter s to select specific letters to scrape\nUser: ') == 's':
        letters = [x.upper() for x in input('Console: Enter all letters to scrape separated by a space (no commas)\nUser: ').split()]
    else:
        start_str = input('Console: Enter starting letter\nUser: ').upper()
        end_str = input('Console: Enter ending letter\nUser: ').upper()
        current_str= start_str
        while current_str <= end_str:
            letters.append(current_str)
            current_str = chr(ord(current_str) + 1)

    # check if letter is valid; ex: X
    all_letters = Players_Scraper().get_letters()
    letters = [l for l in letters if l in all_letters]

    if input('Console: Press Enter to scrape all players for each letter or Enter s to select number of players to scrape\nUser: ') == 's':
        num_players = input('Console: Input number of players\nUser: ')
    else:
        num_players = sys.maxsize

    confirmation = ''
    while confirmation != 'n' and confirmation != 'y':
        if num_players == sys.maxsize:
            confirmation = input('Console: You want to scrape all players for letters [' + ', '.join(str(l) for l in letters) +']? (y/n)\nUser: ')
        else:
            confirmation = input('Console: You want to scrape the first '+ str(num_players)+' players for letters [' + ', '.join(str(l) for l in letters) +']? (y/n)\nUser: ')

        if confirmation == 'y':
            break
        elif confirmation == 'n':
            num_players, letters = console_input()

    for letter in letters:
        # check letter
        if (not letter.isalpha()) or (not (len(letter) == 1)):
            print('Error: Letters must be one character long and a letter')
            num_players, letters = console_input()

    # check num_players
    if str(num_players).isdigit():
        num_players = int(num_players)
    else:
        print('Error: Number of players must be an integer')
        num_players, letters = console_input()
        
    return num_players, letters

def separate_postions(): # work in-progress
    # read in Player_Data_All.csv to dataframe
    '''
    try:
        df = pd.read_csv(DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv', low_memory=False)
    except:
        print('*! File not found', DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv')
        return
    '''

    df = load_obj('All_Players')
    print(df)


###########################################
##             main function             ##
###########################################
if __name__ == '__main__':

    # makes a directory for all saved data files if path does not already exist
    if not os.path.exists(os.getcwd() + '/' + DATA_DIRECTORY_NAME):
        print('Creating folder', '/' + DATA_DIRECTORY_NAME)
        os.makedirs(os.getcwd() + '/' + DATA_DIRECTORY_NAME)

    # makes a directory for all errors if path does not already exist
    if not os.path.exists(os.getcwd() + '/' + ERROR_LOG_DIRECTORY_NAME):
        print('Creating folder', '/' + ERROR_LOG_DIRECTORY_NAME)
        os.makedirs(os.getcwd() + '/' + ERROR_LOG_DIRECTORY_NAME)

    # quickly gets all of the last name letters
    letters = Players_Scraper.get_letters()

    # if len([url_dict_A.pkl, url_dict_B.pkl, url_dict_C.pkl,...]) != len([A, B, C, ...])
    if len(glob.glob(DATA_DIRECTORY_NAME+URL_DICT_FILENAME + '?.pkl')) != len(letters):
        # then create all url_dicts and save them to file directory
        Players_Scraper.create_url_dicts(letters)

    # multiprocessing main function
    critical_errors = []
    pool = multiprocessing.Pool(4) # between 1 and 5 works best, +6 results in a lot of 404 & 503 Service errors
    # uses multiprocessing to complete n number of jobs at once
    # func saves result to csv files located in player data directory
    # func returns all critical errors that occured while scraping

    # inputs: letters to scrape, number of players to scrape in each letter
    n_players, letters = console_input()

    func = partial(Players_Scraper().scrape_player_multiprocessing, n_players)
    critical_errors = pool.map(func, letters)

    # save critical errors to file
    for i in range(len(letters)):
        if len(critical_errors[i]) != 0:
            print('*! Saved', len(critical_errors[i]),'Critical Error(s) to file:', ERROR_LOG_DIRECTORY_NAME+'critical_errors_'+ letters[i]+ '_' + str(datetime.strftime(datetime.now(), '%Y/%m/%d_%H:%M:%S')))
            # save all errors to file
            save_obj(critical_errors[i], ERROR_LOG_DIRECTORY_NAME+'critical_errors_'+ letters[i]+ '_' + str(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')))

    # print out critical errors
    for i in range(len(letters)):
        for ce in critical_errors[i]:
            print(ce)

    # creates combined dataframe csv file: Player_Data_All.csv
    #combine_dataframes()

    # check data
    #data_checks()

    # separate positions
    #separate_postions()


