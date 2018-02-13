# http://www.nfl.com/player/
import bs4 as bs
from urllib.request import urlopen
import time
from datetime import datetime
import pickle
import os
import string
import sys
import re
import multiprocessing
import os
import glob
import pandas as pd
from functools import partial

global DATA_DIRECTORY_NAME

# directory and file name constants
DATA_DIRECTORY_NAME = 'NFL Player Scraper Data/'
ERROR_LOG_DIRECTORY_NAME = 'NFL_errorlog/'
PLAYER_DATA_FILENAME = 'NFL_Player_Data_'
URL_DICT_FILENAME = 'NFL_url_dict_'

class NFL_Scraper(object):

    # scrapes NFL.com for all player's names and gamelog urls for a given letter
    # output: dict{player names : player gamelog urls}
    # * will return an empty dict if failed
    def get_player_urls(letter, sleep_time=1):

        player_urls = []
        player_names = []

        player_type = ['current','historical']
        page_numbers = list(range(1,9999999)) # <- Needs to have a high ceiling

        for pt in player_type:
            for page_num in page_numbers:
                url = 'http://www.nfl.com/players/search?category=lastName&playerType='+pt+'&d-447263-p='+str(page_num)+'&filter='+letter

                # url request delay time
                time.sleep(sleep_time)

                try:
                    print('- Scraping NFL.com player urls: '+ letter + ' page '+str(page_num) + ' ('+pt+')')

                    # open url
                    source = urlopen(url)
                    soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")

                    # find players section
                    table = soup.find('table', {'class': 'data-table1', 'id': 'result'})

                    # No players found, url with page_num doesnt exist
                    if table is None:
                        break

                    table = table.findAll('tbody')[-1]

                    players = table.findAll('tr')
                    players = [p.find('a') for p in players]

                    player_names = player_names + [p.text for p in players]
                    player_urls = player_urls + [p.get('href',None) for p in players]

                except:
                    print('*! Scrape failed on', url)
                    print('Empty dict returned.')
                    return dict()

        # safe test
        if None in player_urls or None in player_names:
            print('*! None value found on page', url)
            print('Empty dict returned.')
            return dict()

        # unique player names
        for i in range(len(player_names)):
            player_names[i] = player_names[i] + ' (' + str(i + 1) + ' of ' + str(len(player_names)) + ')'

        # format urls
        player_urls = ['http://www.nfl.com'+pURL.replace('profile','gamelogs') for pURL in player_urls]

        # create dict
        url_dict = dict(zip(player_names, player_urls))
        return url_dict

    # saves {Player name : URL} dictionaries for each letter (only needs to be ran once)
    # output: url_dict{}.pkl
    @staticmethod
    def create_url_dicts():
        n_players = 0
        letters = list(string.ascii_uppercase)
        for letter in letters:
            letter_urls = NFL_Scraper.get_player_urls(letter)

            if letter_urls is not dict():
                save_obj(letter_urls, DATA_DIRECTORY_NAME + URL_DICT_FILENAME + '{}'.format(letter))
                n_players += len(letter_urls)
            else: # handle NFL.com timeouts (rare)
                print('~! Waiting 5 seconds then trying', letter,'again...')
                time.sleep(5)
                letter_urls = NFL_Scraper.get_player_urls(letter,sleep_time=3)
                if letter_urls is not dict():
                    save_obj(letter_urls, DATA_DIRECTORY_NAME + URL_DICT_FILENAME + '{}'.format(letter))
                    n_players += len(letter_urls)
                else:
                    print('*! URL Scrape failed on letter',letter)
        print('Successfully scraped', n_players, 'player urls')

    def scrape_combine(url, sleep_time=1):
        error_message =  ''
        # combine data
        try:
            # url request delay
            time.sleep(sleep_time)

            # format url
            combine_url = url.replace('gamelogs', 'combine')

            # open url
            try:
                source = urlopen(combine_url)
                soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
            except:
                print('~! Contacting combine url failed. Trying again in 3 seconds...')
                # sleep then try again
                time.sleep(3)
                try:
                    source = urlopen(combine_url)
                    soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
                except:
                    error_message = ('*! Failed contacting combine url: ' + combine_url)
                    print(error_message)
                    return -1, error_message

            combine_stats = soup.find('div', {'id': 'combine-stats'})

            # safe test
            if combine_stats is None:
                error_message = ('~! No combine stat for player ' + combine_url)
                print(error_message)
                return -1, error_message

            combine_stats = combine_stats.findAll('li')

            # combine data tuple
            combine_stats = [[cs.div.find(text=True, recursive=False)+'_NFL.com', cs.find('span').text] for cs in combine_stats]

            return combine_stats, error_message

        except Exception as ex:
            template = "An exception of type {0} occurred. \nArguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            error_message = ('*! Critical Error: on combine data ' + str(message) + ' ' + combine_url)
            print(error_message)
            return -1, error_message

    # scrapes player gamelog data given player's gamelog url
    # url example: http://www.pro-football-reference.com/players/G/GabbBl00/gamelog/
    # output:   SUCCESS(-): gamelog list of dictionaries
    #           NONCRITICAL ERROR(~!): return 0  -- no gamelog table exist for player
    #           CRITICAL ERROR(*!): return -1  -- player data had an unexpected format and cannot be added to database
    def scrape_player(url, sleep_time=1):

        # all game log data will be stored in this list
        all_game_data = []

        error_message = ''

        # url request delay
        time.sleep(sleep_time)

        # open url
        try:
            source = urlopen(url)
            soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
        except:
            print('~! Contacting initial gamelog url failed. Trying again in 3 seconds...')
            # sleep then try again
            time.sleep(3)
            try:
                source = urlopen(url)
                soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
            except:
                error_message = ('*! Failed contacting initial gamelog url: ' + url)
                print(error_message)
                return -1, error_message

        # skips a player if no gamelog table exists
        if soup.find('thead') is None:
            # there may be a better way to handle this
            error_message = ('~! No gamelog table exists: ' + url)
            print(error_message)
            return 0, error_message


        # Biography scrape
        try:
            # temporary variable to put soup in scope
            bio = soup.find('div', {'class': 'player-info'}).findAll('p')
            # formatting -- useless characters
            bio = [b.text.replace(u'\xa0', ' ').replace('\n','').replace('\t','').replace('\r','').strip() for b in bio]
            # format Detroit Lions| Official Team Site -> Detroit Lions
            bio = [b.split('|')[0] if '|' in b else b for b in bio]

            # format team -- Assumes team name is always 2nd entry
            if 'Height' not in bio[1] and 'Weight' not in bio[1]:
                bio[1] = ['Team',bio[1]]

            # format Jared Abbrederis  #10 WR -> [Jared Abbrederis,  #10, WR]
            if '  #' in bio[0]:
                bio[0:1] = ('Name:'+bio[0]).split('  #')
                if ' ' in bio[1]:
                    if str(bio[1][0]).isdigit():
                        bio[1:2] = (['Number',bio[1].split()[0]],['Position',bio[1].split()[-1]])
                    else:
                        bio[1] = (['Position',bio[1].split()[-1]])

            else: # must check for players with positions and #'s
                bio[0] = 'Name:'+bio[0]

            # format 'Height: 6-1   Weight: 195   Age: 26' -> 'Height: 6-1',   'Weight: 195',   'Age: 26'
            for i in range(len(bio)):
                if 'Height' in bio[i] and 'Weight' in bio[i]: # No need to include age in condition

                    if 'Deceased' in bio[i]:
                        bio[i] = bio[i].strip(' Deceased')
                        bio.insert(i+1,'Deceased:True')
                    else:
                        bio.insert(i+1,'Deceased:False')


                    bio[i:i + 1] = bio[i].split('   ')
                    break

            # separate :
            bio = [b.split(':') if isinstance(b, str) else b for b in bio]

            # append column name
            bio = [[b[0]+'_NFL.com',b[1]] for b in bio]

            # removing whitespaces
            bio = [list(map(lambda x: re.sub(' +', ' ', x).strip(), sublist)) for sublist in bio]


        except Exception as ex:
            template = "An exception of type {0} occurred. \nArguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            error_message = ('*! Critical Error: on player biography ' + str(message) + ' ' + url)
            print(error_message)
            return -1, error_message

        # Gamelog table scrape
        try:
            years = soup.find('select',{'id':'season','class':'teamroster'})

            if years is not None:
                years = years.findAll('option')
                years = [y.text for y in years]
            else:
                error_message = ('*! Critical Error: year dropdown not found ' + url)
                print(error_message)
                return -1, error_message

            # scrape gamelog table for each year
            for year in years:

                # url request delay
                time.sleep(sleep_time+2)

                # open url
                try:
                    source = urlopen(url + '?season='+year)
                    soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
                except:
                    print('~! Contacting a gamelog page url failed. Trying again in 3 seconds...')
                    # sleep then try again
                    time.sleep(3)
                    try:
                        source = urlopen(url)
                        soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
                    except:
                        print('~! Contacting a gamelog page url failed. Trying again in 5 seconds...')
                        # sleep then try again
                        time.sleep(5)
                        try:
                            source = urlopen(url)
                            soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
                        except:
                            print('~! Contacting a gamelog page url failed. Trying again in 10 seconds...')
                            # sleep then try again
                            time.sleep(10)
                            try:
                                source = urlopen(url)
                                soup = bs.BeautifulSoup(source, "lxml", from_encoding="utf-8")
                            except:
                                error_message = ('*! Failed contacting a gamelog url: ' + url)
                                print(error_message)
                                return -1, error_message


                tables = soup.findAll('table', {'class': 'data-table1'})
                for table in tables:
                    season_name = table.find('td').text

                    # column header names
                    column_names = table.find('thead').find('tr',{'class':'player-table-key'})
                    column_names = column_names.findAll('td')
                    column_names = [cn.text for cn in column_names] + ['Season']

                    # table row data
                    row_data = table.find('tbody').findAll('tr')
                    row_data = [rd.findAll('td') for rd in row_data]

                    # handle missing row elements due to colspan argument: BYE weeks and TOTALs
                    for rd in row_data:
                        for i in range(len(rd)):
                            if rd[i].get('colspan','') != '' and rd[i].get('class','') == '':
                                replacement_td = bs.BeautifulSoup('<td>'+rd[i].text+'</td>', "lxml").find('td')
                                colspan = int(rd[i].get('colspan',1)) - 1

                                # replace colspan element
                                rd[i] = replacement_td
                                # insert appropriate number of row elements according to colspan
                                for j in range(colspan):
                                    rd.insert(i+1, replacement_td)


                    row_data = [[d.text.replace(u'\xa0', ' ').replace('\n','').replace('\t','').replace('\r','').strip() for d in rd] + [season_name] for rd in row_data]

                    # remove blank rows
                    row_data = [rd for rd in row_data if len(rd) > 2]

                    # safe test, max len of all row_data rows
                    if max([len(rd) for rd in row_data]) != len(column_names):
                        error_message = ('*! Critical Error: game data != table headers', url)
                        print(error_message)
                        print('len(game_data[0]) =', len(row_data[0]), ' len(table_headers) =', len(column_names))
                        return -1, error_message

                    # last row is a list of season totals
                    del row_data[-1]

                    # game date year formatting
                    for rd in row_data:
                        for i in range(len(rd)):
                            if column_names[i] == 'Game Date' and 'Bye' not in rd[i]:
                                # if month is greater than 6, prepend year; else prepend year +1
                                if int(rd[i][0:2]) > 6:
                                    rd[i] = str(year)+'/'+ rd[i]
                                else:
                                    rd[i] = str(int(year)+1)+'/'+ rd[i]
                                # format
                                rd[i] = rd[i].replace('/','-')


                    # prepend column name to data; ['2016-01-10'] -> ['Game Date_NFL.com', '2016-01-10']
                    for rd in row_data:
                        for i in range(len(rd)):
                            rd[i] = [column_names[i]+'_NFL.com',rd[i]]

                    # safe test
                    if max([len(rd) for rd in row_data]) != min([len(rd) for rd in row_data]):
                        error_message = ('*! Critical Error: rows uneven', url)
                        print(error_message)
                        print('Row Lengths:',[len(rd) for rd in row_data])
                        return -1, error_message

                    # concat new row data
                    all_game_data += row_data

        except Exception as ex:
            template = "An exception of type {0} occurred. \nArguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            error_message = ('*! Critical Error: on gamelog table ' + str(message) + ' ' + url)
            print(error_message)
            return -1, error_message

        # combine data
        combine_data, combine_error_message = NFL_Scraper.scrape_combine(url,sleep_time)

        # if not error
        if combine_data != 0 and combine_data != -1:
            bio += combine_data
        else:
            error_message = error_message + '\nCombine: '+combine_error_message

        # append player bio info to each row
        all_game_data = [bio + agd for agd in all_game_data]

        # convert to list of dictionaries
        all_game_data = [dict(agd) for agd in all_game_data]

        return all_game_data, error_message

        # use this version of player scrape when multiprocessing
        # args = [letters to scrape, number of players to scrape per letter]

    @staticmethod
    def scrape_player_multiprocessing(num_players, letter):

        # gets player urls for a particular letter
        letter_urls = load_obj(DATA_DIRECTORY_NAME + URL_DICT_FILENAME +'{}'.format(letter))

        non_critical_errors = []
        critical_errors = []
        df = pd.DataFrame()
        print('- Scraping players with lastname:', letter)

        # for i in range(len(letter_urls)):
        # debugging range
        ran = min(len(letter_urls), num_players)
        for i in range(ran):
            print('-', list(letter_urls.keys())[i], ':', list(letter_urls.values())[i])
            new_data, error_message = NFL_Scraper.scrape_player(list(letter_urls.values())[i])

            if new_data != 0 and new_data != -1:
                df = df.append(new_data)
            elif new_data == 0:
                non_critical_errors.append([list(letter_urls.keys())[i], list(letter_urls.values())[i], error_message])
            elif new_data == -1:
                critical_errors.append([list(letter_urls.keys())[i], list(letter_urls.values())[i], error_message])

        df.to_csv(DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + '{}.csv'.format(letter), index=False)
        return critical_errors

# combines all dataframes  Player_Data_A.csv, Player_Data_B.csv... Player_Data_Z.csv -> Player_Data_All.csv
# saves combined df to a csv and returns df
def combine_dataframes():

    # remove old file if exist
    try:
        os.remove(DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv', dir_fd=None)
    except OSError:
        pass
        # print('Error occured while removing file',DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv')

    # list of all Player Data csv filenames in directory
    csv_files = [i for i in glob.glob(DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + '?.csv')]

    if len(csv_files) == 0:
        print('Error: No files with naming format', PLAYER_DATA_FILENAME + '?.csv',
              'exist in', os.getcwd() + DATA_DIRECTORY_NAME)
        return

    # dataframe for all players data
    df = pd.DataFrame()

    # merge all dfs read from csv
    for csv in csv_files:
        # skips player data csv if empty
        if (os.stat(os.getcwd() + '/' + csv).st_size == 0) or (os.stat(os.getcwd() + '/' + csv).st_size == 1):
            print('- Skipped:', csv)
            continue

        print('- Appending:', csv)
        df = df.append(pd.read_csv(csv, low_memory=False), ignore_index=True)
    df = df.reset_index(drop=True)

    # save merged df to csv
    df.to_csv(DATA_DIRECTORY_NAME + PLAYER_DATA_FILENAME + 'All' + '.csv', index=False)
    print('-', PLAYER_DATA_FILENAME + 'All' + '.csv', 'saved.')
    # df.set_index('Name', inplace=True)
    return df


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
    if input(
            'Console: Press Enter to select a range of letters or Enter s to select specific letters to scrape\nUser: ') == 's':
        letters = [x.upper() for x in
                   input('Console: Enter all letters to scrape separated by a space (no commas)\nUser: ').split()]
    else:
        start_str = input('Console: Enter starting letter\nUser: ').upper()
        end_str = input('Console: Enter ending letter\nUser: ').upper()
        current_str = start_str
        while current_str <= end_str:
            letters.append(current_str)
            current_str = chr(ord(current_str) + 1)


    if input(
            'Console: Press Enter to scrape all players for each letter or Enter s to select number of players to scrape\nUser: ') == 's':
        num_players = input('Console: Input number of players\nUser: ')
    else:
        num_players = sys.maxsize

    confirmation = ''
    while confirmation != 'n' and confirmation != 'y':
        if num_players == sys.maxsize:
            confirmation = input('Console: You want to scrape all players for letters [' + ', '.join(
                str(l) for l in letters) + ']? (y/n)\nUser: ')
        else:
            confirmation = input(
                'Console: You want to scrape the first ' + str(num_players) + ' players for letters [' + ', '.join(
                    str(l) for l in letters) + ']? (y/n)\nUser: ')

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

    # if len([url_dict_A.pkl, url_dict_B.pkl, url_dict_C.pkl,...]) != len([A, B, C, ...])
    if len(glob.glob(DATA_DIRECTORY_NAME + URL_DICT_FILENAME + '?.pkl')) != 26:
        # then create all url_dicts and save them to file directory
        NFL_Scraper.create_url_dicts()

    # quick testing
    #NFL_Scraper.scrape_player('http://www.nfl.com/player/mehdiabdesmad/2556718/gamelogs')
    #NFL_Scraper.scrape_player('http://www.nfl.com/player/jameseaddy/2513393/gamelogs')
    #NFL_Scraper.scrape_player('http://www.nfl.com/player/bruceradford/2523679/gamelogs')

    # multiprocessing main function
    critical_errors = []
    pool = multiprocessing.Pool(4)  # between 1 and 5 works best, +6 results in a lot of 404 & 503 Service errors
    # uses multiprocessing to complete n number of jobs at once
    # func saves result to csv files located in player data directory
    # func returns all critical errors that occured while scraping

    # inputs: letters to scrape, number of players to scrape in each letter
    n_players, letters = console_input()

    func = partial(NFL_Scraper().scrape_player_multiprocessing, n_players)
    critical_errors = pool.map(func, letters)

    # save critical errors to file
    for i in range(len(letters)):
        if len(critical_errors[i]) != 0:
            print('*! Saved', len(critical_errors[i]), 'Critical Error(s) to file:',
                  ERROR_LOG_DIRECTORY_NAME + 'critical_errors_' + letters[i] + '_' + str(
                      datetime.strftime(datetime.now(), '%Y/%m/%d_%H:%M:%S')))
            # save all errors to file
            save_obj(critical_errors[i], ERROR_LOG_DIRECTORY_NAME + 'critical_errors_' + letters[i] + '_' + str(
                datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')))

    # print out critical errors
    for i in range(len(letters)):
        for ce in critical_errors[i]:
            print(ce)

    # creates combined dataframe csv file: Player_Data_All.csv
    combine_dataframes()

