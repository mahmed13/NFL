import bs4 as bs
from urllib.request import urlopen
import pandas as pd
import time
import re
import numpy as np
import itertools
import multiprocessing
from functools import partial


def scrape_one_week_box_scores(year, sleep_time, week):
    core_url = 'http://www.pro-football-reference.com/'

    year_and_week_box_score_url = 'years/{0}/week_{1}.htm'.format(year, week)

    ### Extracting url for box scores for all games in one week ###

    # url request for page containing links to all box scores for a single week
    week_box_scores_request = urlopen(core_url + year_and_week_box_score_url)

    week_box_scores_html = bs.BeautifulSoup(week_box_scores_request, "lxml", from_encoding="utf-8")

    # Grabs all box scores for a week
    box_scores = week_box_scores_html.findAll('td', attrs={'class': 'right gamelink'})

    no_games_this_week = False

    if len(box_scores) == 0:
        print('No games for week {0} in {1}'.format(week, year))
        no_games_this_week = True

    # Scrape and process webpage for each game in a week
    for i in range(len(box_scores)):

        if no_games_this_week == True:
            i = 1  # Used to avoid i == 0 below

        print('Game {0} of week {1}, {2} being scraped'.format(i + 1, week, year))

        time.sleep(sleep_time)

        # Extracts url for a single game
        single_box_score_url = str(box_scores[i]).split('href="')[1].split('">Final')[0]

        ### Extracting all stats from page for one game ###

        # url request for page containing links to all box scores for a single week
        single_box_score_request = urlopen(core_url + single_box_score_url)

        single_box_score_html = bs.BeautifulSoup(single_box_score_request, "lxml", from_encoding="utf-8")

        # Extract full name of both teams (along w/ unnecessary characters to remove)
        both_teams_full_names_unformatted = single_box_score_html.findAll('meta', attrs={'name': 'Description'})

        neutral_site = False
        # Checking whether game was home/away or at neutral site
        if 'vs.' in str(both_teams_full_names_unformatted)[15:55]:

            # Vis #
            vis_team_name_full_w_score = str(both_teams_full_names_unformatted).split(' vs. ')[0].split('content="')[1]
            vis_team_name_full = re.findall('^[^0-9]*', vis_team_name_full_w_score)[0].strip(' ')

            # Home #
            home_team_name_full_w_score = str(both_teams_full_names_unformatted).split(' vs. ')[1]
            home_team_name_full = re.findall('^[^0-9]*', home_team_name_full_w_score)[0].strip(' ')

            neutral_site = True

        elif 'at' in str(both_teams_full_names_unformatted)[15:55]:

            # Vis #
            vis_team_name_full_w_score = str(both_teams_full_names_unformatted).split(' at ')[0].split('content="')[1]
            vis_team_name_full = re.findall('^[^0-9]*', vis_team_name_full_w_score)[0].strip(' ')

            # Home #
            home_team_name_full_w_score = str(both_teams_full_names_unformatted).split(' at ')[1]
            home_team_name_full = re.findall('^[^0-9]*', home_team_name_full_w_score)[0].strip(' ')

        else:
            print('No at or vs. something is broken')
            break

        ### Extracting home and visiting team name from html source code ###

        ## Vis ##
        vis = single_box_score_html.findAll('th', attrs={'data-stat': 'vis_team_score'})
        vis_team_name = str(vis).split('label="')[1].split('" class')[0]

        vis_score_list = single_box_score_html.findAll('td', attrs={'data-stat': 'vis_team_score'})
        vis_score_final = int(re.findall('score">([\s\S]*?)</td>', str(vis_score_list[len(vis_score_list) - 1]))[0])

        ## Home ##
        home = single_box_score_html.findAll('th', attrs={'data-stat': 'home_team_score'})
        home_team_name = str(home).split('label="')[1].split('" class')[0]

        home_score_list = single_box_score_html.findAll('td', attrs={'data-stat': 'home_team_score'})
        home_score_final = int(re.findall('score">([\s\S]*?)</td>', str(home_score_list[len(home_score_list) - 1]))[0])

        # Create string version of html
        single_box_score_string = str(single_box_score_html)

        ##### Extracting rest of stats using regex #####

        ### Extracting team stats ###

        # Extracts everything b/w the characters before and after ([\s\S]*?)
        team_stats = re.findall('<tr ><th scope="row" class="right " data-stat="st([\s\S]*?)</tbody></table>',
                                single_box_score_string)

        vis_stats = re.findall('"vis_stat" >([\s\S]*?)</td', str(team_stats))
        vis_stats_w_score = [vis_score_final] + vis_stats

        home_stats = re.findall('"home_stat" >([\s\S]*?)</td', str(team_stats))
        home_stats_w_score = [home_score_final] + home_stats

        # Need to extract first stat separately
        stat_names_minus_first_stat = re.findall('data-stat="stat" >([\s\S]*?)</th><td class="center "',
                                                 str(team_stats))

        # Isolating first stat --> First Downs (to current knowledge, should confirm)
        first_stat = re.findall('at" >([\s\S]*?)</th', str(team_stats)[:25])

        if len(first_stat) != 1:
            print('Problem with first_stat')
            break

        stat_names = ['points_scored'] + first_stat + stat_names_minus_first_stat

        ##############################

        # Isolating individual team statistics from list of stats

        rush_index_in_stat_names = [i for i, s in enumerate(stat_names) if 'Rush' in s]

        pass_index_in_stat_names = [i for i, s in enumerate(stat_names) if 'Cmp' in s]

        turnovers_index_in_stat_names = [i for i, s in enumerate(stat_names) if 'Turnovers' in s]

        sack_index_in_stat_names = [i for i, s in enumerate(stat_names) if 'Sack' in s]

        # Sanity checks
        if (len(rush_index_in_stat_names) > 1) or (len(pass_index_in_stat_names) > 1):
            print('Index problem, need to fix')
            break

        if ((stat_names[rush_index_in_stat_names[0]] != 'Rush-Yds-TDs') or (
            stat_names[pass_index_in_stat_names[0]] != 'Cmp-Att-Yd-TD-INT')):
            print('Stat change, need to fix')
            break

        ### Home Team ###

        # Pass stats --> len(..) will be > 5 if passing yards are negative
        if len(home_stats_w_score[pass_index_in_stat_names[0]].split('-')) == 5:

            home_pass_cmp = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[0])
            home_pass_att = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[1])
            home_pass_yds = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[2])
            home_pass_td = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[3])
            home_pass_int = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[4])

        elif len(home_stats_w_score[pass_index_in_stat_names[0]].split('-')) == 6:

            home_pass_cmp = int(re.findall('.+?(?=-)', home_stats_w_score[pass_index_in_stat_names[0]])[0])
            home_pass_att = int(re.findall('.+?(?=-)', home_stats_w_score[pass_index_in_stat_names[0]])[1]) * -1
            home_pass_yds = int(re.findall('.+?(?=-)', home_stats_w_score[pass_index_in_stat_names[0]])[3])
            home_pass_td = int(re.findall('.+?(?=-)', home_stats_w_score[pass_index_in_stat_names[0]])[4]) * -1
            home_pass_int = int(home_stats_w_score[pass_index_in_stat_names[0]].split('-')[
                                    len(home_stats_w_score[pass_index_in_stat_names[0]].split('-')) - 1])

        else:
            print('Pass stats broken')
            break

        # Rush stats --> len(..) will be > 3 if rushing yards are negative
        if len(home_stats_w_score[rush_index_in_stat_names[0]].split('-')) == 3:

            home_rush_att = int(home_stats_w_score[rush_index_in_stat_names[0]].split('-')[0])
            home_rush_yds = int(home_stats_w_score[rush_index_in_stat_names[0]].split('-')[1])
            home_rush_td = int(home_stats_w_score[rush_index_in_stat_names[0]].split('-')[2])

        else:

            # Regex searches for substrings ending in '-' but does not include the ending '-'
            home_rush_att = int(re.findall('.+?(?=-)', home_stats_w_score[rush_index_in_stat_names[0]])[0])
            home_rush_yds = int(re.findall('.+?(?=-)', home_stats_w_score[rush_index_in_stat_names[0]])[2])
            home_rush_td = int(home_stats_w_score[rush_index_in_stat_names[0]].split('-')[
                                   len(home_stats_w_score[rush_index_in_stat_names[0]].split('-')) - 1])

        # Sacks --> indicates how many sacks home team made, not took
        home_sacks = int(vis_stats_w_score[sack_index_in_stat_names[0]].split('-')[0])

        home_turnovers = int(home_stats_w_score[turnovers_index_in_stat_names[0]].split('-')[0])

        home_stats_w_score.extend(
            [home_pass_cmp, home_pass_att, home_pass_yds, home_pass_td, home_pass_int, home_rush_att, home_rush_yds,
             home_rush_td, home_turnovers, home_sacks])

        ### Vis Team ###

        # Pass stats --> len(..) will be > 5 if rushing yards are negative
        if len(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')) == 5:

            vis_pass_cmp = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[0])
            vis_pass_att = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[1])
            vis_pass_yds = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[2])
            vis_pass_td = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[3])
            vis_pass_int = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[4])

        elif len(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')) == 6:

            vis_pass_cmp = int(re.findall('.+?(?=-)', vis_stats_w_score[pass_index_in_stat_names[0]])[0])
            vis_pass_att = int(re.findall('.+?(?=-)', vis_stats_w_score[pass_index_in_stat_names[0]])[1]) * -1
            vis_pass_yds = int(re.findall('.+?(?=-)', vis_stats_w_score[pass_index_in_stat_names[0]])[3])
            vis_pass_td = int(re.findall('.+?(?=-)', vis_stats_w_score[pass_index_in_stat_names[0]])[4]) * -1
            vis_pass_int = int(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')[
                                   len(vis_stats_w_score[pass_index_in_stat_names[0]].split('-')) - 1])

        else:
            print('Pass stats broken')
            break

        # Rush stats
        if len(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')) == 3:

            vis_rush_att = int(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')[0])
            vis_rush_yds = int(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')[1])
            vis_rush_td = int(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')[2])

        else:

            # Regex searches for substrings ending in '-' but does not include the ending '-'
            vis_rush_att = int(re.findall('.+?(?=-)', vis_stats_w_score[rush_index_in_stat_names[0]])[0])
            vis_rush_yds = int(re.findall('.+?(?=-)', vis_stats_w_score[rush_index_in_stat_names[0]])[2])
            vis_rush_td = int(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')[
                                  len(vis_stats_w_score[rush_index_in_stat_names[0]].split('-')) - 1])

        # Sacks
        vis_sacks = int(home_stats_w_score[sack_index_in_stat_names[0]].split('-')[0])

        vis_turnovers = int(vis_stats_w_score[turnovers_index_in_stat_names[0]].split('-')[0])

        vis_stats_w_score.extend(
            [vis_pass_cmp, vis_pass_att, vis_pass_yds, vis_pass_td, vis_pass_int, vis_rush_att, vis_rush_yds,
             vis_rush_td, vis_turnovers, vis_sacks])

        #############

        # Adding stat names created above to stat_names

        stat_names.extend(
            ['pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'pass_int', 'rush_att', 'rush_yds', 'rush_td', 'turnovers',
             'sacks'])

        ############################

        ## Extracting game info ##

        game_info = re.findall('<tr ><th scope="row" class="center " data-stat="in([\s\S]*?)</table>',
                               single_box_score_string)

        game_info_titles = re.findall('fo" >([\s\S]*?)</th><td', str(game_info))

        game_info_values = re.findall('data-stat="stat" >([\s\S]*?)</td></tr', str(game_info))

        if ('Weather' not in game_info_titles) and ('dome' not in game_info_values):
            print('\nNo weather for game {0}, week {1}, {2}\n'.format(i + 1, week, year))

        over_under_index = game_info_titles.index('Over/Under')

        over_under = game_info_values[over_under_index].split(" <b>")[0]

        game_info_values[over_under_index] = over_under

        # May utilize later if desired
        # starters =  re.findall('Lower-case means part-time starter." >Pos</th>([\s\S]*?)</tbody></table>', single_box_score_string)
        ############################

        # Extracting the date

        date_not_formatted = single_box_score_url.split('/')[2][:8]

        date_formatted = date_not_formatted[:4] + '-' + date_not_formatted[4:6] + '-' + date_not_formatted[6:]
        ##############################################################################3

        ### Putting data into a dataframe ###

        box_score_df_columns = ['team', 'team_name_full', 'opposing_team', 'date', 'home', 'year']
        box_score_df_columns.extend(game_info_titles + stat_names)

        box_score_df = pd.DataFrame(columns=box_score_df_columns, index=range(2))

        # Both teams
        box_score_df['date'] = date_formatted
        box_score_df[game_info_titles] = game_info_values
        box_score_df['year'] = year
        box_score_df['week'] = week

        # Home
        box_score_df.loc[0, 'team'] = home_team_name
        box_score_df.loc[0, 'team_name_full'] = home_team_name_full
        box_score_df.loc[0, 'opposing_team'] = vis_team_name
        box_score_df.loc[0, 'home'] = 1
        box_score_df.loc[0, stat_names] = home_stats_w_score

        # Vis
        box_score_df.loc[1, 'team'] = vis_team_name
        box_score_df.loc[1, 'team_name_full'] = vis_team_name_full
        box_score_df.loc[1, 'opposing_team'] = home_team_name
        box_score_df.loc[1, 'home'] = 0
        box_score_df.loc[1, stat_names] = vis_stats_w_score

        # Changing home/neutral_site cols if game was at a neutral site
        if neutral_site == True:

            box_score_df.loc[0, 'home'] = 0  # Changing home team 'home' back to 0

            box_score_df['neutral_site'] = 1

        else:

            box_score_df['neutral_site'] = 0

        ## Vegas formatting ##

        # Checking to see where team_name_full != the team in Vegas Line column
        underdog_mask = box_score_df['team_name_full'] != box_score_df['Vegas Line'].apply(
            lambda x: re.findall('^[^0-9]*', x)[0].strip('-').strip(' '))

        # Extracting vegas line point value
        if box_score_df.loc[0, 'Vegas Line'] == 'Pick':
            vegas_line_points = 0
        else:
            vegas_line_points = -1 * float(re.findall(r'-(\S+)', box_score_df.loc[0, 'Vegas Line'])[0])

        box_score_df['vegas_line_formatted'] = np.where(underdog_mask == True, -1, 1) * vegas_line_points

        ## Calculating opponents stats ##

        opp_stat_names = ['opp_{}'.format(stat) for stat in stat_names]

        for opp_stat, stat in zip(opp_stat_names, stat_names):
            box_score_df.loc[0, opp_stat] = box_score_df.loc[1, stat]

            box_score_df.loc[1, opp_stat] = box_score_df.loc[0, stat]

        # Calculate which team won the game
        box_score_df['win'] = np.where(box_score_df['points_scored'] > box_score_df['opp_points_scored'], 1, 0)

        if no_games_this_week == True:

            pass

        elif i == 0:  # If this is the first game in the week/year combo, all_box_scores is equal to this first game df

            all_box_scores = box_score_df

        else:

            all_box_scores = all_box_scores.append(box_score_df)

    if no_games_this_week == True:

        pass

    else:

        if 'Roof' in all_box_scores.columns:
            # Creating one-hots for Roof
            all_box_scores = pd.get_dummies(all_box_scores, columns=['Roof'])

        if 'Surface' in all_box_scores.columns:
            # Creating one-hots for Surface
            all_box_scores = pd.get_dummies(all_box_scores, columns=['Surface'])

        all_box_scores.reset_index(inplace=True, drop=True)

        ### Breaking down weather col (multiple pieces of info separated by commas) into separate, usable columns ###

        if 'Weather' in all_box_scores.columns:

            # Filling in games for which weather was not listed
            all_box_scores['Weather'].fillna(value='missing weather', inplace=True)

            # Split weather column by commas
            weather_col_split = all_box_scores['Weather'].apply(lambda x: x.split(',') if not pd.isnull(x) else x)

            # For formatting purposes in next step
            weather_col_split[all_box_scores.Weather == 'missing weather'] = 'missing weather'

            # Want to double check that there are no roof conditions other than dome that may be indoors
            if 'Roof_dome' in all_box_scores.columns:
                weather_col_split[all_box_scores.Roof_dome == 1] = 'Indoors'

            if 'Roof_retractable roof (closed)' in all_box_scores.columns:
                weather_col_split[all_box_scores['Roof_retractable roof (closed)'] == 1] = 'Indoors'

            # Extracting temp from weather_col_split
            temp = weather_col_split.apply(
                lambda x: x[0].split(' degrees')[0] if ((x != 'Indoors') and (x != 'missing weather'))  else x)

            # Imputing median temp for games with missing weather
            actual_temp_values_mask = np.array(temp.apply(lambda x: str(x).isdigit()))
            median_temp_of_week = np.median(temp[actual_temp_values_mask].astype(int))
            temp[temp.astype(object) == 'missing weather'] = median_temp_of_week

            # Imputing temperature for indoor games
            temp[temp == 'Indoors'] = 69

            # Extracting wind speed from weather_col_split
            wind_speed = weather_col_split.apply(lambda x: int(x[1].split('wind')[1].split('mph')[0].strip(' ')) if (
            (x != 'Indoors') and ('no wind' not in x[1]) and (x != 'missing weather')) else x)

            # Imputing median wind speed for games with missing weather
            actual_wind_values_mask = np.array(wind_speed.apply(lambda x: str(x).isdigit()))
            median_wind_of_week = np.median(wind_speed[actual_wind_values_mask].astype(int))
            wind_speed[wind_speed.astype(object) == 'missing weather'] = median_wind_of_week

            # Setting wind speed = 0 for rows that contain 'no wind' in Weather column or are indoors
            no_wind_mask = weather_col_split.apply(
                lambda x: True if (('no wind' in x[1]) or (x == 'Indoors')) else False)
            wind_speed[no_wind_mask] = 0

            all_box_scores['temp'] = temp

            all_box_scores['wind_mph'] = wind_speed

        all_box_scores.reset_index(inplace=True, drop=True)

        return (all_box_scores)


def main(year, sleep_time, num_weeks):
    iterable = [i for i in range(1, num_weeks + 1)]
    pool = multiprocessing.Pool(4)
    func = partial(scrape_one_week_box_scores, year, sleep_time)
    results = pool.map(func, iterable)
    one_szn_box_scores = pd.DataFrame(columns=results[0].columns, index=[0])
    for df in results:
        one_szn_box_scores = one_szn_box_scores.append(df)

    return (one_szn_box_scores.iloc[1:, :])


year = 1988
num_weeks = 22
sleep_time = 3

if __name__ == "__main__":

    all_box_scores = main(year, sleep_time, num_weeks)

    ### Extra formatting for full weeks that had no weather (i.e. Super Bowl 2016) ###

    # Imputing median temp for games with missing weather
    all_box_scores['temp'].fillna(value='missing weather', inplace=True)
    actual_temp_values_mask = np.array(all_box_scores['temp'].apply(lambda x: str(x).isdigit()))
    median_temp_of_week = np.median(all_box_scores.loc[actual_temp_values_mask, 'temp'].astype(int))
    all_box_scores.loc[all_box_scores['temp'].astype(object) == 'missing weather', 'temp'] = median_temp_of_week

    # Imputing median wind speed for games with missing weather
    all_box_scores['wind_mph'].fillna(value='missing weather', inplace=True)
    actual_wind_values_mask = np.array(all_box_scores['wind_mph'].apply(lambda x: str(x).isdigit()))
    median_wind_of_week = np.median(all_box_scores.loc[actual_wind_values_mask, 'wind_mph'].astype(int))
    all_box_scores.loc[all_box_scores['wind_mph'].astype(object) == 'missing weather', 'wind_mph'] = median_wind_of_week

    if 'Roof_retractable roof (closed)' in all_box_scores.columns:
        all_box_scores.loc[all_box_scores['Roof_retractable roof (closed)'] == 1, 'temp'] = 69
        all_box_scores.loc[all_box_scores['Roof_retractable roof (closed)'] == 1, 'wind_mph'] = 0

    if 'Roof_dome' in all_box_scores.columns:
        all_box_scores.loc[all_box_scores['Roof_dome'] == 1, 'temp'] = 69
        all_box_scores.loc[all_box_scores['Roof_dome'] == 1, 'wind_mph'] = 0

    all_box_scores.sort_values(['week'], inplace=True)

    all_box_scores.to_csv('/Users/Miller/Desktop/{}_full.csv'.format(year))

print('all_box_scores' in globals())







