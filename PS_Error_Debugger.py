# http://www.pro-football-reference.com/players/
import bs4 as bs
from urllib.request import urlopen
import time
import pickle
import os
import sys
import re
import multiprocessing
import os
import glob
import pandas as pd
from functools import partial
from Players_Scraper import scrape_player, DATA_DIRECTORY_NAME



# saving pickles
def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

# loading pickles
def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


###########################################
##             for debugging             ##
###########################################
'''
loadplk = load_obj(DATA_DIRECTORY_NAME+'critical_errors_B_2017-08-09')

errors = []
for letter in loadplk:
    for player in letter:
        errors.append(player[1])

debugger = ['https://www.pro-football-reference.com/players/A/AbbrJa00/gamelog/'] # <- control variable
debugger =  debugger + errors
'''
debugger = ['https://www.pro-football-reference.com/players/A/AbbrJa00/gamelog/',
            'https://www.pro-football-reference.com/players/Y/YeldT.00/gamelog/'] # <- control variable


for debug in debugger:
    val, err = scrape_player(debug)
    #err = ' '.join(err)
    if val is 0 or val is -1:
        print(err,':',debug)
    else:
        print('OUTPUT:' ,val)

