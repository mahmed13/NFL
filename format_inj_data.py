import pandas as pd
import numpy as np
import datetime

# Read in injury data set
injuries = pd.read_csv("injuries.csv")
injuries.drop('Unnamed: 0', axis=1, inplace = True)
injuries.rename(columns={'Played':'played_inj'}, inplace=True)

# Drop healthy rows
inj_no_healthy = injuries.loc[~ (injuries.Injury == 'Healthy'),:]
inj_no_healthy.reset_index(inplace=True)
inj_no_healthy.drop('index', axis=1, inplace=True)

# Changes names in inj_no_healthy to match changes made in weekly RB game log
inj_no_healthy.loc[(inj_no_healthy.Player == 'Adrian Peterson') & (inj_no_healthy.Team == 'CHI'),'Player'] = 'Adrian Georgia Southern Peterson'

# Fixing injury column for players with no text in injury column
no_injury_bool_array = np.array(pd.isnull(inj_no_healthy['Injury']))
inj_no_healthy.loc[no_injury_bool_array, 'Injury'] = 'NONE'

# Creating one-hots for injury types
inj_no_healthy['knee'] = np.where(inj_no_healthy.Injury.str.contains('Knee|knee|KNEE|MCL|mcl|pcl|PCL|ACL|acl|lcl|LCL'),1,0)
inj_no_healthy['ankle'] = np.where(inj_no_healthy.Injury.str.contains('Ankle|ankle|ANKLE|amkle|ankkle|ankile'),1,0)
inj_no_healthy['foot'] = np.where(inj_no_healthy.Injury.str.contains('Foot|foot|FOOT|feet|Feet'),1,0)
inj_no_healthy['toe'] = np.where(inj_no_healthy.Injury.str.contains('toe|Toe'),1,0)
inj_no_healthy['hamstring'] = np.where(inj_no_healthy.Injury.str.contains('Hamstring|hamstring|HAMSTRING|hammy|Hammy|hamsting|hmastring'),1,0)
inj_no_healthy['quad'] = np.where(inj_no_healthy.Injury.str.contains('Quad|quad|QUAD|Quadriceps|quariceps|quadriceps|QUADRICEPS|quadricep|Quadricep|Glute|glute'),1,0)
inj_no_healthy['leg'] = np.where(inj_no_healthy.Injury.str.contains('Leg|leg|LEG|shin|Shin|fibula|tibia|Fibula|Tibia|calf|Calf'),1,0)
inj_no_healthy['arm'] = np.where(inj_no_healthy.Injury.str.contains('Arm|arm|ARM|forearm|Forearm|foream|biceps|Biceps|triceps|Triceps|elbow|Elbow|tricep|bicep|Tricep|Bicep'),1,0)
inj_no_healthy['shoulder'] = np.where(inj_no_healthy.Injury.str.contains('Shoulder|shoulder|SHOULDER|shouldet|scapula|Scapula'),1,0)
inj_no_healthy['groin'] = np.where(inj_no_healthy.Injury.str.contains('Groin|groin|Groin'),1,0)
inj_no_healthy['thigh'] = np.where(inj_no_healthy.Injury.str.contains('Thigh|thigh|THIGH'),1,0)
inj_no_healthy['ribs'] = np.where(inj_no_healthy.Injury.str.contains('Ribs|ribs|RIBS|rib|Rib'),1,0)
inj_no_healthy['finger'] = np.where(inj_no_healthy.Injury.str.contains('Finger|finger|fingers|Fingers|thumb|Thumb'),1,0)
inj_no_healthy['hand'] = np.where(inj_no_healthy.Injury.str.contains('Hand|hand|HAND'),1,0)
inj_no_healthy['concussion'] = np.where(inj_no_healthy.Injury.str.contains('Concussion|concussion|head|Head|migraine|migraines|Migraine|Migraines|concusision'),1,0)
inj_no_healthy['illness'] = np.where(inj_no_healthy.Injury.str.contains('Illness|illness|sick|Sick|infection|flu'),1,0)
inj_no_healthy['wrist'] = np.where(inj_no_healthy.Injury.str.contains('wrist|Wrist|WRIST'),1,0)
inj_no_healthy['back'] = np.where(inj_no_healthy.Injury.str.contains('Back|back|BACK|spine|Spine|beck'),1,0)
inj_no_healthy['chest'] = np.where(inj_no_healthy.Injury.str.contains('Chest|chest|pec|Pectoral|pectoral|PEC|Upper Body|sternum|Collarbone|collarbone|Clavicle|clavicle'),1,0)
inj_no_healthy['groin'] = np.where(inj_no_healthy.Injury.str.contains('Groin|groin|GROIN'),1,0)
inj_no_healthy['hip'] = np.where(inj_no_healthy.Injury.str.contains('Hip|hip|HIP|hiip'),1,0)
inj_no_healthy['neck'] = np.where(inj_no_healthy.Injury.str.contains('neck|Neck|Stinger|stinger'),1,0)
inj_no_healthy['achilles'] = np.where(inj_no_healthy.Injury.str.contains('Achillies|achillies|achilles|Achilles|heel|Heel|anchilles'),1,0)
inj_no_healthy['midsection'] = np.where(inj_no_healthy.Injury.str.contains('abdomina|abs|hernia|Hernia|abdominal|Abdominal|abdominals|core|Core|Pelvis|pelvis|abdomen|Abdomen|abdomden|oblique|Oblique|stomach|Stomach|SolarPlexus|appendectomy'),1,0)
inj_no_healthy['face'] = np.where(inj_no_healthy.Injury.str.contains('ear|Ear|dental|Dental|Nose|nose|chin|eye|Eye|facial|Facial|face|Face|jaw|Jaw|throat|Throat|teeth|Teeth|tooth|Tooth'),1,0)
inj_no_healthy['organ'] = np.where(inj_no_healthy.Injury.str.contains('heart|Heart|lung|Lung|kidney|Kidney|spleen|Spleen'),1,0)
inj_no_healthy['sprain'] = np.where(inj_no_healthy.Injury.str.contains('sprain|Sprain'),1,0)
inj_no_healthy['fracture'] = np.where(inj_no_healthy.Injury.str.contains('fracture|Fracture|break|broken|Broken|fractured|Fractured'),1,0)
inj_no_healthy['strain'] = np.where(inj_no_healthy.Injury.str.contains('strain|Strain'),1,0)
inj_no_healthy['tear'] = np.where(inj_no_healthy.Injury.str.contains('tear|Tear|torn|Torn|rupture|Rupture|ruptured|Ruptured'),1,0)
inj_no_healthy['dislocation'] = np.where(inj_no_healthy.Injury.str.contains('dislocation|Dislocation|dislocated|Dislocated|sublux'),1,0)
inj_no_healthy['surgery'] = np.where(inj_no_healthy.Injury.str.contains('surgery|Surgery'),1,0)
inj_no_healthy['not_injury_related'] = np.where(inj_no_healthy.Injury.str.contains('Related|personal|Personal|Coach|related|Eligibility|rest|Rest|suspension|Suspension|disciplinary|Disciplinary|NONE'),1,0)
inj_no_healthy['undisclosed'] = np.where(inj_no_healthy.Injury.str.contains('undisclosed|Undisclosed'),1,0)

##########################################

# # Adding column for 'other' injuries
# first_inj_one_hot_idx = list(inj_no_healthy.columns).index('knee')
# last_inj_one_hot_idx = list(inj_no_healthy.columns).index('undisclosed')
#inj_no_healthy['other_inj'] = np.where(inj_no_healthy.iloc[:, first_inj_one_hot_idx : last_inj_one_hot_idx + 1].sum(axis=1) < 1, 1, 0)

# # Use to count number of observations in each inj column
# inj_no_healthy.iloc[:, first_inj_one_hot_idx : last_inj_one_hot_idx + 2].sum(axis=0)

# for i in range(len(inj_no_healthy['other_inj'])):
#     if inj_no_healthy.loc[i,'other_inj'] > 0:
#         print(inj_no_healthy.loc[i,'Injury'])

###########################################

# Create column for designations that are injury related --> incorporates 'other_inj'
inj_no_healthy['injury_related_designation'] = np.where(np.array(inj_no_healthy.loc[:, 'not_injury_related']) < 1, 1, 0)

# Creating new date column for merge with weekly stat data
inj_no_healthy['date_formatted'] = pd.to_datetime(inj_no_healthy['Date']).astype(str)

# Creating new team column for merge with weekly stat data
inj_no_healthy['team_formatted'] = inj_no_healthy['Team']
inj_no_healthy.loc[inj_no_healthy.Team == 'CLT','team_formatted'] = 'IND'
inj_no_healthy.loc[inj_no_healthy.Team == 'CRD','team_formatted'] = 'ARI'
inj_no_healthy.loc[inj_no_healthy.Team == 'HTX','team_formatted'] = 'HOU'
inj_no_healthy.loc[inj_no_healthy.Team == 'OTI','team_formatted'] = 'TEN'
inj_no_healthy.loc[inj_no_healthy.Team == 'RAI','team_formatted'] = 'OAK'
inj_no_healthy.loc[inj_no_healthy.Team == 'RAV','team_formatted'] = 'BAL'

# Creating boolean arrays for the different Rams cities
stl_rams = np.where( (inj_no_healthy.date_formatted.apply(lambda x: str(x)[:4]).astype(int) < 2016) & (inj_no_healthy.Team == 'RAM'), True, False)
la_rams = np.where( (inj_no_healthy.date_formatted.apply(lambda x: str(x)[:4]).astype(int) >= 2016) & (inj_no_healthy.Team == 'RAM'), True, False)

inj_no_healthy.loc[stl_rams,'team_formatted'] = 'STL'
inj_no_healthy.loc[la_rams,'team_formatted'] = 'LAR'

# Create one hot column to indicate when a player missed a game with injury
inj_no_healthy['missed_game_due_inj']  = np.array(inj_no_healthy['injury_related_designation']) * ~np.array(inj_no_healthy['played_inj'])

# Create one hots for injury designations
inj_no_healthy = pd.get_dummies(inj_no_healthy, columns=['Chance_of_playing'])

# Similar to injury related designation except not including probable
inj_no_healthy['quest_doubt_out'] = np.array(inj_no_healthy['Chance_of_playing_Out']) + np.array(inj_no_healthy['Chance_of_playing_Questionable']) + np.array(inj_no_healthy['Chance_of_playing_Doubtful'])

# Combine columns that all mean the player is not playing
inj_no_healthy['Chance_of_playing_Out'] = np.array(inj_no_healthy['Chance_of_playing_Out']) + np.array(inj_no_healthy['Chance_of_playing_Suspended']) + np.array(inj_no_healthy['Chance_of_playing_Injured Reserve']) + np.array(inj_no_healthy['Chance_of_playing_Physically Unable to Perform'])

# Drop unwanted columns --> I think we need to drop injury
inj_no_healthy.drop(['Injury', 'Team','played_inj', 'Chance_of_playing_Suspended', 'Chance_of_playing_Physically Unable to Perform', 'Chance_of_playing_Injured Reserve','Team'], axis=1, inplace=True)

inj_no_healthy.rename(columns={'team_formatted':'Team'}, inplace=True)

#inj_no_healthy.to_csv('/Users/Miller/Documents/NFL DFS 2017/all_positions/Data/injuries_from_2009_formatted_no_healthy.csv')

