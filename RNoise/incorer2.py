import pandas as pd
import math
import string
import gurobipy as gp
import numpy as numpy
from gurobipy import GRB
import random
import re
import os
from subprocess import PIPE, run
import pandasql as psql
import time
import matplotlib.pyplot as plt
import subprocess
import datetime
from datetime import date
from collections import defaultdict
from itertools import repeat
import measurments as meas

def calculate_all_probs(df,colomnsInConstraints,beta):
    """
    calculate_all_probs - helper function in order to calculate all columns propabilities and store them into a dictionary

    Parameters
    ----------
    df : dataframe
    colomnsInConstraints : list of string
                            a list of all clomumns who are part of a constraint

    Returns
    -------
    a dictionary in which the keys are the columns and the values are lists of propabilities for each unique value

    """
    all_probs = defaultdict(list)
    i = 1
    for col in colomnsInConstraints:
        temp_sum = harmonic_sum(len(df[col].unique()),beta)
        for cell in df[col].unique():
            value_prob = (1.0/(i ** beta))/float(temp_sum)
            all_probs[col] = all_probs[col] + [cell]*math.ceil(value_prob*10000.0)
            i += 1
        i = 1
    return all_probs

def harmonic_sum(n,beta):
    sum = 0.0
    for i in range(1,n+1):
        sum += 1.0/(i ** beta)
    return sum

def randomize_value(df,data):
    """
    randomize_value - the function that randomize a new value based on its type

    Parameters
    ----------
    df : dataframe
    data : str/int/float/date
        the data which should be randomnly changed into a new value
        
    Returns
    -------
    Generates a new value to be assign into the database
    
    """
    if type(data) is str:
        val = data + random.choice(string.ascii_letters)
        
    elif type(data) is numpy.int64 :
        data_string = str(data)
        rnd_digit = random.randint(1,len(data_string))
        digit = int(data_string[rnd_digit-1])
        coin = random.randint(1, 2)
        if coin == 1 :
            if digit == 9 : digit = 0
            else : digit += 1
            data_string = data_string[:rnd_digit-1] + str(digit)+ data_string[rnd_digit-1 + 1:]
        if coin == 2 :
            if digit == 0 : digit = 9
            else : digit -= 1  
            data_string = data_string[:rnd_digit-1] + str(digit)+ data_string[rnd_digit-1 + 1:]
        val = int(data_string)
            
    elif type(data) is float or type(data) is numpy.float64 or type(data) is numpy.float_ or type(data) is numpy.float32:
        data_string = str(data)
        rnd_digit = 0
        while data_string[rnd_digit-1]=='.' or data_string[rnd_digit-1]=='-' or rnd_digit == 0 :
            rnd_digit = random.randint(1,len(data_string))
        digit = int(data_string[rnd_digit-1])
        coin = random.randint(1, 2)
        if coin == 1 :
            if digit == 9 : digit = 0
            else : digit += 1
            data_string = data_string[:rnd_digit-1] + str(digit)+ data_string[rnd_digit-1 + 1:]
        if coin == 2 :
            if digit == 0 : digit = 9
            else : digit -= 1  
            data_string = data_string[:rnd_digit-1] + str(digit)+ data_string[rnd_digit-1 + 1:]
        val = float(data_string)
        
    elif type(data) is date:
        new_day, new_month, new_year = [data.day,date.month,date.year]
        coin = random.randint(1, 3)
        if coin == 1 : 
            if data.month in [1,3,5,7,8,10,12]:
                new_day = random.randint(1,31)
            elif data.month in [4,6,9,11]:
                new_day = random.randint(1,30)
            elif data.month == 2 and data.year % 4 == 0 and data.year % 100 != 0 :
                new_day = random.randint(1,29)
            else : random.randint(1,28)
        if coin == 2 :
            new_month = random.randint(1,12)
        if coin == 3 :
            new_year = random.randint(1921,datetime.datetime.now().year)
        val = datetime.datetime(new_year, new_month, new_day)
        
    return val

def replace_value(df,data_col,all_probs):
    return all_probs[data_col][random.randint(0,len(all_probs[data_col])-1)]

def flip(p):
    return 1 if random.random() < p else 2

def rand_vio_algorithm(df,colomnsInConstraints,all_probs,typo_prob):
    """
    rand_vio_algorithm - the function chooses a random cell in the database and randomly chooses whether to
                         replace the value with a different value from the column or randomize a new value.

    Parameters
    ----------
    df : dataframe
    colomnsInConstraints : list of string
                            a list of all clomumns who are part of a constraint
    all_probs : dictionary of arrays
                dictionary in which the keys are the columns of the database and the values are
                list of probabilities for each value
        
    Returns
    -------
    Generates a new value to be assign into the database
    
    """
    rand_cell_row = random.randint(0, df.shape[0])
    rand_cell_col = random.choice(colomnsInConstraints)                  
    rand_cell_data = df.iloc[rand_cell_row-1][rand_cell_col]
    
    while(pd.isnull(df.iloc[rand_cell_row-1][rand_cell_col])) :
        rand_cell_row = random.randint(0, df.shape[0])
        rand_cell_col = random.choice(colomnsInConstraints)                  
        rand_cell_data = df.iloc[rand_cell_row-1][rand_cell_col]
    
    coin = flip(typo_prob)
    if coin == 1:
        new_val = randomize_value(df,rand_cell_data)
    if coin == 2:
        new_val = replace_value(df,rand_cell_col,all_probs)
        
    df.at[rand_cell_row-1,rand_cell_col] = new_val

def runTestRand(database_name, err_rate=0.01, skew=0, typo_prob=0.5, measuresToRun={"I_D":True, "I_MI":True, "I_P":True, "I_R":True, "I_lin_R":True, "I_MC":False}):
    """
    runTest - the main function that computes the measures specified by the user on the given database

    Parameters
    ----------
    database_name : string
        the name of the folder containing the database
    percantege : float
        the fraction of the cells that will be changed randomly
    measuresToRun : dictionary
        a dictionary in which the measures are the keys and true/false are the values.
        The function will compute the measures for which the value is true.
        measuresToRun shoud be in the form : {"I_D":True, "I_MI":True, "I_P":True, "I_R":True, "I_lin_R":True, "I_MC":False}

    Returns
    -------
    Generate a chart for each measure where the y axis is the value of the measure and the x axis is the 
    iteration number. The charts will be saved under the folder containing the database.
    
    The files "Running_Time.txt" and "All_results.txt" contain the average running time of each maasure and
    all the results of the execution, respectively.
    
    """
    global df

    # constracting paths for the results     
    resultsDirectoryPath = '/' + str(time.time()) + '_results'
    fullPath = 'Data/'+ database_name + resultsDirectoryPath
    if (not os.path.exists(fullPath)):
        os.makedirs(fullPath);
    runningTimesFileName = fullPath +'/Running_Time.txt'
    allResultsFileName = fullPath +'/All_results.txt'

    start = time.time()
    
    # load the csv file and generate a list of constraints
    df = pd.read_csv('Data/'+ database_name + '/' + "inputDB.csv", keep_default_na=False, na_values=['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A N/A', '#N/A', 'N/A', 'n/a','', '#NA', 'NULL','null', 'NaN', '-NaN', 'nan', '-nan', ''] , header=0)
    constraints_raw = open('Data/'+ database_name+'/dcs.txt', 'r')
    constraints = [line.strip() for line in constraints_raw.readlines()]
    constraints = [x.replace(' ', '_') for x in constraints] #in case the columns names include spaces

    pd.options.mode.chained_assignment = None 
            
    # in case the column names include spaces
    allColumns = {}
    for col in df.columns: 
        allColumns[col] = col.replace(' ','_')
    df = df.rename(columns=allColumns)
    
    # initializations
    exes,measurments1,measurments2,measurments3,measurments4,measurments5,measurments6 = [],[],[],[],[],[],[]
    sum2,sum3,sum4,sum5,sum6 = 0,0,0,0,0
    
    # construct the dynamic queries which will be used for detecting violations in the database
    allConstraints = meas.build_dynamic_queries(constraints,df)
    allColumns = allConstraints[2]  
    
    # calculate all propabilities
    listToStr = ' '.join([str(elem) for elem in constraints])
    colomnsInConstraints = meas.col_in_constraints(listToStr,df)
    all_probs = calculate_all_probs(df,colomnsInConstraints,skew)
    
    # calculations for the first stage - the database should be consistent
    exes.append(0)
    sdfc = meas.constraints_check(df,constraints, allColumns, allConstraints[0], allConstraints[1])
    if (measuresToRun["I_D"]):
        measurments1.append(meas.first_measurer_I_D(sdfc[0]))
    if (measuresToRun["I_MI"]):
        measurments2.append(meas.second_measurer_I_MI(sdfc[0]))
    if (measuresToRun["I_P"]):
        measurments3.append(meas.third_measurer_I_P(sdfc[1]))
    if (measuresToRun["I_R"]):    
        measurments4.append(meas.fourth_measurer_I_R(sdfc[0])[0])
    if (measuresToRun["I_lin_R"]): 
        measurments5.append(meas.fifth_measurer_I_lin_R(sdfc[0])[0])
    if (measuresToRun["I_MC"]):
        measurments6.append(meas.sixth_measurer_I_MC(fullPath, sdfc[0])[0])
     
    cells_count = len(df.columns) * df.shape[0]
    iterations = int(err_rate * cells_count)

    print('Test '+database_name+' : running ' + str(iterations) + ' iterations; startTime:' + str(time.time()))
    for x in range(1, iterations):
        rand_vio_algorithm(df,colomnsInConstraints,all_probs,typo_prob) 
        
        #calculate the measurments every 10 iterations
        if (x%10 == 0):
            # calcuate the queries needed for the measures
            sdfc = meas.constraints_check(df,constraints, allColumns, allConstraints[0], allConstraints[1])
            exes.append(x)

            if (measuresToRun["I_D"]):
                measurments1.append(meas.first_measurer_I_D(sdfc[0]))

            if (measuresToRun["I_MI"]):
                measurments2.append(meas.second_measurer_I_MI(sdfc[0]))
                sum2 += sdfc[2]

            if (measuresToRun["I_P"]):
                measurments3.append(meas.third_measurer_I_P(sdfc[1]))
                sum3 += sdfc[3]

            if (measuresToRun["I_R"]):    
                res1 = meas.fourth_measurer_I_R(sdfc[0])
                measurments4.append(res1[0])
                sum4 += res1[1]

            if (measuresToRun["I_lin_R"]): 
                res2 = meas.fifth_measurer_I_lin_R(sdfc[0])
                measurments5.append(res2[0])
                sum5 += res2[1]

            if (measuresToRun["I_MC"]):
                res3 = meas.sixth_measurer_I_MC(fullPath, sdfc[0])
                measurments6.append(res3[0])
                sum6 += res3[1]
    
    # messages at finish
    print('Test '+database_name+' : runTime = ' + str(time.time()))
    print('Test '+database_name+' finished, preparing the results.')
    
    f_times   = open(runningTimesFileName, "a+")
    f_results = open(allResultsFileName,"a+")
    
    if (measuresToRun["I_D"]):
        plt.scatter(exes, measurments1, c='r')
        plt.title('Drastic inconsistency value I_D:')
        plt.ylabel('results')
        plt.xlabel('number of changes')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_D.jpg', dpi=300)
        plt.clf()

    if (measuresToRun["I_MI"]):
        plt.scatter(exes, measurments2, c='b')
        plt.title('Minimal inconsistent subsets of D I_MI:')
        plt.ylabel('results')
        plt.xlabel('number of changes')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_MI.jpg', dpi=300) 
        f_times.write("AVG for I_MI: ")
        f_times.write(str(float(sum2/iterations)))
        f_results.write("I_MI results: ")
        f_results.write(str(measurments2))
        plt.clf()

    if (measuresToRun["I_P"]):
        plt.scatter(exes, measurments3, c='g')
        plt.title('Problematic facts I_P:')
        plt.ylabel('results')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_P.jpg', dpi=300)
        f_times.write("\nAVG for I_P: ")
        f_times.write(str(float(sum3/iterations))) 
        f_results.write("\nI_P results: ")
        f_results.write(str(measurments3))
        plt.clf()

    if (measuresToRun["I_R"]):
        plt.scatter(exes, measurments4, c='y')
        plt.title('Minimal cost of a sequence of operations that repairs the database I_R:')
        plt.ylabel('results')
        plt.xlabel('number of changes')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_R.jpg', dpi=300)  
        f_times.write("\nAVG for I_R: ")
        f_times.write(str(float((sum4+sum2)/iterations)))
        f_results.write("\nI_R results: ")
        f_results.write(str(measurments4))
        plt.clf()

    if (measuresToRun["I_lin_R"]):
        plt.scatter(exes, measurments5, c='pink')
        plt.title('Linear relaxation of the fourth measurer I_lin_R:')
        plt.ylabel('results')
        plt.xlabel('number of changes')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_lin_R.jpg', dpi=300)  
        f_times.write("\nAVG for I_lin_R: ")
        f_times.write(str(float((sum5+sum2)/iterations)))
        f_results.write("\nI_lin_R results: ")
        f_results.write(str(measurments5))
        plt.clf()

    if (measuresToRun["I_MC"]):
        plt.scatter(exes, measurments6, c='purple')
        plt.title('Maximal cliques I_MC:')
        plt.ylabel('results')
        plt.xlabel('number of changes')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_MC.jpg', dpi=300)  
        f_times.write("\nAVG for I_MC: ")
        f_times.write(str(float((sum6+sum2)/iterations)))
        f_results.write("\nI_MC results: ")
        f_results.write(str(measurments6))
        plt.clf()

    end = time.time()
    f_times.write("\ntotal time ")
    f_times.write(str(end - start))

    f_times.write("\n---\n")
    f_results.write("\n---\n")

    f_times.close()
    f_results.close()

    print('End of test '+database_name + '; total time = ' + str(end - start))
    print('\033[1m'+"Computation finished, outputs can be found in "+'Data/'+ database_name + resultsDirectoryPath +'\n \033[0m')
