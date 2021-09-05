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
import ViolationsAlgorithm as vio
import datetime
from datetime import date
from collections import defaultdict
from itertools import repeat
import measurments as meas

def insertViolationsExp(database_name, timesToRunTheTest=100, measuresToRun={"I_D":True, "I_MI":True, "I_P":True, "I_R":True, "I_lin_R":True, "I_MC":False}, singleIteration=False):
    """
    insertViolationsExp - the main function that computes the measures specified by the user on the given database.
    
    If singleIteration is true, then all the measures will be computed once on the given database.
    
    Otherwise, the function will run a simulation that generates random violations in the given database, and
    computes, after each iteration (i.e., after each change in the database), the values of all the measures.

    Parameters
    ----------
    databas_name : string
        the name of the folder containing the database
    timesToRunTheTest : int
        if singleIteration is false, this is the number of iteration in the simulation.
    measuresToRun : dictionary
        a dictionary in which the measures are the keys and true/false are the values.
        The function will compute the measures for which the value is true.
        measuresToRun shoud be in the form : {"I_D":True, "I_MI":True, "I_P":True, "I_R":True, "I_lin_R":True, "I_MC":False}
    singleIteration : bool
        true if the measures should be computed once on the given database, and false for a simulation.
        
    Returns
    -------
    Generate a chart for each measure where the y axis is the value of the measure and the x axis is the 
    iteration number. The charts will be saved under the folder containing the database.
    
    The files "Running_Time.txt" and "All_results.txt" contain the average running time of each maasure and
    all the results of the execution, respectively.
    
    """
    global df
    # messages at start
    if not singleIteration:
        print('Test '+database_name+' : running ' + str(timesToRunTheTest) + ' iterations; startTime:' + str(time.time()))
    else:
        print('Test '+database_name+' ; startTime:' + str(time.time()))

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
     
    # in case the user wishes to run the violations algorithm and introduce random violations in the database    
    if not singleIteration:    
        for x in range(1, 100):
            global t1,t2
            
            # choose two tuples randomly
            sample = df.sample(n=2)
            t1 = sample.iloc[0]
            t2 = sample.iloc[1]
        
            # clean constraint from excessive chars
            constraintSetRaw = random.choice(constraints)
            constraintSet = constraintSetRaw[4:-1].split('&')
            constraintSet = [re.split('(!=|>=|<=|>|<|=)', i) for i in constraintSet]
            
            # in case the constraint refers to a single tuple
            if "t2" not in constraintSet:
                t2 = t1
                
            # generate violations using the fittingViolationAlgorithm in ViolationsAlgorithm.py
            t = vio.fittingViolationAlgorithm(constraintSet,df,t1,t2)
            vio.updateTable(df,t[0],t[1],sample)

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
        f_times.write(str(float(sum2/timesToRunTheTest)))
        f_results.write("I_MI results: ")
        f_results.write(str(measurments2))
        plt.clf()

    if (measuresToRun["I_P"]):
        plt.scatter(exes, measurments3, c='g')
        plt.title('Problematic facts I_P:')
        plt.ylabel('results')
        plt.savefig('Data/'+ database_name + resultsDirectoryPath + '/I_P.jpg', dpi=300)
        f_times.write("\nAVG for I_P: ")
        f_times.write(str(float(sum3/timesToRunTheTest))) 
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
        f_times.write(str(float((sum4+sum2)/timesToRunTheTest)))
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
        f_times.write(str(float((sum5+sum2)/timesToRunTheTest)))
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
        f_times.write(str(float((sum6+sum2)/timesToRunTheTest)))
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

insertViolationsExp('Airport')
