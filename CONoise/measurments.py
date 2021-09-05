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

def col_in_constraints(constraintSet,df):
    allColomns = []
    for col in df.columns:
        if col in constraintSet:
            allColomns.append(col)
    return allColomns

def build_dynamic_queries(constraintSets,df):
    """
    build_dynamic_queries - generates dynamic queries based on the given constraints.
    This function will generate two queries:
    1. unionOfAllTuples - returns the ids of the tuples participating in a violation of the constraints.
    2. unionOfAllPairs - returns pairs (i1,i2) of ids of tuples that jointly violate the constraints.
    
    Parameters
    ----------
    constraintSets : set of strings
        each string represents a constraint from the dcs file
    df : dataframe
        the database frame
        
    Returns
    -------
    list of three string values:
        unionOfAllTuples, unionOfAllPairs are the generated queries
        allColumns is a string consisting of all column names seperated by ','
    """ 
    
    allColumns = ' '.join([str(elem) for elem in df.columns.values.tolist()]).replace(' ',',')

    #Additional conditions for the queries, in order to ignore missing values in the database
    count = 1
    columnsT1 = ""
    for col in df.columns: 
        columnsT1 += "t1."+col
        if count!=len(df.columns) :
            columnsT1+= ' IS NOT NULL AND '
        count+=1
    columnsT1+=" IS NOT NULL "
    columnsT2 = columnsT1.replace('t1','t2')

    count = 0
    for con in constraintSets: 
        if count == 0:
            unionOfAllPairs = " SELECT t1.rowid as t1ctid ,t2.rowid as t2ctid FROM df t1,df t2 WHERE "
            unionOfAllTuples = " SELECT * FROM df t1,df t2 WHERE " 
        else : 
            unionOfAllPairs += " UNION SELECT t1.rowid as t1ctid ,t2.rowid as t2ctid FROM df t1,df t2 WHERE "
            unionOfAllTuples += " UNION SELECT * FROM df t1,df t2 WHERE "
            
        rep = {" ": "_", "&": " and ","not(":"",")":""} 
        rep = dict((re.escape(k), v) for k, v in rep.items()) 
        pattern = re.compile("|".join(rep.keys()))
        con1 = pattern.sub(lambda m: rep[re.escape(m.group(0))], con)     

        # in case the constraint refers to a single tuple
        if "t2" not in con: 
            con1 = re.sub(r'(t1.*?)t1', r'\1t2', con1, 1)
            unionOfAllPairs += con1 +" and t1.ROWID==t2.ROWID and ("+columnsT1+")"
            unionOfAllTuples += con1 +" and t1.ROWID==t2.ROWID and ("+columnsT1+")"
        else:
            unionOfAllPairs += con1 +" and t1.ROWID!=t2.ROWID and ("+columnsT1+" and "+columnsT2+")"
            unionOfAllTuples += con1 +" and t1.ROWID!=t2.ROWID and ("+columnsT1+" and "+columnsT2+")"
        count+=1
        
    return unionOfAllTuples,unionOfAllPairs,allColumns

def constraints_check(df,constraintSets, allColumns, unionOfAllTuples, unionOfAllPairs):
    """
    constraints_check - runs the dynamic queries that have been generated on the database.
    This function will run two queries:
    1. unionOfAllTuples - returns the ids of the tuples participating in a violation of the constraints.
    2. unionOfAllPairs - returns pairs (i1,i2) of ids of tuples that jointly violate the constraints.
    
    Parameters
    ----------
    constraintSets : set of strings
        each string represents a constraint from the dcs file
    unionOfAllTuples : string
    unionOfAllPairs : string    
    df : dataframe
        the database frame
        
    Returns
    -------
    list of two strings and two double variables:
        sdfcWithRep, sdfcNoRep are the results of the unionOfAllPairs and unionOfAllTuples queries, respectively.
        end1-start, end2-start2 are the running times the queries.
    """    
    
    # finds the pairs of tuples that jointly violate a constraint
    start = time.time()    
    violatingPairs =  psql.sqldf("SELECT DISTINCT * FROM (SELECT CASE WHEN t1ctid <= t2ctid THEN t1ctid ELSE t2ctid END AS id1,CASE WHEN t1ctid <= t2ctid THEN t2ctid ELSE t1ctid END AS id2 FROM ("+unionOfAllPairs+")AS A)AS B")
    end1 = time.time()
    
    # finds the tuples that participate in a violation
    start2 = time.time()
    #violatingTuples =  psql.sqldf("SELECT DISTINCT "+allColumns+" FROM ("+unionOfAllTuples+") AS A")
    violatingTuples = set()
    for pair in violatingPairs.values:
        for item in pair:
            violatingTuples.add(item)
    end2 = time.time()
    
    return violatingPairs, violatingTuples, end1-start, end2-start2

def first_measurer_I_D(uniquePairsDf):
    """
    first_measurer_I_D: computes the drastic inconsistency measure I_d.
    This function checks whether the result of the query that finds the violating pairs of tuples is empty.
    In case it is empty ,the database is consistent. Otherwise, it is inconsistent.
    
    Parameters
    ----------
    uniquePairsDf : dataframe
        the result of the query that finds all pairs of tuples that jointly violate a constraint.
        
    Returns
    -------
    int
        0 if database is consistent, and 1 otherwise
    """  
    if len(uniquePairsDf):
        return 1
    return 0

def second_measurer_I_MI(uniquePairsDf):
    """
    second_measurer_I_MI: computes the measure I_MI that counts the minimal inconsistent subsets of the database.
    
    Parameters
    ----------
    uniquePairsDf : dataframe
        the result of the query that finds all pairs of tuples that jointly violate a constraint.
        
    Returns
    -------
    int
        number of pairs of tuples that jointly violate a constraint.
    """ 
    
    return len(uniquePairsDf)

def third_measurer_I_P(uniqueTuplesDf):
    """
    third_measurer_I_P: computes the measure I_P that counts the number of problematic tuples 
    (tuples participating in a violation of the constraints).
    
    Parameters
    ----------
    uniqueTuplesDf : dataframe
        the result of the query that finds all tuples that particiapte in a violation.
        
    Returns
    -------
    int
        number of tuples participating in a violation of the constraints.
    """ 
    
    return len(uniqueTuplesDf)

def fourth_measurer_I_R(uniquePairsDf):
    """
    fourth_measurer_I_R: computes the measure I_R that is based on the minimal number of tuples that should
    be removed from the database for the constraints to hold.
    The measure is computed via an ILP and the Gurobi optimizer is used to solve the ILP.
    
    - There is a binary variable x for every tuple in the database.
    - The constraints are of the form x + y >= 1 where x and y represent two tuples that jointly vioalte a constraint.
    - The objective function is to minimize the sum of all x's.
    
    Parameters
    ----------
    uniquePairsDf : dataframe
        the result of the query that finds all pairs of tuples that jointly violate a constraint.
        
    Returns
    -------
    list of two int variables:
        database_measurer.objVal is the minimal number of tuples that should be removed for the constraints to hold.
        end1 - start is the running time of the function.
    """ 
    
    start = time.time()
    rows_violations = uniquePairsDf.values
    varsDict2 = {}
    database_measurer = gp.Model('Minimal deletions of tuples')
    database_measurer.setParam('OutputFlag', 0)  # do not show any comments on the screen 
    
    # variables
    for i in rows_violations :
        varsDict2[i[0]] = database_measurer.addVar(vtype=GRB.BINARY, name="x")
        varsDict2[i[1]] = database_measurer.addVar(vtype=GRB.BINARY, name="x")
    
    # constraints
    for i in rows_violations :
        database_measurer.addConstr(varsDict2[i[0]]+varsDict2[i[1]]>=1, name='con')
    vars= []
    for i in varsDict2:
        vars.append(varsDict2[i])
        
    # objective function    
    database_measurer.setObjective(sum(vars), GRB.MINIMIZE)
    
    opt = database_measurer.optimize()
    end1 = time.time()
    return database_measurer.objVal , end1 - start

def fifth_measurer_I_lin_R(uniquePairsDf):
    """
    fifth_measurer_I_lin_R: computes the measure I^lin_R that is the linear relaxation of the ILP used for computing
    the measure I_R.
    
    - There is a variable x for every tuple in the database such that 0<=x<=1.
    - The constraints are of the form x + y >= 1 where x and y represent two tuples that jointly vioalte a constraint.
    - The objective function is to minimize the sum of all x's.
    
    Parameters
    ----------
    uniquePairsDf : dataframe
        the result of the query that finds all pairs of tuples that jointly violate a constraint.
        
    Returns
    -------
    list of two int variables:
        database_measurer.objVal is the result of the LP.
        end2 - start is the running time of the function.
    """ 
    
    start = time.time()
    rows_violations = uniquePairsDf.values
    varsDict2 = {}
    database_measurer = gp.Model('Minimal deletions of tuples relaxed')
    database_measurer.setParam('OutputFlag', 0)  # do not show any comments on the screen 
    
    # variables
    for i in rows_violations :
        varsDict2[i[0]] = database_measurer.addVar(lb=0, ub=1, vtype=GRB.CONTINUOUS, name="x")
        varsDict2[i[1]] = database_measurer.addVar(lb=0, ub=1, vtype=GRB.CONTINUOUS, name="x")
    
    # constraints
    for i in rows_violations :
        database_measurer.addConstr(varsDict2[i[0]]+varsDict2[i[1]]>=1, name='con')
    vars= []
    for i in varsDict2:
        vars.append(varsDict2[i])
    
    # objective function
    database_measurer.setObjective(sum(vars), GRB.MINIMIZE)
    
    opt = database_measurer.optimize()
    end2 = time.time()
    return database_measurer.objVal , end2 -start

def sixth_measurer_I_MC(fullPath, uniquePairsDf):
    """
    sixth_measurer_I_MC: computes the measure I_MC that counts the maximal consistent subsets (i.e., repairs),
    which are also the maximal independent sets of the conflict graph wherein nodes represent tuples
    and edges represent pairs of tuples that jointly violate a constraint.
    This function generates the complement of the conflict graph (where edges represent pairs of tuples that do not 
    jointly violate any constraint). Then, an algorithm for enumearing maximal cliques in a graph is invoked.

    Parameters
    ----------
    fullPath : string
        the path of the directory where the graph will be generated
    uniquePairsDf : dataframe
         the result of the query that finds all pairs of tuples that jointly violate a constraint.
        
    Returns
    -------
    list of two int variables:
        result_output is the number of maximal cliques the algorithm generated.
        end - start is the function running time of the function.
    """
    
    start = time.time()
    rows_violations = uniquePairsDf.values
    num_of_rows = len(df.index)

    varsDict = {}
    for i in range(num_of_rows):
        varsDict[i] = num_of_rows - 1 - numpy.count_nonzero(rows_violations == i+1) 
    
    # cart_prod contains all possible edges in the graph
    all_rows1 = list(range(0, num_of_rows)) 
    all_rows2 = list(range(0, num_of_rows)) 
    cart_prod = [(a,b,1) for a in all_rows1 for b in all_rows2]
    rows_violations = rows_violations - 1
    
    # for each pair that violates the constraints turn off the valid bit
    for i in rows_violations:
        lst = list(cart_prod[i[0]*num_of_rows+i[1]])
        lst[2] = 0
        cart_prod[i[0]*num_of_rows+i[1]] = tuple(lst)
        lst = list(cart_prod[i[1]*num_of_rows+i[0]])
        lst[2] = 0
        cart_prod[i[1]*num_of_rows+i[0]] = tuple(lst)
    
    graphFileName = fullPath + '/graph.nde'

    f = open(graphFileName, "w+")
    
    # construct the nodes with their degrees [degree = number of rows - 1 - number of appereances in rows_vioalations]
    f.write(str(num_of_rows))
    for k, v in varsDict.items():
        f.write('\n'+ str(k) + ' '+ str(v))
    
    # construct the edges 
    for i in cart_prod:
        if i[2] and i[0]!=i[1]:
            f.write('\n'+str(i[0]) + ' '+ str(i[1]))
            lst = list(cart_prod[i[1]*num_of_rows+i[0]])
            lst[2] = 0
            cart_prod[i[1]*num_of_rows+i[0]] = tuple(lst)
    
    f.close()    
    
    # locate the full path to the graph and text_ui
    buildFullPath = os.path.abspath("parallel_enum/build/text_ui")
    graphFullPath = os.path.abspath(graphFileName)
    
    # invoke the algorithm for enumerating maximal cliques with the graph as a parameter
    result = run(buildFullPath+' -system="clique" '+ graphFullPath,shell=True,capture_output=True)
    results = ""
    results = result.stdout
    result_output = int((str(results.split()[14]).replace('b',"").replace("'","")))
    
    end = time.time()
    return result_output, end - start 

