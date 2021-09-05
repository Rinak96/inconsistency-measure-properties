import random
import pandas as pd
import re
import numpy as numpy

def equals_handler(rowA, fieldA, rowB, fieldB, df, t1, t2):
    """
    Handler function for the case of the equality operator (=) as well as for the <= and >= operators.
    In case the condition is of the form t.A=t'.B, the function will update the value of t'.B to the value of t.A.

    Parameters
    ----------
    rowA : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the left-hand side of the = operator)
    rowB : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the right-hand side of the = operator)
    fieldA : string
        the attribute of the first tuple
    fieldB : string
        the attribute of the second tuple
    t1 : 
    	the database tuple corresponding to rowA
    t2: 
    	the database tuple corresponding to rowB
    df : dataframe
        the database frame

    Returns
    -------
    the updated tuples t1 and t2
    """

    if rowA == "t1" and rowB == "t2":
        value = getattr(t1, fieldA)
        setattr(t2, fieldB, value)
    if rowA == "t2" and rowB == "t1":
        value = getattr(t2, fieldA)
        setattr(t1, fieldB, value)
    if rowA == "t1" and rowB == "t1":
        value = getattr(t1, fieldA)
        setattr(t1, fieldB, value)
    if rowA == "t2" and rowB == "t2":
        value = getattr(t2, fieldA)
        setattr(t2, fieldB, value)
    return t1,t2

def aux_not_equal_handler(rowA, fieldA, rowB, fieldB, comp, df, t1 , t2):
    """
    An auxilary function for the disequality handler.
    The function will find an appropriate value from the active domain of the given attribute if such a value
    exists or choose a random value (that depends on the type of the object) otherwise.

    Parameters
    ----------
    rowA : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the left-hand side of the = operator)
    rowB : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the right-hand side of the = operator)
    fieldA : string
        the attribute of the first tuple
    fieldB : string
        the attribute of the second tuple
    comp : object
        the object of the chosen attribute
    t1 : 
    	the database tuple corresponding to rowA
    t2 : 
    	the database tuple corresponding to rowB
    df : dataframe
        the database frame

    Returns
    -------
    object:
        a new value for the chosen attribute
    """

    attr_set = df[fieldA].unique()
    attr_set = attr_set[attr_set != comp]
    if len(attr_set) > 0:
        val = random.choice(attr_set)
    else:
        old_val = getattr(t1, fieldA)

        if(type(old_val) is str):
            val = old_val + "1"

        elif(type(old_val) is numpy.int64 or type(old_val) is numpy.float64 or type(old_val) is numpy.double64):
            val = random.uniform(comp+1, comp+100)

        elif(type(old_val) is date):
            start_date = old_val
            end_date = datetime.today
            time_between_dates = end_date - start_date
            days_between_dates = time_between_dates.days
            random_number_of_days = random.randrange(days_between_dates)
            val = start_date + datetime.timedelta(days=random_number_of_days)
    return val

def not_equal_handler(rowA, fieldA, rowB, fieldB, df, t1, t2):
    """
    Handler function for the case of the disequality operator (!=).
    In case the condition is of the form t.A!=t'.B, the function will update the value of t.A to a different value
    from the active domain of A that is different from t'.B, if such a value exists, and to a random value otherwise.

    Parameters
    ----------
    rowA : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the left-hand side of the = operator)
    rowB : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the right-hand side of the = operator)
    fieldA : string
        the attribute of the first tuple
    fieldB : string
        the attribute of the second tuple
    t1 : 
    	the database tuple corresponding to rowA
    t2 : 
    	the database tuple corresponding to rowB
    df : dataframe
        the database frame

    Returns
    -------
    the updated tuples t1 and t2
    """

    if rowA == "t1":
        # in case the violation already exists
        if getattr(t1, fieldA) != getattr(t2, fieldB):
            return t1,t2
        comp = getattr(t1, fieldA)
        val = aux_not_equal_handler(rowA, fieldA, rowB, fieldB, comp, df, t1, t2)
        setattr(t1, fieldA, val)
        return t1,t2

    if rowA == "t2":
        # in case the violation already exists
        if getattr(t2, fieldA) != getattr(t1, fieldB):
            return t1,t2
        comp = getattr(t2, fieldA)
        val = aux_not_equal_handler(rowA, fieldA, rowB, fieldB, comp, df, t1, t2)
        setattr(t2, fieldA, val)
        return t1,t2

def less_more_handler(rowA, fieldA, rowB, fieldB, op, df, t1, t2):
    """
    Handler function for the case of the less or more operators (< or >).
    In case the condition is of the form t.A<t'.B or t.A>t'.B, the function will update the value of t.A to a different value
    from the active domain of A that sarisfies the condition with t'.B, if such a value exists, and to a random value otherwise.

    Parameters
    ----------
    rowA : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the left-hand side of the = operator)
    rowB : string
        either t1 (in which case the tuple t1 is considered on the left-hand side of the = operator)
        or t2 (in which case the tuple t2 is considered on the right-hand side of the = operator)
    fieldA : string
        the attribute of the first tuple
    fieldB : string
        the attribute of the second tuple
    op: string
    	the comparison operator
    t1 : 
    	the database tuple corresponding to rowA
    t2 : 
    	the database tuple corresponding to rowB
    df : dataframe
        the database frame

    Returns
    -------
    the updated tuples t1 and t2
    """

    if rowA == "t1":
        if op == ">":
            # in case the violation already exists
            if getattr(t1, fieldA) > getattr(t2, fieldB):
                return t1,t2
            attr_set = df[fieldA].unique()
            comp = getattr(t2, fieldB)
            attr_set = attr_set[attr_set > comp]
            if len(attr_set) > 0:
                val = random.choice(attr_set)
            else:
                val = random.uniform(comp+1, comp+100)
            setattr(t1, fieldA, val)
            return t1,t2

        if op == "<":
            # in case the violation already exists
            if getattr(t1, fieldA) < getattr(t2, fieldB):
                return t1,t2
            attr_set = df[fieldA].unique()
            comp = getattr(t2, fieldB)
            attr_set = attr_set[attr_set < comp]
            if len(attr_set) > 0:
                val = random.choice(attr_set)
            else:
                val = random.uniform(comp-100, comp-1)
            setattr(t1, fieldA, val)
            return t1,t2

    if rowA == "t2":
        if op == ">":
            # in case the violation already exists
            if getattr(t2, fieldA) > getattr(t1, fieldB):
                return t1,t2
            attr_set = df[fieldA].unique()
            comp = getattr(t1, fieldB)
            attr_set = attr_set[attr_set > comp]
            if len(attr_set) > 0:
                val = random.choice(attr_set)
            else:
                val = random.uniform(comp+1, comp+100)
            setattr(t2, fieldA, val)
            return t1,t2

        if op == "<":
            # in case the violation already exists
            if getattr(t2, fieldA) < getattr(t1, fieldB):
                return t1,t2
            attr_set = df[fieldA].unique()
            comp = getattr(t1, fieldB)
            attr_set = attr_set[attr_set < comp]
            if len(attr_set) > 0:
                val = random.choice(attr_set)
            else:
                val = random.uniform(comp-100, comp-1)
            setattr(t2, fieldA, val)
            return t1,t2

def fittingViolationAlgorithm(constraintSet,df,t1,t2):
    """
    fittingViolationAlgorithm - changes the database to violate a given constraints.
	For each condition in constraintSet, an appropriate function will be used to ensure that the selected tuples
	jointly satisfy the condition, based on the operator.
	When all the conditions are satisfied, the constraint is violated.

    Parameters
    ----------
    constraintSet : set of string values
    	each string represents a single condition in the selected denial constraint.
    df : dataframe
        the database frame
    t1 : 
    	the first database tuple
    t2 : 
    	the second database tuple

    Returns
    -------
    the updated tuples t1 and t2
    """

    for cst in constraintSet:
        op = cst[1]
        rowA, fieldA = cst[0].split(".")
        rowB, fieldB = cst[2].split(".")
        coin = random.randint(1, 2)
        if op == "=" or op == ">=" or op == "<=" :
            if coin == 1:
                t = equals_handler(rowA, fieldA, rowB, fieldB, df, t1, t2)
            if coin == 2:
                t = equals_handler(rowB, fieldB, rowA, fieldA, df, t1, t2)
        if op == "!=" :
            if coin == 1:
                t = not_equal_handler(rowA, fieldA, rowB, fieldB, df, t1, t2)
            if coin == 2:
                t = not_equal_handler(rowB, fieldB, rowA, fieldA, df, t1, t2)
        if op == ">" or op == "<" :
            if coin == 1:
                t = less_more_handler(rowA, fieldA, rowB, fieldB, op, df, t1, t2)
            if coin == 2:
                t = less_more_handler(rowB, fieldB, rowA, fieldA, op, df, t1, t2)
    return t


def updateTable(df,t1,t2,sample):
    df.loc[sample.index[0]] = list(t1)
    df.loc[sample.index[1]] = list(t2)
