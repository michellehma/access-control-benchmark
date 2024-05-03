""" Author: Michelle Ma
    Description: This program generates random access control policies for three models: ABAC, RBAC, and PBAC.
    There are four different test cases for the generation of policies:
    1. There are no policies that correspond to the permissions in the permission list.
    2. There are policies that correspond to some of the permissions in the permission list.
    3. There are policies that correspond to all the permissions in the permission list with conditions.
    4. There are policies that correspond to all the permissions in the permission list with no conditions.
    For each case, the user can choose to add as many extra policies as they wish. For case 1,
    the extra policies will involve inserting different users/subjects, roles/subject attributes, and objects.
    For case 2, 3, and 4, the extra policies will have the same user/subject, role/subject, and objects so that
    when querying for a specific user or attribute, the whole policy table will be searched. However,
    the code can easily be altered so that the addition of extra policies can more resemble case 1.
    The generated policies are added to a csv file and are loaded into MySQL databases. Each new run of this
    program will empty the csv files and databases and start anew. The refered permission list corresponds
    to each of the selections of the 22 TCP-H queries. Please read the TCP-H documentation for more information
    on these queries: https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf. 
    The query definitions begin on page 29.    
"""

import random
import re
from sqlalchemy import create_engine, text

#list of all permissions that each TCP-H query selects
permissionList = [["l_returnflag", "l_linestatus", "sum(l_quantity)", "sum(l_extendedprice)", "sum(l_discount)", "sum(l_tax)", "avg(l_quantity)", "avg(l_extendedprice)", "avg(l_discount)", "count(lineitem)"],
                  ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
                  ["l_orderkey", "sum(l_extendedprice)", "sum(l_discount)", "o_orderdate", "o_shippriority"],
                  ["o_orderpriority", "count(orders)"],
                  ["n_name", "sum(l_extendedprice)", "sum(l_discount)"],
                  ["sum(l_extendedprice)", "sum(l_discount)"],
                  ["n_name", "l_shipdate", "sum(l_extendedprice)", "sum(l_discount)"],
                  ["o_orderdate", "sum(l_extendedprice)", "sum(l_discount)"],
                  ["n_name", "o_orderdate", "sum(l_extendedprice)", "sum(l_discount)", "sum(ps_supplycost)", "sum(l_quantity)"],
                  ["c_custkey", "c_name", "sum(l_extendedprice)", "sum(l_discount)", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
                  ["ps_partkey", "sum(ps_supplycost)", "sum(ps_availqty)"],
                  ["l_shipmode", "sum(o_orderpriority)"],
                  ["count(o_orderkey)", "count(c_custkey)"],
                  ["sum(p_type)", "sum(l_extendedprice)", "sum(l_discount)"],
                  [["l_suppkey", "sum(l_extendedprice)", "sum(l_discount)"], ["s_suppkey", "s_name", "s_address", "s_phone"]],
                  ["p_brand", "p_type", "p_size", "count(ps_suppkey)"],
                  ["sum(l_extendedprice)"],
                  ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"],
                  ["sum(l_extendedprice)", "sum(l_discount)"],
                  ["s_name", "s_address"],
                  ["s_name", "count(supplier)", "count(lineitem)", "count(orders)", "count(nation)"],
                  ["c_phone", "count(customer)", "sum(c_acctbal)"]]

#random conditions for some of the columns in the dataset
conditionDict = {"s":["s_suppkey > 100", "s_suppkey < 10000", "s_nationkey > 5", "s_nationkey < 20", "s_acctbal > 10", "s_acctbal < 5000"],
                 "p":["p_partkey >= 1000", "p_partkey <= 10000", "p_size > 10", "p_size < 40", "p_retailprice > 1000", "p_retailprice < 1600", "p_type = 'PROMO BURNISHED COPPER'", "p_type = 'LARGE BRUSHED BRASS'", "p_type = 'STANDARD BURNISHED NICKEL'", "p_type = 'SMALL PLATED COPPER'", "p_type = 'SMALL ANODIZED NICKEL'", "p_container = 'WRAP CASE'", "p_container = 'MED BAG'", "p_container = 'LG DRUM'", "p_container = 'JUMBO BOX'", "p_container = 'JUMBO PACK'"],
                 "ps":["ps_partkey >= 100", "ps_partkey <= 1000", "ps_suppkey > 100", "ps_suppkey < 10000", "ps_availqty > 1000", "ps_availqty < 7000", "ps_supplycost >= 100", "ps_supplycost <= 700"],
                 "c":["c_custkey > 100", "c_custkey < 10000", "c_nationkey > 5", "c_nationkey < 15", "c_acctbal > 10", "c_acctbal < 2000", "c_mktsegment = 'BUILDING'", "c_mktsegment = 'AUTOMOBILE'", "c_mktsegment = 'HOUSEHOLD'", "c_mktsegment = 'MACHINERY'", "c_mktsegment = 'FURNITURE'"],
                 "l":["l_orderkey > 100", "l_orderkey < 10000", "l_suppkey > 100", "l_suppkey < 10000", "l_partkey >= 1000", "l_partkey <= 10000", "l_linenumber = 6", "l_linenumber = 1", "l_quantity > 10", "l_extendedprice > 10000", "l_discount > 0.06", "l_tax != 0", "l_returnflag = 'N'", "l_linestatus = 'F'", "l_shipdate between '1993-01-01' and '1996-01-01'", "l_commitdate between '1993-01-01' and '1996-01-01'", "l_shipdate between '1994-01-01' and '1996-01-01'", "l_shipinstruct != 'NONE'", "l_shipmode = 'MAIL'", "l_shipmode = 'AIR'", "l_shipmode = 'TRUCK'"],
                 "o":["o_orderkey > 100", "o_orderkey < 10000", "o_custkey > 100", "o_custkey < 5000", "o_orderstatus = 'O'", "o_totalprice < 100000", "o_orderdate between '1993-01-01' and '1996-01-01'", "o_orderpriority = '2-HIGH'", "o_orderpriority = '3-MEDIUM'"],
                 "n":["n_nationkey > 5", "n_nationkey < 20", "n_nationkey < 15", "n_regionkey >= 3", "n_regionkey < 3"]}

#adds generic policies that will go unused/unchecked
#used for test case 1, where there are no policies that provide
# access to the query selections
def addNoise1(num):
    id = 0

    #generates the generic policies
    for i in range(num):
        f = open("rbacUser.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        f = open("rbacRole.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        f = open("abacSubject.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        f = open("abacSAttribute.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        f = open("abacObject.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        f = open("abacOAttribute.csv", "a")
        f.write("attribute " + str(i) + ",\n")
        f.close()

        pbac = "{},look at notes,l_comment,,\n".format(str(id))
        rbac1 = "{},{},l_comment,,\n".format(str(id), "attribute " + str(i))
        abac1 = "{},l_comment,{},{},security 1,\n".format(str(id), "attribute " + str(i), "attribute " + str(i))
        f = open("pbacPolicy.csv", "a")
        f.write(pbac)
        f.close()

        f = open("rbacPolicy.csv", "a")
        f.write(rbac1)
        f.close()

        f = open("abacPolicy.csv", "a")
        f.write(abac1)
        f.close()

        id += 1

    #adds the permissions and conditions into the abac object and object attribute files
    for p in permissionList:
        for p1 in p:
            if isinstance(p1, list):
                for p2 in p1:
                    f = open("abacObject.csv", "a")
                    f.write(p2 + ",\n")
                    f.close()
            else:
                f = open("abacObject.csv", "a")
                f.write(p1 + ",\n")
                f.close()
    for c in conditionDict.values():
        for c1 in c:
            f = open("abacOAttribute.csv", "a")
            f.write(c1 + ",\n")
            f.close()

    f = open("abacOAttribute.csv", "a")
    f.write("admin owner,\n")
    f.close()

    return id

#deletes the content of the csv files
def clearFiles():
    open("pbacPolicy.csv", "w").close()
    open("abacPolicy.csv", "w").close()
    open("rbacPolicy.csv", "w").close()
    open("rbacUser.csv", "w").close()
    open("rbacRole.csv", "w").close()
    open("abacSubject.csv", "w").close()
    open("abacSAttribute.csv", "w").close()
    open("abacObject.csv", "w").close()
    open("abacOAttribute.csv", "w").close()

#used for the rest of the test cases to add generic policies
# that will be checked but will not match any of the necessary permissions
def addNoise2(num):
    id = 0
    user = "Alice,\n"
    role = "CEO,\n"
    abac4 = "l_comment,\n"
    abac5 = "comment,\n"

    f = open("rbacUser.csv", "a")
    f.write(user)
    f.close()

    f = open("rbacRole.csv", "a")
    f.write(role)
    f.close()

    f = open("abacSubject.csv", "a")
    f.write(user)
    f.close()

    f = open("abacSAttribute.csv", "a")
    f.write(role)
    f.close()

    f = open("abacObject.csv", "a")
    f.write(abac4)
    f.close()

    f = open("abacOAttribute.csv", "a")
    f.write(abac5)
    f.close()

    #generates the generic policies
    for i in range(num):
        pbac = "{},perform CEO tasks,l_comment,,\n".format(str(id))
        rbac1 = "{},CEO,l_comment,,\n".format(str(id))
        abac1 = "{},l_comment,CEO,comment,security 1,\n".format(str(id))
        f = open("pbacPolicy.csv", "a")
        f.write(pbac)
        f.close()

        f = open("rbacPolicy.csv", "a")
        f.write(rbac1)
        f.close()

        f = open("abacPolicy.csv", "a")
        f.write(abac1)
        f.close()

        id += 1

    #adds the permissions and conditions into the abac object and object attribute files
    for p in permissionList:
        for p1 in p:
            if isinstance(p1, list):
                for p2 in p1:
                    f = open("abacObject.csv", "a")
                    f.write(p2 + ",\n")
                    f.close()
            else:
                f = open("abacObject.csv", "a")
                f.write(p1 + ",\n")
                f.close()
    for c in conditionDict.values():
        for c1 in c:
            f = open("abacOAttribute.csv", "a")
            f.write(c1 + ",\n")
            f.close()

    f = open("abacOAttribute.csv", "a")
    f.write("admin owner,\n")
    f.close()
    return id

#creates policies for test case 2, where some policies will 
# grant permissions for the queries that will be executed
def createPolicies2(num):
    id = addNoise2(num) + 1
    abacID = id

    #add users, roles, and subject attributes
    if num == 0:
        user = "Alice,\n"
        role = "CEO,\n"

        f = open("rbacUser.csv", "a")
        f.write(user)
        f.close()

        f = open("rbacRole.csv", "a")
        f.write(role)
        f.close()

        f = open("abacSubject.csv", "a")
        f.write(user)
        f.close()

        f = open("abacSAttribute.csv", "a")
        f.write(role)
        f.close()

    used = []
    #generates policies
    for i in range(len(permissionList)):

        #deals with the special case for query 15
        if i == 14:
            #has policies for all permissions in permissionList[14][0]
            #has to have at least one policy for permissionList[14][1]
            for k in range(len(permissionList[i][0])):
                if permissionList[i][0][k] not in used:
                    condUsed = []
                    noneCount = 0
                    permission = permissionList[i][0][k]       

                    #random number of conditions
                    randNum3 = random.randint(1, 3)
                    for j in range(randNum3):
                        
                        #type of condition
                        cond = random.randint(0, 2)
                        if noneCount >= 1:
                            cond = 1
                        condition = "admin owner"
                        pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                        rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                        #gets the table type
                        splitPer = []
                        table = ""
                        if "(" in permission and "_" in permission:
                            splitPer = re.split(r"[(_]+", permission)
                            table = splitPer[1]
                        elif "(" in permission:
                            splitPer = permission.split("(")
                            table = splitPer[1][0]
                        else:
                            splitPer = permission.split("_")
                            table = splitPer[0]

                        #get random condition from dictionary
                        randNum4 = random.randint(0, len(conditionDict[table])-1)

                        if cond == 0 or cond == 1:
                            while conditionDict[table][randNum4] in condUsed:
                                randNum4 = random.randint(0, len(conditionDict[table])-1)
                            condition = conditionDict[table][randNum4]
                            
                            pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                            rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                        else:
                            noneCount += 1

                        condUsed.append(condition)

                        #for random number of environment attributes
                        randNum5 = random.randint(1, 4)
                        envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                        usedEnvir = []
                        for l in range(randNum5):

                            #random environment attribute
                            randNum6 = random.randint(0, 3)
                            while envir[randNum6] in usedEnvir:
                                randNum6 = random.randint(0, 3)

                            #creates abac policy
                            abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                            f = open("abacPolicy.csv", "a")
                            f.write(abac1)
                            f.close()
                            abacID += 1
                            usedEnvir.append(envir[randNum6])
                            
                        #adds pbac and rbac policy
                        f = open("pbacPolicy.csv", "a")
                        f.write(pbac)
                        f.close()

                        f = open("rbacPolicy.csv", "a")
                        f.write(rbac1)
                        f.close()

                        id += 1

                    used.append(permission)

            randNum = random.randint(1, len(permissionList[i][1]))
            
            #for a random number of permissions in each permission list
            for k in range(randNum):

                #random permission
                randNum2 = random.randint(0, len(permissionList[i][1])-1)
                whileCount = 0
                while permissionList[i][1][randNum2] in used:
                    if whileCount > len(permissionList[i][1]):
                        break
                    randNum2 = random.randint(0, len(permissionList[i][1])-1)
                    whileCount += 1
                condUsed = []
                noneCount = 0
                permission = permissionList[i][1][randNum2]            

                #random number of conditions
                randNum3 = random.randint(1, 3)
                for j in range(randNum3):
                    
                    #type of condition
                    cond = random.randint(0, 2)
                    if noneCount >= 1:
                        cond = 1
                    condition = "admin owner"
                    pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                    rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                    #gets the table type
                    splitPer = []
                    table = ""
                    if "(" in permission and "_" in permission:
                        splitPer = re.split(r"[(_]+", permission)
                        table = splitPer[1]
                    elif "(" in permission:
                        splitPer = permission.split("(")
                        table = splitPer[1][0]
                    else:
                        splitPer = permission.split("_")
                        table = splitPer[0]

                    #get condition from dictionary
                    randNum4 = random.randint(0, len(conditionDict[table])-1)

                    if cond == 0 or cond == 1:
                        while conditionDict[table][randNum4] in condUsed:
                            randNum4 = random.randint(0, len(conditionDict[table])-1)
                        condition = conditionDict[table][randNum4]
                        
                        pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                        rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                    else:
                        noneCount += 1

                    condUsed.append(condition)

                    #for random number of environment attributes
                    randNum5 = random.randint(1, 4)
                    envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                    usedEnvir = []
                    for l in range(randNum5):

                        #random environment attribute
                        randNum6 = random.randint(0, 3)
                        while envir[randNum6] in usedEnvir:
                            randNum6 = random.randint(0, 3)

                        #creates abac policy
                        abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                        f = open("abacPolicy.csv", "a")
                        f.write(abac1)
                        f.close()
                        abacID += 1
                        usedEnvir.append(envir[randNum6])
                        
                    #adds pbac and rbac policy
                    f = open("pbacPolicy.csv", "a")
                    f.write(pbac)
                    f.close()

                    f = open("rbacPolicy.csv", "a")
                    f.write(rbac1)
                    f.close()

                    id += 1

                used.append(permission)
        else:
            
            #for a random number of permissions in each permission list
            for k in range(2):

                #random permission
                randNum2 = random.randint(0, len(permissionList[i])-1)
                whileCount = 0
                while permissionList[i][randNum2] in used:
                    if whileCount > len(permissionList[i]):
                        break
                    randNum2 = random.randint(0, len(permissionList[i])-1)
                    whileCount += 1
                condUsed = []
                noneCount = 0
                permission = permissionList[i][randNum2]            

                #random number of conditions
                randNum3 = random.randint(1, 3)
                for j in range(randNum3):
                    
                    #type of condition
                    cond = random.randint(0, 2)
                    if noneCount >= 1:
                        cond = 1
                    condition = "admin owner"
                    pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                    rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                    #gets the table type
                    splitPer = []
                    table = ""
                    if "(" in permission and "_" in permission:
                        splitPer = re.split(r"[(_]+", permission)
                        table = splitPer[1]
                    elif "(" in permission:
                        splitPer = permission.split("(")
                        table = splitPer[1][0]
                    else:
                        splitPer = permission.split("_")
                        table = splitPer[0]

                    #get condition from dictionary
                    randNum4 = random.randint(0, len(conditionDict[table])-1)

                    if cond == 0 or cond == 1:
                        while conditionDict[table][randNum4] in condUsed:
                            randNum4 = random.randint(0, len(conditionDict[table])-1)
                        condition = conditionDict[table][randNum4]
                        
                        pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                        rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                    else:
                        noneCount += 1

                    condUsed.append(condition)

                    #for random number of environment attributes
                    randNum5 = random.randint(1, 4)
                    envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                    usedEnvir = []
                    for l in range(randNum5):

                        #random environment attribute
                        randNum6 = random.randint(0, 3)
                        while envir[randNum6] in usedEnvir:
                            randNum6 = random.randint(0, 3)

                        #creates abac policy
                        abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                        f = open("abacPolicy.csv", "a")
                        f.write(abac1)
                        f.close()
                        abacID += 1
                        usedEnvir.append(envir[randNum6])
                        
                    #adds pbac and rbac policy
                    f = open("pbacPolicy.csv", "a")
                    f.write(pbac)
                    f.close()

                    f = open("rbacPolicy.csv", "a")
                    f.write(rbac1)
                    f.close()

                    id += 1

                used.append(permission)

#creates policies for test case 3, where all permissions from the list are accounted
# for in the generated policies
def createPolicies3(num):
    id = addNoise2(num) + 1
    abacID = id

    #add users, roles, and subject attributes
    if num == 0:
        user = "Alice,\n"
        role = "CEO,\n"

        f = open("rbacUser.csv", "a")
        f.write(user)
        f.close()

        f = open("rbacRole.csv", "a")
        f.write(role)
        f.close()

        f = open("abacSubject.csv", "a")
        f.write(user)
        f.close()

        f = open("abacSAttribute.csv", "a")
        f.write(role)
        f.close()

    #generates policies
    used = []
    for i in range(len(permissionList)):

        #deals with the special case for query 15
        if i == 14:
            #has policies for all permissions in permissionList[14][0]
            #has to have at least one policy for permissionList[14][1]
            for k in range(len(permissionList[i][0])):
                if permissionList[i][0][k] not in used:
                    condUsed = []
                    noneCount = 0
                    permission = permissionList[i][0][k]       
                    
                    #random number of conditions
                    randNum3 = random.randint(1, 3)
                    for j in range(randNum3):
                        
                        #type of condition
                        cond = random.randint(0, 2)
                        if noneCount >= 1:
                            cond = 1
                        condition = "admin owner"
                        pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                        rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                        #gets the table type
                        splitPer = []
                        table = ""
                        if "(" in permission and "_" in permission:
                            splitPer = re.split(r"[(_]+", permission)
                            table = splitPer[1]
                        elif "(" in permission:
                            splitPer = permission.split("(")
                            table = splitPer[1][0]
                        else:
                            splitPer = permission.split("_")
                            table = splitPer[0]

                        #get condition from dictionary
                        randNum4 = random.randint(0, len(conditionDict[table])-1)

                        if cond == 0 or cond == 1:
                            while conditionDict[table][randNum4] in condUsed:
                                randNum4 = random.randint(0, len(conditionDict[table])-1)
                            condition = conditionDict[table][randNum4]
                            
                            pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                            rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                        else:
                            noneCount += 1

                        condUsed.append(condition)

                        #for random number of environment attributes
                        randNum5 = random.randint(1, 4)
                        envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                        usedEnvir = []
                        for l in range(randNum5):

                            #random environment attribute
                            randNum6 = random.randint(0, 3)
                            while envir[randNum6] in usedEnvir:
                                randNum6 = random.randint(0, 3)

                            #creates abac policy
                            abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                            f = open("abacPolicy.csv", "a")
                            f.write(abac1)
                            f.close()
                            abacID += 1
                            usedEnvir.append(envir[randNum6])
                            
                        #adds pbac and rbac policy
                        f = open("pbacPolicy.csv", "a")
                        f.write(pbac)
                        f.close()

                        f = open("rbacPolicy.csv", "a")
                        f.write(rbac1)
                        f.close()

                        id += 1

                    used.append(permission)
            
            #for a random number of permissions in each permission list
            for k in range(len(permissionList[i][1])):
                if permissionList[i][1][k] not in used:
                    condUsed = []
                    noneCount = 0
                    permission = permissionList[i][1][k]            

                    #random number of conditions
                    randNum3 = random.randint(1, 3)
                    for j in range(randNum3):
                        
                        #type of condition
                        cond = random.randint(0, 2)
                        if noneCount >= 1:
                            cond = 1
                        condition = "admin owner"
                        pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                        rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                        #gets the table type
                        splitPer = []
                        table = ""
                        if "(" in permission and "_" in permission:
                            splitPer = re.split(r"[(_]+", permission)
                            table = splitPer[1]
                        elif "(" in permission:
                            splitPer = permission.split("(")
                            table = splitPer[1][0]
                        else:
                            splitPer = permission.split("_")
                            table = splitPer[0]

                        #get condition from dictionary
                        randNum4 = random.randint(0, len(conditionDict[table])-1)

                        if cond == 0 or cond == 1:
                            while conditionDict[table][randNum4] in condUsed:
                                randNum4 = random.randint(0, len(conditionDict[table])-1)
                            condition = conditionDict[table][randNum4]
                            
                            pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                            rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                        else:
                            noneCount += 1

                        condUsed.append(condition)

                        #for random number of environment attributes
                        randNum5 = random.randint(1, 4)
                        envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                        usedEnvir = []
                        for l in range(randNum5):

                            #random environment attribute
                            randNum6 = random.randint(0, 3)
                            while envir[randNum6] in usedEnvir:
                                randNum6 = random.randint(0, 3)

                            #creates abac policy
                            abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                            f = open("abacPolicy.csv", "a")
                            f.write(abac1)
                            f.close()
                            abacID += 1
                            usedEnvir.append(envir[randNum6])
                            
                        #adds pbac and rbac policy
                        f = open("pbacPolicy.csv", "a")
                        f.write(pbac)
                        f.close()

                        f = open("rbacPolicy.csv", "a")
                        f.write(rbac1)
                        f.close()

                        id += 1

                    used.append(permission)
        else:
            #for all permissions in each permission list
            for k in range(len(permissionList[i])):
                condUsed = []
                noneCount = 0
                
                permission = permissionList[i][k]
                if permission not in used:

                    #random number of conditions
                    randNum3 = random.randint(1, 3)
                    for j in range(randNum3):
                        
                        #type of condition
                        cond = random.randint(0, 2)
                        if noneCount >= 1:
                            cond = 1
                        condition = "admin owner"
                        pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                        rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                        #gets the table type
                        splitPer = []
                        table = ""
                        if "(" in permission and "_" in permission:
                            splitPer = re.split(r"[(_]+", permission)
                            table = splitPer[1]
                        elif "(" in permission:
                            splitPer = permission.split("(")
                            table = splitPer[1][0]
                        else:
                            splitPer = permission.split("_")
                            table = splitPer[0]

                        #get condition from dictionary
                        randNum4 = random.randint(0, len(conditionDict[table])-1)

                        if cond == 0 or cond == 1:
                            while conditionDict[table][randNum4] in condUsed:
                                randNum4 = random.randint(0, len(conditionDict[table])-1)
                            condition = conditionDict[table][randNum4]
                            
                            pbac = "{},perform CEO tasks,{},{},\n".format(str(id), permission, condition)
                            rbac1 = "{},CEO,{},{},\n".format(str(id), permission, condition)
                        else:
                            noneCount += 1

                        condUsed.append(condition)

                        #for random number of environment attributes
                        randNum5 = random.randint(1, 4)
                        envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                        usedEnvir = []
                        for l in range(randNum5):

                            #random environment attribute
                            randNum6 = random.randint(0, 3)
                            while envir[randNum6] in usedEnvir:
                                randNum6 = random.randint(0, 3)

                            #creates abac policy
                            abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                            f = open("abacPolicy.csv", "a")
                            f.write(abac1)
                            f.close()
                            abacID += 1
                            usedEnvir.append(envir[randNum6])
                            
                        #adds pbac and rbac policy
                        f = open("pbacPolicy.csv", "a")
                        f.write(pbac)
                        f.close()

                        f = open("rbacPolicy.csv", "a")
                        f.write(rbac1)
                        f.close()

                        id += 1

                    used.append(permission)

#creates policies for test case 4, where all permissions from the list are accounted for
# except no conditions are assigned to the policies
def createPolicies4(num):
    id = addNoise2(num) + 1
    abacID = id

    #add users, roles, and subject attributes
    if num == 0:
        user = "Alice,\n"
        role = "CEO,\n"

        f = open("rbacUser.csv", "a")
        f.write(user)
        f.close()

        f = open("rbacRole.csv", "a")
        f.write(role)
        f.close()

        f = open("abacSubject.csv", "a")
        f.write(user)
        f.close()

        f = open("abacSAttribute.csv", "a")
        f.write(role)
        f.close()

    #generates policies
    used = []
    for i in range(len(permissionList)):

        #deals with the special case for query 15
        if i == 14:
            #has policies for all permissions in permissionList[14][0]
            #has to have at least one policy for permissionList[14][1]
            for k in range(len(permissionList[i][0])):
                if permissionList[i][0][k] not in used:
                    permission = permissionList[i][0][k]       
                    condition = "admin owner"
                    pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                    rbac1 = "{},CEO,{},,\n".format(str(id), permission)
                    
                    #for random number of environment attributes
                    randNum5 = random.randint(1, 4)
                    envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                    usedEnvir = []
                    for l in range(randNum5):

                        #random environment attribute
                        randNum6 = random.randint(0, 3)
                        while envir[randNum6] in usedEnvir:
                            randNum6 = random.randint(0, 3)

                        #creates abac policy
                        abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                        f = open("abacPolicy.csv", "a")
                        f.write(abac1)
                        f.close()
                        abacID += 1
                        usedEnvir.append(envir[randNum6])
                        
                    #adds pbac and rbac policy
                    f = open("pbacPolicy.csv", "a")
                    f.write(pbac)
                    f.close()

                    f = open("rbacPolicy.csv", "a")
                    f.write(rbac1)
                    f.close()

                    id += 1

                used.append(permission)
            
            for k in range(len(permissionList[i][1])):
                if permissionList[i][1][k] not in used:
                    condition = "admin owner"
                    pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                    rbac1 = "{},CEO,{},,\n".format(str(id), permission)

                    #for random number of environment attributes
                    randNum5 = random.randint(1, 4)
                    envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                    usedEnvir = []
                    for l in range(randNum5):

                        #random environment attribute
                        randNum6 = random.randint(0, 3)
                        while envir[randNum6] in usedEnvir:
                            randNum6 = random.randint(0, 3)

                        #creates abac policy
                        abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                        f = open("abacPolicy.csv", "a")
                        f.write(abac1)
                        f.close()
                        abacID += 1
                        usedEnvir.append(envir[randNum6])
                        
                    #adds pbac and rbac policy
                    f = open("pbacPolicy.csv", "a")
                    f.write(pbac)
                    f.close()

                    f = open("rbacPolicy.csv", "a")
                    f.write(rbac1)
                    f.close()

                    id += 1

                used.append(permission)
        else:
            #for all permissions in each permission list
            for k in range(len(permissionList[i])):
                permission = permissionList[i][k]
                if permission not in used:

                    condition = "admin owner"
                    pbac = "{},perform CEO tasks,{},,\n".format(str(id), permission)
                    rbac1 = "{},CEO,{},,\n".format(str(id), permission)
                    
                    #for random number of environment attributes
                    randNum5 = random.randint(1, 4)
                    envir = ["5/20/2020", "security 1", "morning", "hp laptop"]
                    usedEnvir = []
                    for l in range(randNum5):

                        #random environment attribute
                        randNum6 = random.randint(0, 3)
                        while envir[randNum6] in usedEnvir:
                            randNum6 = random.randint(0, 3)

                        #creates abac policy
                        abac1 = "{},{},CEO,{},{},\n".format(str(abacID), permission, condition, envir[randNum6])
                        f = open("abacPolicy.csv", "a")
                        f.write(abac1)
                        f.close()
                        abacID += 1
                        usedEnvir.append(envir[randNum6])
                        
                    #adds pbac and rbac policy
                    f = open("pbacPolicy.csv", "a")
                    f.write(pbac)
                    f.close()

                    f = open("rbacPolicy.csv", "a")
                    f.write(rbac1)
                    f.close()

                    id += 1

                used.append(permission)

#loads the csv files into the access control model databases
#when executing the LOAD DATA operation, the correct file path to the csv files must be given
def loadFiles(num, choice):
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rbac')
    with engine.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/abac')
    with engine.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/pbac')
    with engine.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
    engine1 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/rbac?local_infile=1", echo=True)
    with engine1.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/rbacUser.csv' INTO TABLE user FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/rbacRole.csv' INTO TABLE role FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        if choice == 1:
            for i in range(num):
                conn.execute(text("INSERT INTO assignment (u_name, r_name) VALUES ('{}', '{}');".format("attribute " + str(i), "attribute " + str(i))))
                conn.commit()
        if num != 0 or choice != 1:
            conn.execute(text("INSERT INTO assignment (u_name, r_name) VALUES ('Alice', 'CEO');"))
            conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/rbacPolicy.csv' INTO TABLE policy FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
    engine2 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/abac?local_infile=1", echo=True)
    with engine2.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/abacSubject.csv' INTO TABLE subject FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/abacSAttribute.csv' INTO TABLE s_attributes FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/abacObject.csv' INTO TABLE object FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/abacOAttribute.csv' INTO TABLE o_attributes FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
        if choice == 1:
            for i in range(num):
                conn.execute(text("INSERT INTO s_assignment (s_name, s_attribute) VALUES ('{}', '{}');".format("attribute " + str(i), "attribute " + str(i))))
                conn.commit()
        if num != 0 or choice != 1:
            conn.execute(text("INSERT INTO s_assignment (s_name, s_attribute) VALUES ('Alice', 'CEO');"))
            conn.commit()
            for i in permissionList:
                for permission in i:
                    splitPer = []
                    table = ""
                    if isinstance(permission, list):
                        for permission2 in permission:
                            if "(" in permission2 and "_" in permission2:
                                splitPer = re.split(r"[(_]+", permission2)
                                table = splitPer[1]
                            elif "(" in permission2:
                                splitPer = permission2.split("(")
                                table = splitPer[1][0]
                            else:
                                splitPer = permission2.split("_")
                                table = splitPer[0]
                            for condition in conditionDict[table]:
                                conn.execute(text("INSERT INTO o_assignment (o_name, o_attribute) VALUES ('{}', \"{}\");".format(permission2, condition)))
                                conn.commit()
                            conn.execute(text("INSERT INTO o_assignment (o_name, o_attribute) VALUES ('{}', 'admin owner');".format(permission2)))
                            conn.commit()
                    else:
                        if "(" in permission and "_" in permission:
                            splitPer = re.split(r"[(_]+", permission)
                            table = splitPer[1]
                        elif "(" in permission:
                            splitPer = permission.split("(")
                            table = splitPer[1][0]
                        else:
                            splitPer = permission.split("_")
                            table = splitPer[0]
                        for condition in conditionDict[table]:
                            conn.execute(text("INSERT INTO o_assignment (o_name, o_attribute) VALUES ('{}', \"{}\");".format(permission, condition)))
                            conn.commit()
                        conn.execute(text("INSERT INTO o_assignment (o_name, o_attribute) VALUES ('{}', 'admin owner');".format(permission)))
                        conn.commit()
        if choice == 1:
            for i in range(num):
                conn.execute(text("INSERT INTO o_assignment (o_name, o_attribute) VALUES ('{}', '{}');".format("attribute " + str(i), "attribute " + str(i))))
                conn.commit()
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/abacPolicy.csv' INTO TABLE policy FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()
    engine3 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/pbac?local_infile=1", echo=True)
    with engine3.connect() as conn:
        conn.execute(text("SET GLOBAL local_infile=1;"))
        conn.execute(text("LOAD DATA LOCAL INFILE '/Users/miche/Documents/Spring 24/Cmsc 491/Project/pbacPolicy.csv' INTO TABLE policy FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"))
        conn.commit()

#deletes all the records in each access control database
def truncate():
    engine1 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/rbac", echo=True)
    with engine1.connect() as conn:
        conn.execute(text("DELETE FROM policy;"))
        conn.commit()
        conn.execute(text("DELETE FROM assignment;"))
        conn.commit()
        conn.execute(text("DELETE FROM user;"))
        conn.commit()
        conn.execute(text("DELETE FROM role;"))
        conn.commit()
    engine2 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/abac", echo=True)
    with engine2.connect() as conn:
        conn.execute(text("DELETE FROM policy;"))
        conn.commit()
        conn.execute(text("DELETE FROM s_assignment;"))
        conn.commit()
        conn.execute(text("DELETE FROM s_attributes;"))
        conn.commit()
        conn.execute(text("DELETE FROM subject;"))
        conn.commit()
        conn.execute(text("DELETE FROM o_assignment;"))
        conn.commit()
        conn.execute(text("DELETE FROM o_attributes;"))
        conn.commit()
        conn.execute(text("DELETE FROM object;"))
        conn.commit()
    engine3 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/pbac", echo=True)
    with engine3.connect() as conn:
        conn.execute(text("TRUNCATE TABLE policy;"))

#driver to execute the different test cases
action = input("Pick test case: 1, 2, 3, 4\n")
if action == "1":
    num = input("Enter number of extra policies to add: \n")
    truncate()
    clearFiles()
    addNoise1(int(num))

    if num != '0':
        user = "Alice,\n"
        role = "CEO,\n"

        f = open("rbacUser.csv", "a")
        f.write(user)
        f.close()
        f = open("abacSubject.csv", "a")
        f.write(user)
        f.close()

        f = open("rbacRole.csv", "a")
        f.write(role)
        f.close()
        f = open("abacSAttribute.csv", "a")
        f.write(role)
        f.close()
    loadFiles(int(num), 1)
elif action == "2":
    num = input("Enter number of extra policies to add: \n")
    truncate()
    clearFiles()
    createPolicies2(int(num))
    loadFiles(int(num), 2)
elif action == "3":
    num = input("Enter number of extra policies to add: \n")
    truncate()
    clearFiles()
    createPolicies3(int(num))
    loadFiles(int(num), 3)
elif action == "4":
    num = input("Enter number of extra policies to add: \n")
    truncate()
    clearFiles()
    createPolicies4(int(num))
    loadFiles(int(num), 4)

