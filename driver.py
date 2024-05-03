""" Author: Michelle Ma
    Description: This program is a driver to execute the 22 TPC-H queries and measure the performance of 
    three access control models: ABAC, RBAC, PBAC using different metrics such as runtime and memory. There is
    also an option to execute the queries without any access control at all. The access control models are implemented
    through a permission database for each model using MySQL, and the dataset for the querying was generated based on
    the TPC-H dbgen program (refer to https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf). When
    executing the queries with access control implemented, before each query is executed, the program checks
    the permission database for policies and sees if any policies allows for the selection items to be accessed. If not,
    modifications are made to the query so that only the allowed selections are executed. If nothing is allowed,
    the query is not executed at all. Modifications to the query may depend on the model. For example, if rbac
    or pbac policies have conditions, a view must be created to accomodate for them. Only one model cam be used and 
    only one run of the 22 queries can be done at one time. Choosing to get memory will only output the memory in mb
    of the model's database. Data from the execution of the queries gets outputed to data1.csv when running without
    access control and data.csv when running with access control. 
"""

from sqlalchemy import create_engine, text
import re
import time

#all of the 22 TCP-H queries (https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)
queryList = ["select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1998-12-01' - interval 90 day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus",
             "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100;",
             "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;",
             "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= '1993-07-01' and o_orderdate < '1993-07-01' + interval 3 month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority;",
             "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1994-01-01' + interval 1 year group by n_name order by revenue desc;",
             "select sum(l_extendedprice*l_discount) as revenue from lineitem where l_shipdate >= '1994-01-01' and l_shipdate < '1994-01-01' + interval 1 year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24;",
             "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between '1995-01-01' and '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;",
             "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year;",
             "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc;",
             "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-10-01' and o_orderdate < '1993-10-01' + interval 3 month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20;",
             "select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by value desc;",
             "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1994-01-01' + interval 1 year group by l_shipmode order by l_shipmode;",
             "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;",
             "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= '1995-09-01' and l_shipdate < '1995-09-01' + interval 1 month;",
             "create view revenue0 (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= '1996-01-01' and l_shipdate < '1996-01-01' + interval 3 month group by l_suppkey; select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, revenue0 where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue0 ) order by s_suppkey; drop view revenue0;",
             "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size;",
             "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey );",
             "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100;",
             "select sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON');",
             "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= '1994-01-01' and l_shipdate < '1994-01-01' + interval 1 year ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;",
             "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100;",
             "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;"]

#all of the permissions that the TCP-H queries select
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
                  ["l_suppkey", "sum(l_extendedprice)", "sum(l_discount)", "s_suppkey", "s_name", "s_address", "s_phone"],
                  ["p_brand", "p_type", "p_size", "count(ps_suppkey)"],
                  ["sum(l_extendedprice)"],
                  ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"],
                  ["sum(l_extendedprice)", "sum(l_discount)"],
                  ["s_name", "s_address"],
                  ["s_name", "count(supplier)", "count(lineitem)", "count(orders)", "count(nation)"],
                  ["c_phone", "count(customer)", "sum(c_acctbal)"]]

#the permissions' corresponding text from the query
selectList = [["l_returnflag, ", "l_linestatus, ", "sum(l_quantity) as sum_qty, ", "sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, ", "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, ", "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, ", "avg(l_quantity) as avg_qty, ", "avg(l_extendedprice) as avg_price, ", "avg(l_discount) as avg_disc, ", "count(*) as count_order "],
              ["s_acctbal, ", "s_name, ", "n_name, ", "p_partkey, ", "p_mfgr, ", "s_address, ", "s_phone, ", "s_comment "],
              ["l_orderkey, ", "sum(l_extendedprice*(1-l_discount)) as revenue, ", "sum(l_extendedprice*(1-l_discount)) as revenue, ", "o_orderdate, ", "o_shippriority "],
              ["o_orderpriority, ", "count(*) as order_count "],
              ["n_name, ", "sum(l_extendedprice * (1 - l_discount)) as revenue, ", "sum(l_extendedprice * (1 - l_discount)) as revenue "],
              ["sum(l_extendedprice*l_discount) as revenue ", "sum(l_extendedprice*l_discount) as revenue "],
              ["supp_nation, cust_nation, ", "l_year, ", "sum(volume) as revenue ", "sum(volume) as revenue "],
              ["o_year, ", "sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share ", "sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share "],
              ["nation, ", "o_year, ", "sum(amount) as sum_profit ", "sum(amount) as sum_profit ", "sum(amount) as sum_profit ", "sum(amount) as sum_profit "],
              ["c_custkey, ", "c_name, ", "sum(l_extendedprice * (1 - l_discount)) as revenue, ", "sum(l_extendedprice * (1 - l_discount)) as revenue, ", "c_acctbal, ", "n_name, ", "c_address, ", "c_phone, ", "c_comment "],
              ["ps_partkey, ", "sum(ps_supplycost * ps_availqty) as value ", "sum(ps_supplycost * ps_availqty) as value "],
              ["l_shipmode, ", "sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count "],
              ["c_count, ", "count(*) as custdist "],
              ["100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue ", "100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue ", "100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "],
              ["l_suppkey, ", "sum(l_extendedprice * (1 - l_discount)) ", "sum(l_extendedprice * (1 - l_discount)) ", "s_suppkey, ", "s_name, ", "s_address, ", "s_phone, ", "total_revenue "],
              ["p_brand, ", "p_type, ", "p_size, ", "count(distinct ps_suppkey) as supplier_cnt "],
              ["sum(l_extendedprice) / 7.0 as avg_yearly "],
              ["c_name, ", "c_custkey, ", "o_orderkey, ", "o_orderdate, ", "o_totalprice, ", "sum(l_quantity) "],
              ["sum(l_extendedprice * (1 - l_discount) ) as revenue ", "sum(l_extendedprice * (1 - l_discount) ) as revenue "],
              ["s_name, ", "s_address "],
              ["s_name, ", "count(*) as numwait ", "count(*) as numwait ", "count(*) as numwait ", "count(*) as numwait "],
              ["cntrycode, ", "count(*) as numcust, ", "sum(c_acctbal) as totacctbal "]]

engine1 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/business", echo=True)

#determines which access control model to use
model = input("Enter number for access control model \n1. ABAC \n2. RBAC \n3. PBAC\n")
while model != '1' and model != '2' and model != '3':
    model = input("Enter number for access control model \n1. ABAC \n2. RBAC \n3. Contextual RBAC\n4. CT-RBAC \n5. OT-ABAC \n6. PBAC\n")
statement = ""
if model == '1':
    statement = "mysql+pymysql://root:root@127.0.0.1:3306/abac"
elif model == '2':
    statement = "mysql+pymysql://root:root@127.0.0.1:3306/rbac"
elif model == '3':
    statement = "mysql+pymysql://root:root@127.0.0.1:3306/pbac"

engine2 = create_engine(statement, echo=True)
engine3 = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/")


""" queries the abac database to get the policies that correspond to the 
    current query's selections and compares the subject, object, and 
    environment attributes of the query results with the user's, selection's, 
    and current environment's attributes. The attributes are currently set
    to default values to align with the policy generator, but they can be changed
    or set as commented out below. 

    returns a dictionary of selections that are allowed, along with the
    conditions that correspond to them if there are any
"""
def checkABAC(listCol, query):
    allowed = []
    allowedDict = {}
    queryCount = 0
    appliedCount = 0
    with engine2.connect() as conn:
        subject = "Alice"
        environment = "5/20/2020, security 1, morning, hp laptop"
        #subject = input("Enter subject: ")
        #object = input("Enter object attribute (if more than one, separate with a comma and a space): ")
        #environment = input("Enter environment attributes (if more than one, separate with a comma and a space): ")
        query1 = "select s_attribute from s_assignment where s_name='{}'".format(subject)
        result1 = conn.execute(text(query1))
        queryCount += 1
        setResult1 = set(result1)
        strResult1 = str(setResult1)
        strResult1 = strResult1.replace("{(", "").replace(",)}", "").replace(",)", "").replace(" (", "")
        strResult1 = strResult1.replace("'", "")\
        
        #list of the user's subject attributes
        listResult1 = strResult1.split(",")
        if strResult1 == "set()":
            listResult1 = []
            listResult1.append("'" + subject + "'")

        #list of object attributes from the given query
        listResult2 = []
        for object in listCol:
            query2 = "select o_attribute from o_assignment where o_name='{}'".format(object)
            result2 = conn.execute(text(query2))
            oAttr = []
            for row in result2:
                oAttr.append(row.o_attribute)
            listResult2.append(oAttr)
            queryCount += 1

        #list of the user's environment attributes
        listResult3 = environment.split(", ")

        if listResult1 and listResult2 and listResult3: 
            perS = []
            perO = []
            perE = []

            #retrieves all the attributes from the query's selections
            for i in listCol:
                sList = []
                oList = []
                eList = []
                query4 = "select s_attribute, o_attribute, e_attribute from policy where permission='{}'".format(i)
                result4 = conn.execute(text(query4))
                queryCount += 1
                for row in result4:
                    sList.append(row.s_attribute)
                    oList.append(row.o_attribute)
                    eList.append(row.e_attribute)
                perS.append(sList)
                perO.append(oList)
                perE.append(eList)

            #compares given/current attributes to the query's attributes 
            #if they match, the selection item is added to the allowed list
            for sub in range(len(perS)):
                intersection_set = set(perS[sub]).intersection(set(listResult1))
                if len(intersection_set)==len(set(perS[sub])):
                    allowed.append(listCol[sub])
            for en in range(len(perE)):
                intersection_set = set(perE[en]).intersection(set(listResult3))
                if len(intersection_set)!=len(set(perE[en])) and listCol[en] in allowed:
                    allowed.remove(listCol[en])
            for ob in range(len(perO)):
                intersection_set = set(perO[ob]).intersection(set(listResult2[ob]))
                if len(intersection_set)!=len(set(perO[ob])) and listCol[ob] in allowed:
                    allowed.remove(listCol[ob]) 
                elif len(intersection_set)==len(set(perO[ob])) and listCol[ob] in allowed:
                    
                    #adds to the allowed list and dictionary
                    for o in perO[ob]:
                        if o != "any" and ("<" in o or ">" in o or "=" in o or "!=" in o or "between" in o):
                            if listCol[ob] in allowedDict.keys():
                                if "n1" in query and "n_" in o:
                                    o = o.replace("n_", "n1.n_")
                                if allowedDict[listCol[ob]] != None:
                                    allowedDict[listCol[ob]] = allowedDict[listCol[ob]] + " and " + o
                                else:
                                    allowedDict[listCol[ob]] = o
                                appliedCount += 1
                            else:
                                if "n1" in query and "n_" in o:
                                    o = o.replace("n_", "n1.n_")
                                allowedDict[listCol[ob]] = o
                                appliedCount += 1
                        else:
                            if listCol[ob] not in allowedDict.keys():
                                allowedDict[listCol[ob]] = None
                                appliedCount += 1  
            #prints and saves the number of queries made to the permission database and
            # the number of policies that corresponded to the query;
            # returns the allowed dictionary
            f = open("data.csv", "a")
            f.write(str(queryCount) + "," + str(appliedCount) + ",")
            f.close()
            print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
            print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
            return allowedDict
        #returns empty dictionary if there are no attributes at all to compare
        else:
            f = open("data.csv", "a")
            f.write(str(queryCount) + "," + str(appliedCount) + ",")
            f.close()
            print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
            print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
            return allowedDict
            
""" queries the rbac database to get the roles that correspond to the 
    user and the permissions that the roles have. The permissions are 
    compared to the selections of the query and if they match, they are added
    to the allowed dictionary with their conditions if they have any. The user 
    is currently set to default value to align with the policy generator, but 
    it can be changed or set as commented out below. 

    returns a dictionary of selections that are allowed, along with the
    conditions that correspond to them if there are any
"""
def checkRBAC(listCol, query):
    allowed = {}
    dictResult = {}
    queryCount = 0
    appliedCount = 0
    with engine2.connect() as conn:
        #user = input("Enter your user/name: ")
        user = "Alice"

        #retrieves the roles of the user
        query2 = "select r_name from assignment where u_name='{}'".format(user)
        result2 = conn.execute(text(query2))
        queryCount += 1
        setResult2 = set(result2)
        if setResult2:

            #retrieves the permissions that the roles have
            query3 = "select permission, con from policy where"
            count = 0
            for r in setResult2:
                r2 = str(r)
                r2 = re.sub('[(),]', '', r2)
                if count == 0:
                    query3 = query3 + " r_name={}".format(r2)
                else:
                    query3 = query3 + "and r_name={}".format(r2)
                count += 1
            result3 = conn.execute(text(query3))
            queryCount += 1

            #adds the permissions to a dictionary
            for row in result3:
                if row.con:
                    if row.permission in dictResult.keys():
                        con = row.con
                        if "n1" in query and "n_" in con:
                            con = con.replace("n_", "n1.n_")
                        if dictResult[row.permission] != None: 
                            dictResult[row.permission] = dictResult[row.permission] + " and " + con
                        else:
                            dictResult[row.permission] = con
                        appliedCount += 1
                    else:
                        con = row.con
                        if "n1" in query and "n_" in con:
                            con = con.replace("n_", "n1.n_")
                        dictResult[row.permission] = con
                        appliedCount += 1
                else:
                    if row.permission not in dictResult:
                        dictResult[row.permission] = None
                        appliedCount += 1

            #compares the permissions 
            if dictResult:
                for permission in listCol:
                    if permission in dictResult.keys():
                        allowed[permission] = dictResult[permission]

            #returns an empty dictionary if there are no matches
            else:
                f = open("data.csv", "a")
                f.write(str(queryCount) + "," + str(appliedCount) + ",")
                f.close()
                print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
                print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
                return dictResult
        else:
            f = open("data.csv", "a")
            f.write(str(queryCount) + "," + str(appliedCount) + ",")
            f.close()
            print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
            print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
            return dictResult
        
    #prints and saves the number of queries made to the permission database and
    # the number of policies that corresponded to the query;
    # returns the allowed dictionary
    f = open("data.csv", "a")
    f.write(str(queryCount) + "," + str(appliedCount) + ",")
    f.close()
    print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
    print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
    return allowed


""" queries the pbac database to get the permissions that correspond to the 
    purpose. The permissions are compared to the selections of the query and 
    if they match, they are added to the allowed dictionary with their conditions 
    if they have any. The purpose is currently set to default value to align with 
    the policy generator, but it can be changed or set as commented out below. 

    returns a dictionary of selection items that are allowed, along with the
    conditions that correspond to them if there are any
"""
def checkPBAC(listCol, query0):
    allowed = {}
    dictResult = {}
    queryCount = 0
    appliedCount = 0
    #purpose = input("Enter purpose: ")
    purpose = "organize data"
    with engine2.connect() as conn:

        #retrieves the permissions allowed based on the purpose
        query = "select permission, con from policy where purpose='{}'".format(purpose)
        result = conn.execute(text(query))
        queryCount += 1

        #adds the permissions to a dictionary along with their conditions
        for row in result:
            if row.con:
                if row.permission in dictResult.keys():
                    con = row.con
                    if "n1" in query0 and "n_" in con:
                        con = con.replace("n_", "n1.n_")
                    if dictResult[row.permission] != None: 
                        dictResult[row.permission] = dictResult[row.permission] + " and " + con
                    else:
                        dictResult[row.permission] = con
                    appliedCount += 1
                else:
                    con = row.con
                    if "n1" in query0 and "n_" in con:
                        con = con.replace("n_", "n1.n_")
                    dictResult[row.permission] = con
                    appliedCount += 1
            else:
                if row.permission not in dictResult:
                    dictResult[row.permission] = None
                    appliedCount += 1

        #compares the permissions to the selection items in the query
        if dictResult:
            for permission in listCol:
                if permission in dictResult.keys():
                    allowed[permission] = dictResult[permission]

        #returns an empty dictionary if there are no matches
        else:
            f = open("data.csv", "a")
            f.write(str(queryCount) + "," + str(appliedCount) + ",")
            f.close()
            print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
            print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
            return dictResult
        
    #prints and saves the number of queries made to the permission database and
    # the number of policies that corresponded to the query;
    # returns the allowed dictionary
    f = open("data.csv", "a")
    f.write(str(queryCount) + "," + str(appliedCount) + ",")
    f.close()
    print("\nNUMBER OF QUERIES TO PERMISSION TABLES: " + str(queryCount) + "\n")
    print("\nNUMBER OF POLICIES APPLIED TO QUERY: " + str(appliedCount) + "\n")
    return allowed

#returns the table name given a column
def getTableName(column):
    splitPer = column.split("_")
    if splitPer[0] == 's':
        return "supplier"
    elif splitPer[0] == 'p':
        return "part"
    elif splitPer[0] == 'ps':
        return "partsupp"
    elif splitPer[0] == 'c':
        return "customer"
    elif splitPer[0] == 'o':
        return "orders"
    elif splitPer[0] == 'l':
        return "lineitem"
    elif splitPer[0] == 'n':
        return "nation"
    elif splitPer[0] == 'r':
        return "region"
    return ""

#a list of conditions is separated by the table it is associated with;
#returns a 2D list where each list corresponds to a table 
def separateConditions(conditions):
    separatedCondition = []
    s = []
    p = []
    ps = []
    c = []
    o = []
    l = []
    n = []
    r = []
    for column in conditions:
        splitPer = column.split("_")
        if splitPer[0] == 's':
            s.append(column)
        elif splitPer[0] == 'p':
            p.append(column)
        elif splitPer[0] == 'ps':
            ps.append(column)
        elif splitPer[0] == 'c':
            c.append(column)
        elif splitPer[0] == 'o':
            o.append(column)
        elif splitPer[0] == 'l':
            l.append(column)
        elif splitPer[0] == 'n':
            n.append(column)
        elif splitPer[0] == 'r':
            r.append(column)
    if s:
        separatedCondition.append(s)
    if p:
        separatedCondition.append(p)
    if ps:
        separatedCondition.append(ps)
    if c:
        separatedCondition.append(c)
    if o:
        separatedCondition.append(o)
    if l:
        separatedCondition.append(l)
    if n:
        separatedCondition.append(n)
    if r:
        separatedCondition.append(r)
    return separatedCondition

#creates a view for rbac and pbac models that takes into account conditions in the policies
#returns a modified query that replaces what is after the "from" clause to the new view
def createView(conditions, query, num, i):

    #handles special cases where the "from" clause is followed by a nested query
    if i == 6 or i == 7 or i == 8 or i == 12 or i == 21:
        queryList = query.split(" ")
        count = 0
        count1 = 0
        index1 = 0
        index2 = 0
        insertCount = 0
        insertCount1 = 0
        viewName = ""
        view = ""

        #finds the location of the nested query
        for word in range(len(queryList)):
            if queryList[word] == ")" and queryList[word+1] == "as":
                index2 = word
            if queryList[word] == "from" and count == 0:
                index1 = word+1
                count += 1
        #inserts the conditions after "where"
        for w in range(index1, index2):
            viewName = "temp0"
            if queryList[w] == "where" and count1 == 0:
                for condition in conditions:
                    insertion = w + 1
                    queryList.insert(insertion+insertCount, condition + " and")
                    insertCount += 1
                    index2 += 1 
                count1 += 1
        #handles the case where there is no "where" in the nested query
        if count1 == 0:
            viewName = "temp0 " + queryList[index2 + 3] + " " + queryList[index2 + 4]
            queryList.insert(index2 - 3, "where")
            insertion = index2 - 2
            for condition in range(len(conditions)):
                if condition == len(conditions)-1:
                    queryList.insert(insertion+insertCount1, conditions[condition])
                else:
                    queryList.insert(insertion+insertCount1, conditions[condition] + " and")
                insertCount1 += 1
                index2 += 1
            index2 += 1

        #creates the view statement
        for x in range(index1+1, index2):
            if x == index2:
                view = view + queryList[x]
            else:
                view = view + queryList[x] + " "

        #deletes the nested query
        del1 = index1
        if count1 == 0:
            for x in range(index1, index2+5):
                queryList.pop(del1)
        if count1 != 0:
            for x in range(index1, index2+3):
                queryList.pop(del1)
        
        #creates the view
        with engine1.connect() as conn:
            query1 = "create view {} as {}".format(viewName, view)
            conn.execute(text(query1))
            conn.commit()
        temp = viewName.split(" ")
        viewName = temp[0]
        queryList.insert(index1, viewName)
        query = " ".join(queryList)
    else:
        sep = separateConditions(conditions)
        count = num

        #for each table that has conditions
        for i in sep:
            table = getTableName(i[0])
            condition = ""

            #for each condition corresponding to the current table, merge the conditions into one string
            for c in range(len(i)):
                if c == len(i) - 1:
                    condition = condition + str(i[c])
                else:
                    condition = condition + str(i[c]) + " and "

            #creates a view for the table with conditions
            with engine1.connect() as conn:
                query1 = "create view temp{} as select * from {} where {}".format(str(count), table, condition)
                conn.execute(text(query1))
                conn.commit()

            #replaces the tables after the "from" clause with the views
            table = " " + table + ","
            if table in query:
                query = query.replace(table, " temp{},".format(str(count)), 1)
            else:
                table = table.replace(",", " ", 1)
                query = query.replace(table, " temp{} ".format(str(count)), 1) 

            #removes unnecessary commas 
            list1 = list(query)
            if list1[query.index("where")-2] == ",":
                list1[query.index("where")-2] = ""
            query = "".join(list1)
            count += 1
    return query

#modifies query 21 if any of the permissions are not allowed and returns the modified query;
#this function is required due to formatting needs
def fixQuery21(allowed, query):
    replaced0 = 0
    replaced1 = 0
    replaced2 = 0
    replaced3 = 0

    #if there are no matches for the current permission, certain parts of the query will removed
    for i in permissionList[20]:
        if i not in allowed.keys():
            if i == "count(supplier)" and "s_name" not in allowed.keys():
                query = query.replace("supplier, ", "")
                query = query.replace(" and s_nationkey = n_nationkey", "")
                query = query.replace("s_suppkey = l1.l_suppkey and ", "")
                query = query.replace("group by s_name ", "")
                query = query.replace(", s_name limit", " limit")
                replaced0 = 1
            elif i == "count(lineitem)":
                query = query.replace("lineitem l1, ", "")
                query = query.replace("s_suppkey = l1.l_suppkey and ", "")
                query = query.replace(" o_orderkey = l1.l_orderkey", "")
                query = query.replace(" and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate )", "")
                replaced1 = 1
            elif i == "count(orders)":
                query = query.replace("orders, ", "")
                query = query.replace(" and o_orderkey = l1.l_orderkey", "")
                query = query.replace(" and o_orderstatus = 'F'", "")
                replaced2 = 1
            elif i == "count(nation)":
                query = query.replace("nation ", "")
                query = query.replace(" and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'", "")
                replaced3 = 1
    if replaced0 == 1 and replaced1 == 1 and replaced2 == 1 and replaced3 == 1:
        query = query.replace("s_name, count(*) as numwait ", "")
    elif replaced1 == 1 and replaced2 == 1 and replaced3 == 1:
        query = query.replace("numwait desc, ", "")
    elif replaced0 == 1:
        query = query.replace("t s_name,", "t")

    #removes unnecessary "and"s and commas
    list1 = list(query)
    str1 = list1[query.index("where")+6] + list1[query.index("where")+7] + list1[query.index("where")+8]
    if str1 == "and":
        list1[query.index("where")+6] = ""
        list1[query.index("where")+7] = ""
        list1[query.index("where")+8] = ""
        list1[query.index("where")+9] = ""
    if list1[query.index("where")-2] == ",":
        list1[query.index("where")-2] = ""
    
    #returns the modified query
    query = "".join(list1)    
    return query

#returns the allowed dictionary given the model, selection items, and query
def getAllowed(model, listCol, query):
    if model == '1':
        return checkABAC(listCol, query)
    elif model == '2':
        return checkRBAC(listCol, query)
    elif model == '3':
        return checkPBAC(listCol, query)
    else:
        return {}

#modifies the current query based on the conditions, for the abac model
def abacFix(conditions, query, num):
    queryList = query.split(" ")

    #adds the conditions to the query after the word "where"
    if num == 6 or num == 7 or num == 8 or num == 21:
        for word in range(len(queryList)):
            if queryList[word] == "where":
                for condition in conditions:
                    if condition:
                        queryList.insert(word+1, condition + " and")
                break
    #adds the conditions to the query before the word "group"
    elif num == 12:
        count = 0
        for word in range(len(queryList)):
            if "%" in queryList[word]:
                for condition in conditions:
                    if condition:
                        if count == 0:
                            queryList.insert(word+1, "where " + condition)
                        else:
                            queryList.insert(word+1, "and " + condition)
                        count += 1
                break
    #adds the conditions to the query after the word "where" for both parts of the query
    elif num == 14:
        count = 0
        for word in range(len(queryList)):
            if queryList[word] == "where" and count == 0:
                for condition in conditions:
                    if condition and condition[0] == 'l':
                        queryList.insert(word + 1, condition + " and")
                count += 1
            elif queryList[word] == "where" and count == 1:
                for condition in conditions:
                    if condition and condition[0] == 's':
                        queryList.insert(word + 1, condition + " and")
                count += 1
    #adds the conditions to the query after the word "where"
    else: 
        for word in range(len(queryList)):
            if queryList[word] == "where":
                for condition in conditions:
                    if condition:
                        queryList.insert(word + 1, condition + " and")
                break
    
    #returns the modified query
    query = " ".join(queryList)
    return query

#retrieves and prints the size of each table in the access control database
def getMemory():
    with engine3.connect() as conn:
        result = None
        if model == '1':
            result = conn.execute(text("SELECT TABLE_NAME AS 'Table', (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 AS 'mem' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'abac' ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;"))
        elif model == '2':
            result = conn.execute(text("SELECT TABLE_NAME AS 'Table', (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 AS 'mem' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'rbac' ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;"))
        elif model == '3':
            result = conn.execute(text("SELECT TABLE_NAME AS 'Table', (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 AS 'mem' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'pbac' ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;"))
        for row in result:
            print(row)
            f = open("data.csv", "a")
            f.write(row.Table + "," + str(row.mem) + ",\n")
            f.close()


#executes all the queries without access control and displays the results and runtime
def run():
    with engine1.connect() as conn:

        #traverses through all the queries
        for i in range(0, 22):

            #handles query 15, where there are multiple statements to execute
            if i == 14:
                splitList = queryList[i].split("; ")
                start = time.time()
                result = conn.execute(text(splitList[0]))
                result = conn.execute(text(splitList[1]))
                end = time.time()
                print("\nTIME TO EXECUTE QUERY: " + str(end - start) + "\n")
                f = open("data1.csv", "a")
                f.write(str(end - start) + "\n")
                f.close()
                print(f"\nQUERY NUMBER: {i + 1}")
                count = 0
                for row in result:
                    print(row)
                    count += 1
                print(f"\nNUMBER OF ROWS RETURNED: {count}\n")
                f = open("data2.csv", "a")
                f.write(str(count) + "\n")
                f.close()
                result = conn.execute(text(splitList[2]))
                conn.commit()
            
            else:
                start = time.time()
                result = conn.execute(text(queryList[i]))
                end = time.time()
                print("\nTIME TO EXECUTE QUERY: " + str(end - start) + "\n")
                f = open("data1.csv", "a")
                f.write(str(end - start) + "\n")
                f.close()
                print(f"\nQUERY NUMBER: {i + 1}")
                count = 0
                for row in result:
                    print(row)
                    count += 1
                print(f"\nNUMBER OF ROWS RETURNED: {count}\n")
                f = open("data2.csv", "a")
                f.write(str(count) + "\n")
                f.close()

#executes the queries with access control implemented
def runAC():
    with engine1.connect() as conn:

        #traverses through all the queries
        for i in range(0, 22):

            #handles query 15, where there are multiple statements to execute
            if i == 14:
                start = time.time()
                allowed = getAllowed(model, permissionList[i], queryList[i])
                end = time.time()
                print("\nTIME TO CHECK PERMISSION TABLES: " + str(end - start) + "\n")
                f = open("data.csv", "a")
                f.write(str(end - start) + ",")
                f.close()

                start1 = time.time()

                #removes selection items that are not allowed
                if len(allowed) != len(permissionList[i]) or allowed.values():
                    newQuery = queryList[i]
                    for x in range(len(permissionList[i])):
                        if permissionList[i][x] not in allowed.keys() and selectList[i][x] in newQuery:
                            newQuery = newQuery.replace(selectList[i][x], "", 1)
                    if "sum(l_extendedprice * (1 - l_discount)) " not in newQuery:
                        newQuery = newQuery.replace(selectList[i][7], "", 1)
                        newQuery = newQuery.replace(", total_revenue", "", 1)
                        newQuery = newQuery.replace("and total_revenue = ( select max(total_revenue) from revenue0 )", "", 1)
                    if "l_suppkey, " not in newQuery:
                        newQuery = newQuery.replace("supplier_no, ", "", 1)
                        newQuery = newQuery.replace(" group by l_suppkey;", ";", 1)
                        newQuery = newQuery.replace("s_suppkey = supplier_no and ", "", 1)
                    newQueryList = list(newQuery)

                    #removes unnecessary commas
                    if newQuery[newQuery.index("from")-2] == ",":
                        newQueryList[newQuery.index("from")-2] = ""
                    if newQuery[newQuery.find("from", newQuery.find("from")+1)-2] == ",":
                        newQueryList[newQuery.find("from", newQuery.find("from")+1)-2] = ""
                    newQuery = "".join(newQueryList)
                    splitQ = newQuery.split("; ")

                    #if no selection items remain, then the query is not executed
                    if splitQ[1].index("from") - splitQ[1].index("select") == 7:
                        end1 = time.time()
                        print("\nTIME TO FIX QUERY: " + str(end1 - start1) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end1 - start1) + ",,\n")
                        f.close()
                        print("You do not have permission to make that query")
                    
                    #otherwise, more changes will be made to the query to make it executable
                    else:
                        splitQuery = newQuery.split("; ")

                        #modifies the query by either creating views (rbac, pbac) or reformatting the query (abac)
                        if model == '2' or model == '3':
                            conditions0 = []
                            conditions1 = []
                            for v in allowed.values():
                                if v and v[0] == 'l':
                                    conditions0.append(v)
                                elif v and v[0] == 's':
                                    conditions1.append(v)
                            if conditions0:
                                splitQuery[0] = createView(conditions0, splitQuery[0], 0, i)
                            if conditions1:
                                splitQuery[1] = createView(conditions1, splitQuery[1], 100, i)
                        else:
                            newQuery = abacFix(allowed.values(), newQuery, i)
                            splitQuery = newQuery.split("; ")

                        end1 = time.time()
                        print("\nTIME TO FIX QUERY: " + str(end1 - start1) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end1 - start1) + ",")
                        f.close()

                        start2 = time.time()
                        result = conn.execute(text(splitQuery[0]))
                        result2 = conn.execute(text(splitQuery[1]))
                        end2 = time.time()
                        print("\nTIME TO EXECUTE QUERY: " + str(end2 - start2) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end2 - start2) + ",")
                        f.close()

                        print(f"\nQUERY NUMBER: {i + 1}")
                        count1 = 0
                        for row in result2:
                            print(row)
                            count1 += 1
                        print(f"\nNUMBER OF ROWS RETURNED: {count1}\n")
                        f = open("data.csv", "a")
                        f.write(str(count1) + ",\n")
                        f.close()
                        conn.execute(text(splitQuery[2]))
                        conn.commit()

                        #drops the views that were created
                        if model == '2' or model == '3':
                            for c in range(len(separateConditions(conditions0))):
                                conn.execute(text("drop view temp{};".format(c)))
                                conn.commit()
                            if conditions1:
                                conn.execute(text("drop view temp100;"))
                                conn.commit()

                #executes the query as normal if all selection items were allowed
                else:
                    start2 = time.time()
                    result = conn.execute(text("create view revenue0 (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) from lineitem where l_shipdate >= '1996-01-01' and l_shipdate < '1996-01-01' + interval 3 month group by l_suppkey;"))
                    result2 = conn.execute(text("select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, revenue0 where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue0 ) order by s_suppkey;"))
                    end2 = time.time()
                    print("\nTIME TO EXECUTE QUERY: " + str(end2 - start2) + "\n")
                    f = open("data.csv", "a")
                    f.write(str(end2 - start2) + ",")
                    f.close()
                    print(f"\nQUERY NUMBER: {i + 1}")
                    count = 0
                    for row in result2:
                        print(row)
                        count += 1
                    print(f"\nNUMBER OF ROWS RETURNED: {count}\n")
                    f = open("data.csv", "a")
                    f.write(str(count) + ",\n")
                    f.close()
                    conn.execute(text("drop view revenue0;"))
                    conn.commit()
            else:
                start = time.time()
                allowed = getAllowed(model, permissionList[i], queryList[i])
                end = time.time()
                print("\nTIME TO CHECK PERMISSION TABLES: " + str(end - start) + "\n")
                f = open("data.csv", "a")
                f.write(str(end - start) + ",")
                f.close()

                #removes selection items that are not allowed
                start1 = time.time()
                if len(allowed) != len(permissionList[i]) or allowed.values():
                    newQuery = queryList[i]
                    for x in range(len(permissionList[i])):
                        if permissionList[i][x] not in allowed.keys() and selectList[i][x] in newQuery and i != 20:
                            newQuery = newQuery.replace(selectList[i][x], "", 1)
                        elif i == 20:
                            newQuery = fixQuery21(allowed, newQuery)

                    #removes unnecessary commas
                    newQueryList = list(newQuery)
                    if newQuery[newQuery.index("from")-2] == ",":
                        newQueryList[newQuery.index("from")-2] = ""
                    newQuery = "".join(newQueryList)

                    #if no selection items remain, then the query is not executed
                    if newQuery.index("from") - newQuery.index("select") == 7:
                        end1 = time.time()
                        print("\nTIME TO FIX QUERY: " + str(end1 - start1) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end1 - start1) + ",,\n")
                        f.close()
                        print("You do not have permission to make that query")

                    #otherwise, more changes will be made to the query to make it executable
                    else:
                        if (i == 4 or i == 9) and "sum(l_extendedprice * (1 - l_discount)) as revenue" not in newQuery:
                            newQuery = newQuery.replace(" order by revenue desc", "", 1)
                        elif (i == 2) and "sum(l_extendedprice * (1 - l_discount)) as revenue" not in newQuery:
                             newQuery = newQuery.replace(" revenue desc,", "", 1)
                        elif (i == 10) and "sum(ps_supplycost * ps_availqty) as value" not in newQuery:
                            newQuery = newQuery.replace(" order by value desc", "", 1)
                        elif (i == 12) and "count(*) as custdist" not in newQuery:
                            newQuery = newQuery.replace(" custdist desc,", "", 1)
                        elif (i == 15) and "count(distinct ps_suppkey) as supplier_cnt" not in newQuery:
                            newQuery = newQuery.replace(" supplier_cnt desc,", "", 1)

                        #modifies the query by either creating views (rbac, pbac) or reformatting the query (abac)
                        if model == '2' or model == '3':
                            conditions = []
                            for v in allowed.values():
                                if v:
                                    conditions.append(v)
                            if conditions:
                                newQuery = createView(conditions, newQuery, 0, i)
                        else:
                            newQuery = abacFix(allowed.values(), newQuery, i)

                        end1 = time.time()
                        print("\nTIME TO FIX QUERY: " + str(end1 - start1) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end1 - start1) + ",")
                        f.close()
                        
                        start2 = time.time()
                        result = conn.execute(text(newQuery))
                        end2 = time.time()
                        print("\nTIME TO EXECUTE QUERY: " + str(end2 - start2) + "\n")
                        f = open("data.csv", "a")
                        f.write(str(end2 - start2) + ",")
                        f.close()

                        print(f"\nQUERY NUMBER: {i + 1}")
                        count1 = 0
                        for row in result:
                            print(row)
                            count1 += 1
                        print(f"\nNUMBER OF ROWS RETURNED: {count1}\n")
                        f = open("data.csv", "a")
                        f.write(str(count1) + ",\n")
                        f.close()

                        #drops all the views that were created
                        if model == '2' or model == '3':
                            if (i == 6 or i == 7 or i == 8 or i == 12 or i == 21) and (len(separateConditions(conditions)) != 0):
                                conn.execute(text("drop view temp0;"))
                                conn.commit()
                            else:
                                for c in range(len(separateConditions(conditions))):
                                    conn.execute(text("drop view temp{};".format(c)))
                                    conn.commit()

                #executes the query if there all selection items are allowed
                else:
                    start2 = time.time()
                    result = conn.execute(text(queryList[i]))
                    end2 = time.time()
                    print("\nTIME TO EXECUTE QUERY: " + str(end2 - start2) + "\n")
                    f = open("data.csv", "a")
                    f.write(str(end2 - start2) + ",")
                    f.close()
                    print(f"\nQUERY NUMBER: {i + 1}")
                    count = 0
                    for row in result:
                        print(row)
                        count += 1
                    print(f"\nNUMBER OF ROWS RETURNED: {count}\n")
                    f = open("data.csv", "a")
                    f.write(str(count) + ",\n")
                    f.close()

#determines what to execute
answer = input("1. Get memory, 2. Run with access control, 3. Run without access control\n")
if answer == '1':
    getMemory()
elif answer == '2':
    runAC()
elif answer == '3':
    run()