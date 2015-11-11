import csv
import numpy as np
import pandas as pd
import MySQLdb as mdb

def Nullify(line):
    def f(word):
        if(word == ""):
            return None
        else:
            return word
    return [f(word) for word in line]
    
def BuildInsert(table, numfields):
    datholder = (numfields-1) * "%s, " + "%s"
    query = ("insert into %s" % table) + (" values (%s)" % datholder)
    return query

def Csv2sql(csvfile, db_name, table_name, sqlfields):
    nfields = len(sqlfields)
    sqlfields = ',\n'.join(sqlfields)
    create_cmd = "CREATE TABLE %s \n(%s)" % (table_name, sqlfields)
    mydb = mdb.connect(host='localhost', user='root', passwd='cheng')
    cursor = mydb.cursor()
    try:
        cursor.execute("USE %s" % db_name)
    except mdb.Error:
        cursor.execute('CREATE DATABASE %s' % db_name)
        cursor.execute("USE %s" % db_name)

    try:
        cursor.execute(create_cmd)
    except mdb.Error:
        drop_cmd = "DROP TABLE %s" % table_name
        cursor.execute(drop_cmd)
        cursor.execute(create_cmd)

    mydb.commit()

    csv_data = csv.reader(file(csvfile))
    csv_data.next() #skip the first header row
    query = BuildInsert(table_name, nfields)
    count = 0
    for line in csv_data:
        vals = Nullify(line)
        cursor.execute(query, vals)
        count += 1
        if count > 100000:
            mydb.commit() # commit every 100000 rows
            count = 0
        #cursor.execute(query, vals)
    mydb.commit()
    mydb.close()
    return(1)

def Data_Norm(data):
  price_data = np.zeros((data.shape[0]*4))
  N = data.shape[0]
  for i in range(4):
    price_data[i*N:(i+1)*N] = data[:,i]
  price_data[N:2*N] = data[:,4]
  means = np.mean(data, axis = 0)
  means[0] = np.mean(price_data)
  means[2] = means[0]
  means[3] = means[0]
  means[4] = means[0]
  stds = np.std(data, axis = 0)
  stds[0] = np.std(price_data)
  stds[2] = stds[0]  
  stds[3] = stds[0]
  stds[4] = stds[0]

  std_data = np.zeros((data.shape[0], data.shape[1]))
  for i in range(data.shape[0]):
    for j in range(data.shape[1]):
      std_data[i,j] = (data[i,j] - means[j])/stds[j]
  return std_data

def Autocor(x, lag):
    n = len(x)
    if(lag>n-1):
        lag=n-1
    acf = np.zeros((lag,))
    m1 = np.mean(x)
    m2 = np.mean(x)
    for i in range(lag):
        acf[i] = 1.0/(n)*np.sum((x[:n-i]-m1)*(x[i:]-m2))/np.std(x)**2
    return acf

#print correlation matrix to screen 
def Print_Cor_Mat(cm, items):
    n = cm.shape[0]
    print "        ",
    for i in range(n):
        print "%-8s" % (items[i]),
    print "\n",
    for i in range(n):
        print "%-8s" % (items[i]),
        for j in range(n):
            if i!=j:
                print "%-8.4f" % (cm[i][j]),
            else:
                print "%-8s" % "  1   ",
        print "\n",
    print "\n"

def Data_Rescale(X):
    mean = np.mean(X, 0)
    std = np.std(X, 0)
    for i in range(X.shape[1]):
        X[:,i] = (X[:,i]-mean[i])/std[i]
    return X
    
def TargetY(data):
    return np.transpose(np.array(data[:-1,1]>data[1:,1], dtype=int))

def GetX(data, lag=3):
    newdata = np.zeros((data.shape[0]-lag+1, (lag-1)*data.shape[1]))
    [l,w] = data.shape
    for i in range(lag-1):
        newdata[:,(i*w):((i+1)*w)] = data[i+1:(l-lag+i+2),:]
    return newdata
