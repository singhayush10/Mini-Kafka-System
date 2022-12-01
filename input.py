import os
from zookeeper import *
import mysql.connector as connector

class DB_helper:
    con = connector.connect(host='localhost' , port='3306',user='root',database='BigDataProject')

    def fun1(self,Topic_NAME,message,partition_number):
        query="INSERT INTO replicas(Topic_Name,`Message`,Partition_no,Broker_No) VALUES('{}','{}',{},1)".format(Topic_NAME,message,partition_number)
        cur=self.con.cursor()
        cur.execute(query)
        self.con.commit()
# n=len(sys.argv)

# topic=sys.argv[1]

# message=""
helper = DB_helper()

# for i in range(2,n-1):
#     message+=sys.argv[i]

# n_partitions=sys.argv[n]

f=open('topics.txt','r')
f1=open('topics.txt','a')
flag=0


topic = input("Enter topic : ")

for each in f:
    each=each.split(",")
    if (each[0]==topic) :
        flag=1
        partitions=each[1]
        partitions=int(partitions)

if(flag==0):
    partitions=int(input("Enter number of partitions : "))
    f1.write(topic+","+str(partitions)+'\n')


message = input("Enter the message : ")
while (message != "$$"):
    arr=[1,1,1]
    
    if(message=="error"):
        arr=[0,1,1]
    if(message=="error1"):
        arr=[0,0,1]
    b_num=check(arr)
    b_num=str(b_num)
    str_len = len(message)
    temp=str_len%partitions
    os.system("node broker"+b_num+".js "+topic+"partition"+str(temp)+" "+ message )

    

    os.system("node broker2.js "+topic+"partition"+str(temp)+"rep "+ message)
    helper.fun1(topic,message,temp)
  
    os.system("node broker3.js "+topic+"partition"+str(temp)+"rep "+ message )
    helper.fun1(topic,message,temp)
    
    message = input("Enter the message : ")



# if(str_len%2==0): 

#     if (flag==1):
#         print("Already their")
#         os.system("node publisher.js "+topic+"partition1 "+ message )


#     else:
#         print("Does not Exist")
#         os.system("node publisher.js "+topic+"partition1 "+ message )
#         f1.write(topic+'\n')
    
#     os.system("node publisher1.js "+topic+"partition1rep "+ message )
#     helper.fun1(topic,message)



# else:
#     if(flag==1):
#         print("Already their")
#         os.system("node publisher.js "+topic+"partition2 "+message )


#     else:
#         print("Does not Exist")
#         os.system("node publisher.js "+topic+"partition2 "+message )
#         f1.write(topic+'\n')

#     os.system("node publisher2.js "+topic+"partition2rep "+ message )
#     helper.fun2(topic,message)








