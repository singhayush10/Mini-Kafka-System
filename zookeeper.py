def check(arr):
    if (arr[0]==0):
        print("Leader broker has failed")
        if(arr[1]==0):
            print("Broker 3 is leader node")
            return 3
        else:
            print("Broker 2 is leader Node")
            return 2
    else:
        return 1
        
         
