import socket
import sys
import threading



RegisteredUsers = {}#Registered Users dictionary

#REGISTER|RQ#|Name|Role|IP_Address|UDP_Port#|TCP_Port#|Storage_Capacity|
#REGISTER 01 Alice BOTH 192.168.1.10 5001 6001 1024MB



#ApplicationMessage = f"REGISTER|{payload_length:02d}|{UserMessage}".encode("utf-8")
def UDPConnection(self,HOST,PORT):
    try :
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #Create a UDP socket 
        print ('Socket created')
    except socket.error as msg :
        int ('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()

    try:#Bind the socket to the port and an ip address
        s.bind((HOST, PORT))
        print ('Socket bind complete')
        return s
    except socket.error as msg:
        print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()

def RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port, storageCapacity):
    print(data)
    Request = data.decode("utf-8")

    if(Name not in RegisteredUsers):
        RegisteredUsers[Name] = {
            "Role": Role,
            "IP_Address": IP_Address,
            "UDP_Port": UDP_Port,
            "Capacity": storageCapacity
        }
        print(f"User {Name} registered successfully.")
        reply = "REGISTERED|RQ1|SUCCESS|".encode("utf-8")
        s.sendto(reply, addr)
    else:
        print("User already registered")
        reply = "REGISTER-DENIED|RQ#|User already Exists".encode("utf-8")
        s.sendto(reply, addr)




def Deregistration(data, addr, Name, Role, IP_Address, UDP_Port):
    print(data)
    Request = data.decode("utf-8")
    
    if(Name in RegisteredUsers):
        del RegisteredUsers[Name] 

        print(f"User {Name} Has been deregistered successfully.")
        reply = "DE-REGISTER |RQ1|SUCCESS|".encode("utf-8")
        s.sendto(reply, addr)
    else:
####HOW TO HANDLE IF USER NOT REGISTERED? WHAT DOES IGNORE MEAN
        print("User is not regitered yet")
        reply = "REGISTER-DENIED|RQ#|User already Exists".encode("utf-8")
        s.sendto(reply, addr)

def backupRequest(self, requestName, requestNumber, fileName, fileSize ,checkSum, HOST, socket):
    storagePeers = []
    numberOfPeers=0
    newFileSize = 0
    if(requestName == "BACKUP_REQ"):
        print("Backup request Received")
        for Name in RegisteredUsers:
            storageCapacity = (RegisteredUsers[Name])["Capacity"]
            if (storageCapacity >= fileSize): # check if there is a peer which can store the whole file
                storagePeers.append(Name) # this will fill the "Owner_Name"
                break
        if (storagePeers.__sizeof__ == 0): # there is no peer which can store the whole file by itself
            newFileSize = fileSize/2 # split the file storing into 2 chunks, since bytes are in powers of 2
            for Name in RegisteredUsers:
                if (storageCapacity >= newFileSize): # check if there is a peer which can store the whole file
                    storagePeers.append(Name) # this will fill the "Owner_Name"
                    if storagePeers.__sizeof__ == 2:
                        break # exit while loop, since you have enough peers to store the file in question
        if (storagePeers.__sizeof__ == 1):
            peerRequest = f"STORAGE_TASK|{requestNumber}|{fileName}|{fileSize}|{storagePeers[0]}"
            clientUDPPort = (RegisteredUsers[Name])["UDP_Port"]
            socket.sendto(peerRequest, (HOST, clientUDPPort))
            #SEND WITH UDP CONNECTION 
        elif (storagePeers.__sizeof__ == 2):
            peerRequest = f"STORAGE_TASK|{requestNumber}|{fileName}|{newFileSize}|{storagePeers[0]}"
            clientUDPPort = (RegisteredUsers[Name])["UDP_Port"]
            socket.sendto(peerRequest, (HOST, clientUDPPort))
            #SEND WITH UDP CONNECTION 
            peerRequest = f"STORAGE_TASK|{requestNumber}|{fileName}|{newFileSize}|{storagePeers[1]}"
            clientUDPPort = (RegisteredUsers[Name])["UDP_Port"]
            socket.sendto(peerRequest, (HOST, clientUDPPort))
            #SEND WITH UDP CONNECTION 
        #should I consider the case when we need more than 2 peers ? 
        # If yes, I need to add another loop in the for loop to increment the size by 2 every iteration until we find enough peers with available space to store the chunks
        return storagePeers
    else:
        print("Request type error")



if __name__ == "__main__":

    HOST = '0.0.0.0' # Symbolic name meaning all available interfaces
    PORT = 8888 # Arbitrary non-privileged port

    s = UDPConnection(HOST,PORT)
    
    isWaiting = False
    peersAccepted =[]


    while 1:
        d = s.recvfrom(1024)
        data = d[0]
        addr = d[1]


        if not data:
            break
        #Split incoming request into fields
        else:
            data_string = data.decode("utf-8")
            Request = data_string.split('|')
            command = Request[0]
            args = Request[1:]


        if(command == "REGISTER"):
                command = Request[0]
                RequestID = Request[1]
                Name = Request[2]
                Role = Request[3]
                IP_Address = Request[4]
                UDP_Port = Request[5]

                RegistrationCheck(d[0],d[1], Name, Role, IP_Address, UDP_Port)

        elif(command == "DE-REGISTER"):
            command = Request[0]
            RequestID = Request[1]
            Name = Request[2]
            Deregistration(d[0],d[1], Name, Role, IP_Address, UDP_Port)

        elif(command == "BACKUP_REQ"): # f"BACKUP_REQ|{rq}|{fileName}|{fileSize}|{checkSum}"
            command = Request[0]
            requestNumber = Request[1]
            fileName = Request[2]
            fileSize = Request[3]
            checkSum = Request[4]
            backupRequest(command, requestNumber, fileName, fileSize, checkSum, HOST, s)

        elif(command == "STORAGE_TASK"):
            #peersAccepted = []
            answer =  Request[5] 
            if (answer == "ACCEPTED"):
                thePeer = Request[4]
                peersAccepted.append(thePeer)

        else:
            reply = ("REGISTER-DENIED|RQ#|Unkown Command").encode("utf-8")
            s.sendto(reply, addr)
            print("Unknown request received")


    
    s.close()