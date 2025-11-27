import socket
import sys
import threading



RegisteredUsers = {}#Registered Users dictionary
ServerRQ = 1

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
        reply = "REGISTERED|RQ{ServerRQ}|SUCCESS|".encode("utf-8")
        ServerRQ += 1
        s.sendto(reply, addr)
    else:
        print("User already registered")
        reply = "REGISTER-DENIED|RQ{ServerRQ}|User already Exists".encode("utf-8")
        ServerRQ += 1
        s.sendto(reply, addr)



def Deregistration(data, addr, Name, Role, IP_Address, UDP_Port):
    print(data)
    Request = data.decode("utf-8")
    
    if(Name in RegisteredUsers):
        del RegisteredUsers[Name] 

        print(f"User {Name} Has been deregistered successfully.")
        reply = "DE-REGISTER |RQ{ServerRQ}|SUCCESS|".encode("utf-8")
        ServerRQ += 1
        s.sendto(reply, addr)
    else:
####HOW TO HANDLE IF USER NOT REGISTERED? WHAT DOES IGNORE MEAN
        print("User is not regitered yet")
        reply = "REGISTER-DENIED|RQ{ServerRQ}|User already Exists".encode("utf-8")
        ServerRQ += 1
        s.sendto(reply, addr)



def differentChunkSizes(self, requestName, fileSize, minimumChunks):
    newChunkSize=fileSize
    storagePeers = []
    chunksNumber = minimumChunks # initially, we want to find files with enough space to store whole file
    potentialPeers = {
    "size": newChunkSize,
    "availablePeers": storagePeers,
    "requiredChunks": chunksNumber,
    "usedPeers": [],
    "acceptedPeers": [],
    "requesterAddr": None  # will be used to send the BACKUP_PLAN or BACKUP_DENIED
    }
    if(requestName == "BACKUP_REQ"):
        while(True):
            for Name in RegisteredUsers:
                storageCapacity = (RegisteredUsers[Name])[3]
                if (storageCapacity >= newChunkSize): # check if there is a peer which can store the whole file
                    storagePeers.append(Name) # this will fill the "Owner_Name"
            
            if (storagePeers.__sizeof__ >= chunksNumber):
                print("We have enough potential peers who can store the file at chunk size:" + newChunkSize)
                return potentialPeers
            else: # not enough peers which can store this chunk Size
                newChunkSize = fileSize/2
                chunksNumber = chunksNumber*2
    else:
        print("Request type error")


""""def sendStorageRequests(self, potentialPeers, requestNumber, fileName, chunkSize, checkSum, HOST, socket):
    size = potentialPeers["size"]
    peersList = potentialPeers["availablePeers"]
    for i in range(size):
        peerRequest = f"STORAGE_TASK|{requestNumber}|{fileName}|{chunkSize}|{peersList[i]}"        
        clientUDPPort = (RegisteredUsers[Name])[2]
        socket.sendto(peerRequest, (HOST, clientUDPPort))"""

def get_next_peer(peersList, usedPeers):

    for peer in peersList:
        if peer not in usedPeers: #usedPeers = peers already tried (ACCEPTED or DENIED)
            return peer #returns the next untested peer from peersList
    return None

def sendStorageRequests(self, potentialPeers, requestNumber, fileName, chunkSize, checksum, HOST, socket):
    peersList = potentialPeers["availablePeers"]
    requiredChunks = potentialPeers["requiredChunks"]   # <-- we add this field
    usedPeers = potentialPeers["usedPeers"]             # <-- peers already contacted

    while len(potentialPeers["acceptedPeers"]) < requiredChunks:  # try until enough peers accept

        nextPeer = get_next_peer(peersList, usedPeers)

        if nextPeer is None:
            print("No more peers to try — sending BACKUP_DENIED")
            denyMsg = f"BACKUP-DENIED|{requestNumber}|NotEnoughStorage".encode("utf-8")
            socket.sendto(denyMsg, potentialPeers["requesterAddr"])
            return

        print(f"Trying next peer: {nextPeer}")
        usedPeers.append(nextPeer) # record that this peer has been attempted
        peerIP = RegisteredUsers[nextPeer]["IP_Address"]
        peerPort = RegisteredUsers[nextPeer]["UDP_Port"]
        msg = f"STORAGE_TASK|{requestNumber}|{fileName}|{chunkSize}|{nextPeer}"
        socket.sendto(msg.encode("utf-8"), (peerIP, int(peerPort)))

        return  # wait for response before sending next one



if __name__ == "__main__":

    HOST = '0.0.0.0' # Symbolic name meaning all available interfaces
    PORT = 8888 # Arbitrary non-privileged port

    s = UDPConnection(HOST,PORT)
    
    isWaiting = False
    peersAccepted =[]
    minimumChunks = 1


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
            potentialPeers=differentChunkSizes(command,fileSize,minimumChunks)
            sendStorageRequests(potentialPeers, requestNumber, fileName, checkSum, HOST, s)

        elif(command == "STORAGE_TASK"):
            answer = Request[5]
            peerName = Request[4]

            if answer == "ACCEPTED":
                potentialPeers["acceptedPeers"].append(peerName)

                if len(potentialPeers["acceptedPeers"]) == potentialPeers["requiredChunks"]:
                    # SEND BACKUP_PLAN
                    peerListStr = ",".join(potentialPeers["acceptedPeers"])
                    msg = f"BACKUP_PLAN|RQ{ServerRQ}|{fileName}|[{peerListStr}]|{potentialPeers['size']}"
                    ServerRQ +=1
                    s.sendto(msg.encode("utf-8"), potentialPeers["requesterAddr"])
                else:
                    # Need more peers
                    sendStorageRequests(potentialPeers, requestNumber, fileName, potentialPeers["size"], HOST, s)
            else:
                minimumChunks = minimumChunks*2
                potentialPeers = differentChunkSizes(command,fileSize,minimumChunks)
                sendStorageRequests(potentialPeers, requestNumber, fileName, checkSum, HOST, s)

        else:
            reply = f"REGISTERED|RQ{ServerRQ}|SUCCESS|".encode("utf-8")
            ServerRQ += 1
            s.sendto(reply, addr)
            print("Unknown request received")
    
    s.close()