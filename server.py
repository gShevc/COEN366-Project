import socket
import sys
import threading

HOST = '0.0.0.0' # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port

RegisteredUsers = {}#Registered Users dictionary

#REGISTER|RQ#|Name|Role|IP_Address|UDP_Port#|TCP_Port#|Storage_Capacity|
#REGISTER 01 Alice BOTH 192.168.1.10 5001 6001 1024MB



#ApplicationMessage = f"REGISTER|{payload_length:02d}|{UserMessage}".encode("utf-8")


def RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port):
    print(data)
    Request = data.decode("utf-8")

    if(Name not in RegisteredUsers):
        RegisteredUsers[Name] = {
            "Role": Role,
            "IP_Address": IP_Address,
            "UDP_Port": UDP_Port
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


    
try :
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #Create a UDP socket 
    print ('Socket created')
except (socket.error , msg) :
    int ('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()

try:#Bind the socket to the port and an ip address
    s.bind((HOST, PORT))
    print ('Socket bind complete')
except (socket.error , msg):
    print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()


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

    elif(command == "BACKUP_REQ"):
        print("Backup request")

    else:
        reply = ("REGISTER-DENIED|RQ#|Unkown Command").encode("utf-8")
        s.sendto(reply, addr)
        print("Unknown request received")


 
s.close()