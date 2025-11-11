import socket
import sys


HOST = '0.0.0.0' # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port

RegisteredUsers = {}#Registered Users dictionary

#REGISTER|RQ#|Name|Role|IP_Address|UDP_Port#|TCP_Port#|Storage_Capacity|
#REGISTER 01 Alice BOTH 192.168.1.10 5001 6001 1024MB



#ApplicationMessage = f"REGISTER|{payload_length:02d}|{UserMessage}".encode("utf-8")



def RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port):
    print(data)
    Request = data.decode("utf-8")
    if(data.decode("utf-8").startswith("REGISTER")):
        #Registration Handling
        if(Name not in RegisteredUsers):
            RegisteredUsers[Name] = {
                "Role": Role,
                "IP_Address": IP_Address,
                "UDP_Port": UDP_Port
            }
            print(f"User {Name} registered successfully.")
            reply = "REGISTERED|RQ1|SUCCESS|".encode("utf-8")
            s.sendto(reply, addr)
            print("Registration request received")
        else:
            print("User already registered")
            reply = "REGISTER-DENIED|RQ#|Reason".encode("utf-8")
            s.sendto(reply, addr)
       
    else:
        reply = "REGISTER-DENIED|RQ#|Reason".encode("utf-8")
        s.sendto(reply, addr)
        print("Unknown request received")
    


    
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
        data_string = "REGISTER|RQ1|asdafa|asdasf|0.0.0.0|8889|"
        Request = data_string.split('|')

        command = Request[0]
        RequestID = Request[1]
        Name = Request[2]
        Role = Request[3]
        IP_Address = Request[4]
        UDP_Port = Request[5]

    if( command == "REGISTER"):
        RegistrationCheck(d[0],d[1], Name, Role, IP_Address, UDP_Port)
    
#    reply = data
#    s.sendto(reply, addr)
    print ('Message[' + addr[0] + ':' + str(addr[1]) + '] - ' + data.decode("utf-8"))

s.close()