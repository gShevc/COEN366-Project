import socket 
import sys 
import threading
import binascii


client_host = "0.0.0.0"
client_port = 8889


def display_menu():
    print("\n--- Main Menu ---")
    print("1. Register New User")
    print("2. Deregister User")
    print("3. Backup a File")
    print("4. Exit")

def UDPConnection(self, HOST, PORT):
        mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mySocket.bind((HOST,PORT))  # bind to any free port
        print("UDP Connection Achieved")
        return mySocket

def TCPConnection(self, HOST , PORT):
    try :
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #Create a UDP socket
        print ('Socket created')
    except socket.error as msg:
        int ('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
    s.bind((HOST, PORT))
    return s


def RegisterClient(rq, name, role, host, port, storateCapacity,s):
    msg = f"REGISTER|{rq}|{name}|{role}|{host}|{port}|{storateCapacity}|"
     
    s.sendto(msg.encode("utf-8"), ('localhost', 8888))
    d = s.recvfrom(1024)
    data = d[0]
    print ("Server reply : " + str(data.decode('utf-8')))

def DeregisterClient(rq, name, host, port,s):
    msg = f"DE-REGISTER|{rq}|{name}|"
    s.sendto(msg.encode("utf-8"), ('localhost', 8888))
    d = s.recvfrom(1024)

    data = d[0]
    print ("Server reply : " + str(data.decode('utf-8')))

def BackupRequest(rq, fileName, fileSize, checkSum, host, port,s):
    msg = f"BACKUP_REQ|{rq}|{fileName}|{fileSize}|{checkSum}"
    s.sendto(msg.encode("utf-8"), ('localhost', 8888))
    d = s.recvfrom(1024)

    data = d[0]
    print ("Server reply : " + str(data.decode('utf-8')))

def findChecksum(self,data):
    checksum = binascii.crc32(data)
    return checksum


if __name__ == "__main__":
    counter = 1;
    try:
        while True:
            display_menu()
            choice = input("Enter your choice (1-4): ").strip()
            if(choice <'1' | choice >'4'):
                print("Invalid choice. Please try again.")
            else:
                UDPConnection("0.0.0.0", 8889)
                if choice == '1':
                    #UDPConnection("0.0.0.0", 8889)
                    Name, Role = input("Enter name and Role: ").split()
                    RegisterClient("RQ"+counter, Name, Role, "0.0.0.0", 8889, 1024) #added 1024MB for capacity
                    counter +=1
                elif choice == '2':
                    #UDPConnection("0.0.0.0", 8889)
                    Name = input("Please enter user to deregister: ")
                    DeregisterClient("RQ"+counter, Name, "0.0.0.0", 8889)
                    counter +=1
                elif choice == '3':#BACKUP_REQ
                    #UDPConnection("0.0.0.0", 8889)
                    fileName = input("Please enter the name of the file you wish to backup:")
                    fileSize = input("Please enter the size of the file you wish to backup:")
                    data = fileName + fileSize
                    checkSum = findChecksum(data)
                    BackupRequest("RQ"+counter, fileName, fileSize, checkSum, "0.0.0.0", 8889)
                    counter +=1
                elif choice == '4':
                    #UDPConnection("0.0.0.0", 8889)
                    print("You chose to exit the program")
                    sys.exit()
                
    except (socket.error) :
        print ("Error")

