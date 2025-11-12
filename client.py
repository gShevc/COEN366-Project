import socket 
import sys 
import threading


client_host = "0.0.0.0"
client_port = 8889


def display_menu():
    print("\n--- Main Menu ---")
    print("1. Register New User, Enter Name and Role")
    print("2. Deregister User, Enter Name")
    print("3. Exit")



try :
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #Create a UDP socket 
    print ('Socket created')
except (socket.error ) :
    int ('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()

    s.bind((client_host, client_port))


def RegisterClient(rq, name, role, host, port):
    msg = f"REGISTER|{rq}|{name}|{role}|{host}|{port}|"
     
    s.sendto(msg.encode("utf-8"), ('localhost', 8888))
    d = s.recvfrom(1024)
    data = d[0]
    addr = d[1]
    print ("Server reply : " + str(data.decode('utf-8')))

def DeregisterClient(rq, name, role, host, port):
    msg = f"DE-REGISTER|{rq}|{name}|"



if __name__ == "__main__":
    try:
        while True:
            display_menu()
            choice = input("Enter your choice (1-3): ").strip()
            if choice == '1':
                Name, Role = input("Enter name and Role: ").split()
                RegisterClient("RQ1", Name, Role, "0.0.0.0", 8889)
            elif choice == '2':
                DeregisterClient()
            else:
                print("Invalid choice. Please try again.")
    except (socket.error) :
        print ("Error")

