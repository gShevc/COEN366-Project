import socket 
import sys 
import threading
import binascii
import os

client_host = "0.0.0.0"
client_port = 8889

server_addr = ('localhost', 8888)

# each peer will store received chunks in this directory
STORAGE_DIR = "storage_chunks"
os.makedirs(STORAGE_DIR, exist_ok=True)

#########################################################
#                   MENU / CONNECTIONS
#########################################################

def display_menu():
    print("\n--- Main Menu ---")
    print("1. Register New User")
    print("2. Deregister User")
    print("3. Backup a File")
    print("4. Exit")

def UDPConnection(self, HOST, PORT):
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mySocket.bind((HOST,PORT))
    print("UDP Connection Achieved")
    return mySocket

def TCPConnection(self, HOST , PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    print("TCP Connection Achieved")
    return s

#########################################################
#                  SEND REQUESTS TO SERVER
#########################################################

def RegisterClient(rq, name, role, host, port, storageCapacity, s):
    msg = f"REGISTER|{rq}|{name}|{role}|{host}|{port}|{storageCapacity}|"
    s.sendto(msg.encode("utf-8"), server_addr)

def DeregisterClient(rq, name, host, port, s):
    msg = f"DE-REGISTER|{rq}|{name}|"
    s.sendto(msg.encode("utf-8"), server_addr)

def BackupRequest(rq, fileName, fileSize, checkSum, host, port, s):
    msg = f"BACKUP_REQ|{rq}|{fileName}|{fileSize}|{checkSum}"
    s.sendto(msg.encode("utf-8"), server_addr)

def findChecksum(self,data):
    checksum = binascii.crc32(data)
    return checksum

#########################################################
#            RECEIVING MESSAGES FROM SERVER
#########################################################

def listen_for_messages(udp_socket):
    """Background thread to receive any incoming server messages"""
    while True:
        data, addr = udp_socket.recvfrom(2048)
        msg = data.decode("utf-8").strip()
        parts = msg.split('|')

        command = parts[0]

        if command == "STORAGE_TASK":
            handle_STORAGE_TASK(parts, udp_socket)

        elif command == "BACKUP_PLAN":
            handle_BACKUP_PLAN(parts)

        elif command == "SEND_CHUNK":
            handle_SEND_CHUNK(parts)

        else:
            print(f"[INFO] Received server message: {msg}")


#########################################################
#          HANDLING STORAGE_TASK (ACCEPT/DENY)
#########################################################

def handle_STORAGE_TASK(parts, udp_socket):
    """
    Format received:
    STORAGE_TASK|RQx|fileName|chunkSize|peerName
    We must answer:
    STORAGE_TASK|RQx|fileName|chunkSize|peerName|ACCEPTED or |DENIED
    """

    rq = parts[1]
    fileName = parts[2]
    chunkSize = int(parts[3])
    peerName = parts[4]

    # For now ALWAYS ACCEPT
    # (You may later check real disk space)
    answer = "ACCEPTED"

    reply = f"STORAGE_TASK|{rq}|{fileName}|{chunkSize}|{peerName}|{answer}"
    udp_socket.sendto(reply.encode("utf-8"), server_addr)

    print(f"[STORAGE_TASK] Accepted storage for chunk of {fileName}")


#########################################################
#            HANDLE BACKUP_PLAN (TCP transfers)
#########################################################

def handle_BACKUP_PLAN(parts):
    """
    BACKUP_PLAN|RQx|fileName|[peerA,peerB]|chunkSize
    The peer receiving this is the *backup owner*.
    It must open TCP connections and send chunks.
    """

    rq = parts[1]
    fileName = parts[2]
    peerList = parts[3].strip("[]").split(',')
    chunkSize = int(parts[4])

    print(f"[BACKUP_PLAN] Received. Must send chunks to: {peerList}")

    # for now: fake file content for testing
    fileContent = b'X' * (chunkSize * len(peerList))

    chunks = []
    for i in range(len(peerList)):
        start = i * chunkSize
        end = start + chunkSize
        chunks.append(fileContent[start:end])

    # now send each chunk via TCP
    for i, peer in enumerate(peerList):
        # Your assignment uses REGISTER UDP_Port == TCP_Port, but unclear
        # So here we assume TCP port = UDP port + 100 (example)
        # You adjust this as needed
        tcp_port = 8890

        send_chunk_tcp(fileName, chunks[i], peer, tcp_port)


#########################################################
#        TCP CHUNK SENDING FUNCTION (owner → peer)
#########################################################

def send_chunk_tcp(fileName, chunkBytes, peerName, tcp_port):
    """Connect to a peer and send a chunk"""

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", tcp_port))

    msg = f"{fileName}|{len(chunkBytes)}|".encode("utf-8") + chunkBytes
    s.sendall(msg)
    s.close()

    print(f"[TCP] Sent chunk to peer {peerName}")


#########################################################
#      HANDLE SEND_CHUNK (peer receives TCP chunk)
#########################################################

def handle_SEND_CHUNK(parts):
    """
    SEND_CHUNK|RQx|fileName|peerName|tcpPort
    This message tells a peer to prepare to receive a TCP chunk.
    """

    rq = parts[1]
    fileName = parts[2]
    peerName = parts[3]
    tcpPort = int(parts[4])

    print(f"[SEND_CHUNK] Preparing to receive a TCP chunk on port {tcpPort}")

    threading.Thread(
        target=receive_chunk_tcp,
        args=(fileName, tcpPort),
        daemon=True
    ).start()


def receive_chunk_tcp(fileName, tcp_port):
    """Peer receives chunk over TCP and stores it"""

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", tcp_port))
    s.listen(1)

    conn, addr = s.accept()

    header = conn.recv(1024).split(b'|')
    fileName = header[0].decode("utf-8")
    size = int(header[1].decode("utf-8"))

    chunk = conn.recv(size)
    conn.close()

    outFile = os.path.join(STORAGE_DIR, f"{fileName}.chunk")
    with open(outFile, "wb") as f:
        f.write(chunk)

    print(f"[TCP] Stored chunk for {fileName} in {outFile}")


#########################################################
#                    MAIN LOOP
#########################################################

if __name__ == "__main__":
    counter = 1
    udp_socket = UDPConnection("x", client_host, client_port)

    # listen for tasks from server (STORAGE_TASK, SEND_CHUNK, BACKUP_PLAN)
    threading.Thread(target=listen_for_messages, args=(udp_socket,), daemon=True).start()

    try:
        while True:
            display_menu()
            choice = input("Enter your choice (1-4): ").strip()

            if choice == '1':
                Name, Role = input("Enter name and Role: ").split()
                RegisterClient("RQ"+str(counter), Name, Role, client_host, client_port, 1024, udp_socket)
                counter += 1

            elif choice == '2':
                Name = input("Please enter user to deregister: ")
                DeregisterClient("RQ"+str(counter), Name, client_host, client_port, udp_socket)
                counter += 1

            elif choice == '3':
                fileName = input("File name to back up: ")
                fileSize = input("File size in bytes: ")
                data = (fileName + fileSize).encode("utf-8")
                checksum = findChecksum("x", data)
                BackupRequest("RQ"+str(counter), fileName, fileSize, checksum, client_host, client_port, udp_socket)
                counter += 1

            elif choice == '4':
                print("Exiting client.")
                sys.exit()

    except socket.error:
        print("Socket error.")
