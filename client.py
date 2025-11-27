import socket
import threading
import os
import binascii
import sys

#########################################################
# AUTO-INCREMENT PORT SYSTEM
#########################################################

PORT_FILE = "ports.dat"

def get_next_ports():
    """
    Automatically assigns a unique UDP and TCP port for each client.
    Uses ports.dat to remember the next free ports.
    """
    base_udp = 9001
    base_tcp = 9101

    if os.path.exists(PORT_FILE):
        with open(PORT_FILE, "r") as f:
            line = f.read().strip()
            if line:
                udp, tcp = map(int, line.split(","))
            else:
                udp, tcp = base_udp, base_tcp
    else:
        udp, tcp = base_udp, base_tcp

    my_udp = udp
    my_tcp = tcp

    # Increment for next run
    with open(PORT_FILE, "w") as f:
        f.write(f"{udp+1},{tcp+1}")

    return my_udp, my_tcp


#########################################################
# CONFIG
#########################################################

CLIENT_UDP_PORT, CLIENT_TCP_PORT = get_next_ports()
CLIENT_HOST = "127.0.0.1"
SERVER_ADDR = ("localhost", 8888)

LOCAL_NAME = None
LOCAL_ROLE = None

STORAGE_DIR = "storage_chunks"
os.makedirs(STORAGE_DIR, exist_ok=True)

udp_socket = None
LocalFiles = {}

clientsInfo = {
    "name": None,
    "role": None,
    "filesOwned": {}
}

pending_backup = {
    "rq": None,
    "fileName": None,
    "chunkSize": None,
    "peers": [],
    "chunks_ok": set(),
}

#########################################################
# UTILS
#########################################################

def udp_send(msg, addr):
    udp_socket.sendto(msg.encode("utf-8"), addr)

def crc32(data):
    return binascii.crc32(data) & 0xffffffff


#########################################################
# CREATE FILE
#########################################################

def create_file(desiredFileName, desiredFileSize, desiredOwner):

    if isinstance(desiredFileSize, str):
        desiredFileSize = int(desiredFileSize)

    if clientsInfo["name"] != desiredOwner:
        print("You cannot create a file for another peer.")
        return

    if desiredFileName in clientsInfo["filesOwned"]:
        print("[ERROR] A file with this name already exists.")
        return

    with open(desiredFileName, "wb") as f:
        f.write(b"\x00" * desiredFileSize)

    clientsInfo["filesOwned"][desiredFileName] = {
        "size": desiredFileSize,
        "owner": desiredOwner,
        "path": desiredFileName,
        "checksum": crc32(open(desiredFileName, "rb").read())
    }

    print(f"[FILE] Created {desiredFileSize}-byte file '{desiredFileName}' for owner {desiredOwner}")


#########################################################
# TCP FUNCTIONS
#########################################################

def TCPConnection(host, port):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind((host, port))
    listener.listen(1)
    print(f"[TCP-LISTEN] Listening for chunk on TCP {port}")
    conn, addr = listener.accept()
    print(f"[TCP-LISTEN] Accepted connection from {addr}")
    return conn, listener


def TCPConnectAndSend(host, port, header_bytes, payload_bytes):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(header_bytes + payload_bytes)
    s.close()


#########################################################
# REGISTER / DEREGISTER / BACKUP
#########################################################

def RegisterClient(rq, name, role, host, udp, tcp, capacity):
    msg = f"REGISTER|{rq}|{name}|{role}|{host}|{udp}|{tcp}|{capacity}|"
    udp_send(msg, SERVER_ADDR)

def DeregisterClient(rq, name):
    msg = f"DE-REGISTER|{rq}|{name}|"
    udp_send(msg, SERVER_ADDR)

def BackupRequest(rq, fileName, fileSize, checksum):
    msg = f"BACKUP_REQ|{rq}|{fileName}|{fileSize}|{checksum}|"
    udp_send(msg, SERVER_ADDR)


#########################################################
# STORAGE_TASK RESPONSE
#########################################################

def handle_STORAGE_TASK(parts):
    rq = parts[1]
    fileName = parts[2]
    chunkSize = parts[3]
    peerName = parts[4]

    reply = f"STORAGE_TASK|{rq}|{fileName}|{chunkSize}|{peerName}|ACCEPTED|"
    udp_send(reply, SERVER_ADDR)
    print(f"[STORAGE_TASK] ACCEPTED storing chunk of {fileName}")


#########################################################
# BACKUP_PLAN (Owner receives plan)
#########################################################

def handle_BACKUP_PLAN(parts):

    rq = parts[1]
    fileName = parts[2]
    peerListStr = parts[3].strip("[]")
    chunkSize = int(parts[4])

    peers = []
    if peerListStr:
        for entry in peerListStr.split(","):
            name, ip, udp, tcp = entry.split(":")
            peers.append((name, ip, int(udp), int(tcp)))

    pending_backup["rq"] = rq
    pending_backup["fileName"] = fileName
    pending_backup["chunkSize"] = chunkSize
    pending_backup["peers"] = peers
    pending_backup["chunks_ok"] = set()

    # Send STORE_REQ to storage peers
    for i, (name, ip, udp, tcp) in enumerate(peers):
        msg = f"STORE_REQ|{rq}|{fileName}|{i}|{clientsInfo['name']}|{CLIENT_UDP_PORT}|{CLIENT_TCP_PORT}|"
        udp_send(msg, (ip, udp))

    print(f"[BACKUP_PLAN] Sent STORE_REQ to all peers for file {fileName}")


#########################################################
# OWNER receives SEND_CHUNK
#########################################################

def handle_SEND_CHUNK(parts, addr):
    rq = parts[1]
    fileName = parts[2]
    peerName = parts[3]
    tcpPort = int(parts[4])
    peer_ip = addr[0]

    # Determine chunk index
    peers = pending_backup["peers"]
    names = [p[0] for p in peers]
    idx = names.index(peerName)

    # Load full file
    with open(fileName, "rb") as f:
        full_data = f.read()

    chunkSize = pending_backup["chunkSize"]
    start = idx * chunkSize
    chunk = full_data[start:start+chunkSize]

    header = f"{fileName}|{len(chunk)}|".encode("utf-8")

    TCPConnectAndSend(peer_ip, tcpPort, header, chunk)
    print(f"[OWNER] Sent chunk to {peerName} ({peer_ip}:{tcpPort})")


#########################################################
# STORAGE PEER: STORE_REQ
#########################################################

def handle_STORE_REQ(parts, addr):
    rq = parts[1]
    fileName = parts[2]
    chunkID = int(parts[3])
    ownerUDP = int(parts[5])

    # Listen for TCP chunk
    conn, listener = TCPConnection("0.0.0.0", CLIENT_TCP_PORT)

    # Notify owner TCP is ready
    msg = f"SEND_CHUNK|{rq}|{fileName}|{clientsInfo['name']}|{CLIENT_TCP_PORT}|"
    udp_send(msg, (addr[0], ownerUDP))

    # Receive header
    header = b""
    while b"|" not in header:
        header += conn.recv(1)

    rest = conn.recv(1024)
    header += rest
    parts2 = header.split(b"|", 2)
    size = int(parts2[1].decode("utf-8"))

    remaining = parts2[2]
    chunk = remaining
    to_read = size - len(chunk)
    while to_read > 0:
        data = conn.recv(min(4096, to_read))
        if not data:
            break
        chunk += data
        to_read -= len(data)

    listener.close()
    conn.close()

    outPath = os.path.join(STORAGE_DIR, f"{fileName}.chunk{chunkID}")
    with open(outPath, "wb") as f:
        f.write(chunk)

    print(f"[STORAGE] Stored chunk {chunkID} of {fileName}")

    # Notify owner
    udp_send(f"CHUNK_OK|{rq}|{fileName}|{chunkID}|", (addr[0], ownerUDP))

    # Notify server
    udp_send(f"STORE_ACK|{rq}|{fileName}|{chunkID}|{clientsInfo['name']}|", SERVER_ADDR)


#########################################################
# OWNER handles CHUNK_OK
#########################################################

def handle_CHUNK_OK(parts):

    rq = parts[1]
    fileName = parts[2]
    chunkID = int(parts[3])

    pending_backup["chunks_ok"].add(chunkID)

    print(f"[OWNER] CHUNK_OK received for chunk {chunkID}")

    if len(pending_backup["chunks_ok"]) == len(pending_backup["peers"]):
        msg = f"BACKUP_DONE|{rq}|{fileName}|"
        udp_send(msg, SERVER_ADDR)
        print("[OWNER] All chunks OK. Sent BACKUP_DONE to server.")


#########################################################
# UDP LISTENER THREAD
#########################################################

def listen_udp():
    while True:
        data, addr = udp_socket.recvfrom(4096)
        msg = data.decode("utf-8").strip()

        parts = msg.split("|")
        cmd = parts[0]

        if cmd == "STORAGE_TASK":
            handle_STORAGE_TASK(parts)

        elif cmd == "BACKUP_PLAN":
            handle_BACKUP_PLAN(parts)

        elif cmd == "STORE_REQ":
            handle_STORE_REQ(parts, addr)

        elif cmd == "SEND_CHUNK":
            handle_SEND_CHUNK(parts, addr)

        elif cmd == "CHUNK_OK":
            handle_CHUNK_OK(parts)

        else:
            print(f"[RECV] {msg}")


#########################################################
# MAIN MENU
#########################################################

def display_menu():
    print("\n--- MENU ---")
    print(f"(Running on UDP:{CLIENT_UDP_PORT} TCP:{CLIENT_TCP_PORT})")
    print("1. Register")
    print("2. Deregister")
    print("3. Backup File")
    print("4. Create File")
    print("5. Exit")


if __name__ == "__main__":

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind((CLIENT_HOST, CLIENT_UDP_PORT))

    print(f"[UDP] Client running on {CLIENT_HOST}:{CLIENT_UDP_PORT}")
    print(f"[INFO] Auto-assigned TCP port: {CLIENT_TCP_PORT}")

    threading.Thread(target=listen_udp, daemon=True).start()

    counter = 1

    while True:
        display_menu()
        c = input("Choice: ")

        if c == "1":
            clientsInfo["name"] = input("Name: ")
            clientsInfo["role"] = input("Role (BOTH/STORAGE/OWNER): ")
            storageCapacity = input("Storage Capacity (bytes): ")

            RegisterClient(
                f"RQ{counter}",
                clientsInfo["name"],
                clientsInfo["role"],
                CLIENT_HOST,
                CLIENT_UDP_PORT,
                CLIENT_TCP_PORT,
                int(storageCapacity)
            )
            counter += 1

        elif c == "2":
            name = input("Name to deregister: ")
            DeregisterClient(f"RQ{counter}", name)
            counter += 1

        elif c == "3":
            fileName = input("File name: ")
            if fileName not in clientsInfo["filesOwned"]:
                print("[ERROR] You do not own this file.")
                continue

            metadata = clientsInfo["filesOwned"][fileName]
            BackupRequest(f"RQ{counter}", fileName, metadata["size"], metadata["checksum"])
            counter += 1

        elif c == "4":
            desiredFileName = input("Enter the name of the file to be created: ").strip()
            desiredFileSize = input("Enter the size of the file to be created: ").strip()
            desiredOwner = input("Enter the owner of the file: ").strip()
            create_file(desiredFileName, desiredFileSize, desiredOwner)

        elif c == "5":
            sys.exit()

        else:
            print("Invalid choice.")
