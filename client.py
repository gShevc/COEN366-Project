import socket
import threading
import os
import binascii
import sys

#########################################################
# CONFIG
#########################################################

CLIENT_HOST = "0.0.0.0"
CLIENT_UDP_PORT = 9001      # CHANGE for each peer you run
CLIENT_TCP_PORT = 9101      # CHANGE for each peer you run
SERVER_ADDR = ("localhost", 8888)

LOCAL_NAME = None
LOCAL_ROLE = None

STORAGE_DIR = "storage_chunks"
os.makedirs(STORAGE_DIR, exist_ok=True)

udp_socket = None

# Used only during backup as the owner
pending_backup = {
    "rq": None,
    "fileName": None,
    "chunkSize": None,
    "peers": [],
    "chunks_ok": set(),
}


#########################################################
# TCP FUNCTIONS (Option C)
#########################################################

def TCPConnection(host, port):
    """
    Storage peer: binds & listens for incoming TCP chunk.
    Returns the accepted connection and listener.
    """
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind((host, port))
    listener.listen(1)
    print(f"[TCP-LISTEN] Listening for chunk on TCP {port}")
    conn, addr = listener.accept()
    print(f"[TCP-LISTEN] Accepted connection from {addr}")
    return conn, listener


def TCPConnectAndSend(host, port, header_bytes, payload_bytes):
    """
    Owner peer: connects to storage peer and sends chunk.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(header_bytes + payload_bytes)
    s.close()


#########################################################
# UTILS
#########################################################

def udp_send(msg, addr):
    udp_socket.sendto(msg.encode("utf-8"), addr)

def crc32(data):
    return binascii.crc32(data) & 0xffffffff


#########################################################
# REGISTER / DEREGISTER / BACKUP REQUEST
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
# STORAGE_TASK (Reply ACCEPTED)
#########################################################

def handle_STORAGE_TASK(parts):
    """
    STORAGE_TASK|RQ#|fileName|chunkSize|peerName|
    As storage peer: respond ACCEPTED (always).
    """
    rq = parts[1]
    fileName = parts[2]
    chunkSize = parts[3]
    peerName = parts[4]

    reply = f"STORAGE_TASK|{rq}|{fileName}|{chunkSize}|{peerName}|ACCEPTED|"
    udp_send(reply, SERVER_ADDR)
    print(f"[STORAGE_TASK] ACCEPTED storing chunk of {fileName}")


#########################################################
# BACKUP_PLAN (Owner receives plan of peers to contact)
#########################################################

def handle_BACKUP_PLAN(parts):
    """
    BACKUP_PLAN|RQ#|fileName|[peer:ip:udp:tcp,...]|chunkSize|
    Owner: must contact all peers with STORE_REQ.
    """
    rq = parts[1]
    fileName = parts[2]
    peerListStr = parts[3].strip("[]")
    chunkSize = int(parts[4])

    peers = []
    for entry in peerListStr.split(","):
        entry = entry.strip()
        if entry == "":
            continue
        name, ip, udp, tcp = entry.split(":")
        peers.append((name, ip, int(udp), int(tcp)))

    pending_backup["rq"] = rq
    pending_backup["fileName"] = fileName
    pending_backup["chunkSize"] = chunkSize
    pending_backup["peers"] = peers
    pending_backup["chunks_ok"] = set()

    # send STORE_REQ to each peer
    for i, (name, ip, udp, tcp) in enumerate(peers):
        msg = f"STORE_REQ|{rq}|{fileName}|{i}|{LOCAL_NAME}|{CLIENT_UDP_PORT}|{CLIENT_TCP_PORT}|"
        udp_send(msg, (ip, udp))

    print(f"[BACKUP_PLAN] Sent STORE_REQ to all peers for file {fileName}")


#########################################################
# SEND_CHUNK → we must send the chunk via TCP
#########################################################

def handle_SEND_CHUNK(parts, addr):
    """
    SEND_CHUNK|RQ#|fileName|peerName|tcpPort|
    As owner: now send the chunk to this peer.
    """
    rq = parts[1]
    fileName = parts[2]
    peerName = parts[3]
    tcpPort = int(parts[4])
    peer_ip = addr[0]

    # compute chunk index
    peers = pending_backup["peers"]
    names = [p[0] for p in peers]
    idx = names.index(peerName)

    # Read file or create fake data
    if os.path.exists(fileName):
        with open(fileName, "rb") as f:
            full_data = f.read()
    else:
        chunkSize = pending_backup["chunkSize"]
        full_data = b'X' * (chunkSize * len(peers))

    chunkSize = pending_backup["chunkSize"]
    start = idx * chunkSize
    chunk = full_data[start:start+chunkSize]

    header = f"{fileName}|{len(chunk)}|".encode("utf-8")

    TCPConnectAndSend(peer_ip, tcpPort, header, chunk)
    print(f"[OWNER] Sent chunk to {peerName} ({peer_ip}:{tcpPort})")


#########################################################
# STORAGE PEER: RECEIVE CHUNK VIA TCP
#########################################################

def handle_STORE_REQ(parts, addr):
    """
    STORE_REQ|RQ#|fileName|chunkID|ownerName|ownerUDP|ownerTCP|
    Start TCP listener and send SEND_CHUNK back to owner.
    """
    rq = parts[1]
    fileName = parts[2]
    chunkID = int(parts[3])
    ownerUDP = int(parts[5])

    # Start TCP listener
    conn, listener = TCPConnection("0.0.0.0", CLIENT_TCP_PORT)

    # Inform owner the TCP port is ready
    msg = f"SEND_CHUNK|{rq}|{fileName}|{LOCAL_NAME}|{CLIENT_TCP_PORT}|"
    udp_send(msg, (addr[0], ownerUDP))

    # Receive chunk
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

    # Save chunk
    outPath = os.path.join(STORAGE_DIR, f"{fileName}.chunk{chunkID}")
    with open(outPath, "wb") as f:
        f.write(chunk)

    print(f"[STORAGE] Stored chunk {chunkID} of {fileName}")

    # Notify owner
    udp_send(f"CHUNK_OK|{rq}|{fileName}|{chunkID}|", (addr[0], ownerUDP))

    # Notify server
    udp_send(f"STORE_ACK|{rq}|{fileName}|{chunkID}|{LOCAL_NAME}|", SERVER_ADDR)


#########################################################
# OWNER receives CHUNK_OK
#########################################################

def handle_CHUNK_OK(parts):
    rq = parts[1]
    fileName = parts[2]
    chunkID = int(parts[3])

    pending_backup["chunks_ok"].add(chunkID)

    print(f"[OWNER] CHUNK_OK received for chunk {chunkID}")

    # if all chunks ok → backup done
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
# MAIN
#########################################################

def display_menu():
    print("\n1. Register")
    print("2. Deregister")
    print("3. Backup File")
    print("4. Exit")

if __name__ == "__main__":
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind((CLIENT_HOST, CLIENT_UDP_PORT))
    print(f"[UDP] Client running on {CLIENT_HOST}:{CLIENT_UDP_PORT}")

    threading.Thread(target=listen_udp, daemon=True).start()

    counter = 1

    while True:
        display_menu()
        c = input("Choice: ")

        if c == "1":
            LOCAL_NAME = input("Name: ")
            LOCAL_ROLE = input("Role (BOTH/STORAGE/OWNER): ")
            RegisterClient(f"RQ{counter}", LOCAL_NAME, LOCAL_ROLE,
                           CLIENT_HOST, CLIENT_UDP_PORT, CLIENT_TCP_PORT, 1024)
            counter += 1

        elif c == "2":
            name = input("Name to deregister: ")
            DeregisterClient(f"RQ{counter}", name)
            counter += 1

        elif c == "3":
            fileName = input("File name: ")
            if os.path.exists(fileName):
                data = open(fileName, "rb").read()
            else:
                size = int(input("Fake file size: "))
                data = b"X"*size

            checksum = crc32(data)
            BackupRequest(f"RQ{counter}", fileName, len(data), checksum)
            counter += 1

        elif c == "4":
            sys.exit()

        else:
            print("Invalid choice.")
