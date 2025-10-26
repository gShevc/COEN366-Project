import socket 
import threading
import os  # Checks if files exist

def handle_client(conn):

        while True:
            filename = conn.recv(1024).decode('utf-8')
            if not filename:
                break
            if os.path.exists(filename):
                # send token and filesize separated by a space so client can parse
                conn.send(b"EXISTS " + str(os.path.getsize(filename)).encode('utf-8'))
                # Opens file in binary read mode reads chunks
                with open(filename, 'rb') as f:
                    bytes_read = f.read(1024)
                    while bytes_read:
                        conn.send(bytes_read)
                        bytes_read = f.read(1024)
                print(f"Sent: {filename}")
            else:
                conn.send(b"ERR")
        conn.close()
def start_server(host='127.0.0.1',port=65432): # Initialize the Server create a tcp socket
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.bind((host,port))
    sock.listen(5)
    print(f"Listening on {host}: {port}")
    while True:
        conn, addr = sock.accept()
        print(f"Connected by  {addr}")
        # Create a new thread for the client connection
        threading.Thread(target=handle_client, args=(conn,)).start()
        #Create a new thread with client connection
def request_file(host='localhost', port=65432, filename=''):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host,port))
    # send only the basename so server looks for the file in its working directory
    basename = os.path.basename(filename)
    sock.sendall(basename.encode('utf-8'))

    response = sock.recv(1024).decode('utf-8')
    if response.startswith("EXISTS"):
        filesize = int(response.split()[1])
        print(f"File exists, size:{filesize} bytes")
        file_save_as = basename
        with open(os.path.join(os.getcwd(), file_save_as), 'wb') as f:
            bytes_received = 0

            while bytes_received < filesize:
                bytes_read = sock.recv(1024)
                if not bytes_read:
                    break
                f.write(bytes_read)
                bytes_received += len(bytes_read)
        print(f"Download: {file_save_as} ")

    else:
        print("File does not exist on server")
    sock.close()
    print("File does not exist on server")
    sock.close()

if __name__ == "__main__":
    choice = input("Start server (s) or request file (r)?")

    if choice.lower() == 's':
        start_server()
    elif choice.lower() == 'r':
        filename = input("Enter Name of the file: ")
        request_file(filename=filename)