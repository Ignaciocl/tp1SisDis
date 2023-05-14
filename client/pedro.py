import json
import socket
import time

LINES_TO_SEND = 15  # It must have less than 8192 chars


def formatSendMessage(data):
    strToSend = f'{json.dumps(data)}'
    strToSend = f'{str(len(strToSend)).zfill(5)}{strToSend}'
    return strToSend


def sendCSVFile(nameFile, client):
    print(f'sending {nameFile}')
    data = {
        "file": nameFile.split('/')[-1][:-4],
        "data": []
    }
    with open(nameFile) as f:
        headers = f.readline().rstrip('\n').split(',')
        isEof = False
        sent = []
        while not isEof:
            lines = []
            for po in range(LINES_TO_SEND):
                line = f.readline()
                if not line:
                    isEof = True
                    break
                d = {}
                values = line.rstrip('\n').split(',')
                for i in range(len(values)):
                    d[headers[i]] = values[i]
                lines.append(d)
            data["data"] = lines
            strToSend = formatSendMessage(data)
            d = bytes(strToSend, 'utf-8')
            client.send(d)
            sent.append(strToSend)
            receiveData(client)
        data["data"], data["eof"] = [], True
        strToSend = formatSendMessage(data)
        client.send(bytes(strToSend, 'utf-8'))
        sent.append(strToSend)
        resServer = receiveData(client)
        print(json.loads(resServer))


def receiveData(sock):
    bytesToRead = b''
    while len(bytesToRead) < 5:
        bytesToRead += sock.recv(5 - len(bytesToRead))
    return sock.recv(int(bytesToRead))


if __name__ == "__main__":
    a = time.time()
    folders = ['montreal', 'toronto', 'washington']
    files = ['stations.csv', 'weather.csv', 'trips.csv']
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 3034))
        for x in folders:
            for y in files:
                sendCSVFile(f'./files/{x}/{y}', s)
    res = {}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 3334))
        while True:
            dataSending = formatSendMessage({"please": "give me my data"})
            s.send(bytes(dataSending, 'utf-8'))
            nextTime = receiveData(s)
            if len(nextTime) > 5:
                res = json.loads(nextTime)
                break
            print("polling failed, waiting a little bit")
            time.sleep(1)  # to not overload the server, this line could easily not be here
    b = time.time()
    c = b-a
    print(f"time for response is {c} and response is {res}")

