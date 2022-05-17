from audioop import add
from curses import REPORT_MOUSE_POSITION
from itertools import count
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient
from mbientlab.metawear.cbindings import *
from mbientlab.metawear import MetaWear, libmetawear, parse_value, cbindings
from mbientlab.warble import *
from threading import Event
from ctypes import c_void_p, cast, POINTER
import numpy as np
import asyncio
import base64
import json
from Device import State
import csv  
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import os

# Create an S3 access object
s3 = boto3.resource('s3')
bucket = 'storageimu'

app = FastAPI()

ws_connected_event = asyncio.Event()
start_event = asyncio.Event()
data_queue = asyncio.Queue()

start_event.set()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/register")
async def register():
    return {"message": "register"}

@app.get("/start")
async def start():
    if not ws_connected_event.is_set():
        return {"message": "no websocket connection"}
    start_event.set()
    return {"message": "start"}

@app.get("/stop")
async def stop():
    if not ws_connected_event.is_set():
        return {"message": "no websocket connection"}
    start_event.clear()
    return {"message": "stop"}

@app.websocket("/ws")
async def websocket(websocket: WebSocket):
    await websocket.accept()
    ws_connected_event.set()
    try:
        while True:
            data = await data_queue.get()
            await websocket.send_json(data)
    except WebSocketDisconnect:
    # except Exception:
        ws_connected_event.clear()

async def data_generator(period=0.25):
    e = Event()

    def device_discover_task(result):
        global address
        if (result.has_service_uuid(MetaWear.GATT_SERVICE)):
            # grab the first discovered metawear device
            address = result.mac
            e.set()

    BleScanner.set_handler(device_discover_task)
    BleScanner.start()
    e.wait()
    global s, d
    # connect
    d = MetaWear(address)
    d.connect()
    print("Connected to " + d.address + " over " + ("USB" if d.usb.is_connected else "BLE"))
    BleScanner.stop()
    s = State(d)
    print("Configuring %s" % (s.device.address))
    s.setup()
    
    labels = ['ACC_X','ACC_Y','ACC_Z','GYR_X','GYR_Y','GYR_Z','MAG_X','MAG_Y','MAG_Z']
    data = np.empty(len(labels)).tolist()
    while True:
        # check whether start command is sent
        if start_event.is_set():
            data_get = s.get_data_packed()
            data_get = np.array(list(data_get.values())).astype(np.int32)
            for ch in range(9):
                data[ch] =   base64.b64encode(data_get[ch].tobytes()).decode('utf8')
            yield {'labels': labels, 'data': data}
        await asyncio.sleep(period)

async def record_data(period = 0.01):
    labels = ['Timestamp','ACC_X','ACC_Y','ACC_Z','GYR_X','GYR_Y','GYR_Z','MAG_X','MAG_Y','MAG_Z']
    count = 0
    while True:
        # check whether start command is sent
        if start_event.is_set():
            count += 1
            #Create flie CSV
            if  count == 1:
                my_date = datetime.now()
                pathfile = 'Data/' + str(my_date.strftime('%Y_%m_T%H.%M.%S')) + '.csv'
                with open(pathfile, 'w') as f:
                    writer = csv.DictWriter(f, fieldnames=labels)
                    writer.writeheader()
            else:
                pass
            with open(pathfile, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=labels)
                data_get = s.get_data_stream()
                updict = {'Timestamp' : str(datetime.now().strftime('%H:%M:%S'))}
                updict.update(data_get)
                for data in [updict]:
                    writer.writerow(data)
        else:
            try:
                if count > 0:
                    s3.meta.client.upload_file(pathfile, bucket, pathfile)
            except ClientError as e:
                logging.error(e)
                return False
            count = 0
        await asyncio.sleep(period)

async def read_data():
    async for data in data_generator():
        # if there is websocket connection
        if ws_connected_event.is_set():
            data_queue.put_nowait(data)

@app.on_event("startup")
async def startup():
    asyncio.create_task(read_data())
    asyncio.create_task(record_data())