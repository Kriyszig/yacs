import sys
import json
import asyncio
import logging
from datetime import datetime

class Worker:
    def __init__(self, port):
        self.ip_addr = '127.0.0.1'
        self.port = port

    async def task_harness(self, reader, writer):
        logging.info('Received request from the Master')
        data = await reader.read(1000)
        payload_string = data.decode()
        task = json.loads(payload_string)
        
        task_id = task['task_id']
        task_runtime = task['duration']
        logging.info('STARTING: Task with Task ID: {} on Worker'.format(task_id))
        start_time = datetime.now()
        await asyncio.sleep(task_runtime)
        logging.info('ENDING: Task with Task ID: {} on Worker that ran for {}s'.format(task_id, (datetime.now() - start_time).total_seconds()))
        
        status_m = {'code': 200, 'response' : 'Success'}
        send_m = json.dumps(status_m)
        
        logging.info('Communicating the task end to master')
        writer.write(send_m.encode())
        await writer.drain()
        writer.close()

    async def main(self):     
        server = await asyncio.start_server(self.task_harness, self.ip_addr, self.port)
        # Get and display the socket address
        # on the console
        addr = server.sockets[0].getsockname()
        # print("Worker started on: {}:{}".format(addr[0], addr[1]))
        logging.info('Worker Started')
            
        async with server:
            await server.serve_forever()
            
    def entrypoint(self):
        asyncio.run(self.main())

if __name__ == "__main__":
    if (len(sys.argv) < 2):
        sys.exit('Worker needs to be run specifying the port to listen on\nUsage example:\n\t./worker.py 65565\nwhere 65565 is the port where worker listens for task')
    
    port = 0
    try:
        port = int(sys.argv[1])
    except Exception:
        sys.exit('The port number must be an integer\nUsage example:\n\t./worker.py 65565\nwhere 65565 is the port where worker listens for task')
    
    logging.basicConfig(format='%(asctime)s  ::  %(message)s', filename='log/worker_{}.log'.format(port), level=logging.INFO)
    logging.info('Starting worker')

    worker = Worker(port)
    worker.entrypoint()
