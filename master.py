import json
import random
import asyncio
import logging
import argparse
from functools import reduce
from datetime import datetime

class Master:
    def __init__(self, algorithm, CONFIG_PATH = 'config.json'):
        self._wait_queue = None
        self.host = '127.0.0.1'
        self.port = 5000
        self.algorithm = algorithm
        
        self.scheduler_map = {'RANDOM': self._random_scheduler, 'LEAST-LOADED': self._least_loaded_scheduler, 'ROUND-ROBIN': self._round_robin_scheduler}

        # Config Dependencies
        # Initialize worker tracker
        # and free slots counter

        with open(CONFIG_PATH) as conf:
            config = json.load(conf)
        self.workers = config['workers']
        self.slot_size = sum([i['slots'] for i in self.workers])

    # Client Section
    # All the client related tasks to talk
    # to the workers go here


    async def _connect_to_worker(self, job_id, payload, slot_index, slots, scheduler_queue, job_rc, slot_rc, slot_rc_lock):
        # Extract the 'schedule_ahead' field from the
        # payload object. If the field is not present assign
        # None to the variable
        schedule_forward = payload.pop('schedule_forward', None)
        task_id = payload['task_id']
        logging.info("Starting task with Task ID: {} of Job ID: {} with the worker".format(task_id, job_id))

        # Open a socket connection to the worker and get the reader writer pair to communicate with worker
        reader, writer = await asyncio.open_connection(self.host, self.workers[slots[slot_index][2]]['port'])

        # Stringify the payload
        payload_string = json.dumps(payload)
        # Send the payload
        writer.write(payload_string.encode())
        await writer.drain()

        # Wait for worker to respond and then
        # close the connection to free up the
        # worker
        data = await reader.read(1000)
        writer.close()
        await writer.wait_closed()
        logging.info("Finished task with Task ID: {} of Job ID: {} with the worker".format(task_id, job_id))

        # If there is a schedule forward field, that means we
        # speculated this task to be the last mapper task to
        # run after this, we schedule the reducer task on the scheduler
        # queue and the tasks will be dealt on a FCFS basis
        if schedule_forward:
            # If the job reference count is not equal to one
            # there is still a map task running (bad speculation
            # that this task was the last map task to complete)
            # In this case defer scheduling for a second and check
            # the reference count again
            while job_rc[job_id] != 1:
                await asyncio.sleep(1)

            # Update refcount to number of reducers
            job_rc[job_id] = len(schedule_forward)
            logging.info("Finished all the mappers of Job ID: {}".format(job_id))
            await scheduler_queue.put((0, sum([random.random() for i in range(5)]), {'job_id': job_id, 'type': 'Reducer', 'tasks': schedule_forward}))
        else:
            # Reduce refcount by 1 as this task has finished
            # execution
            job_rc[job_id] -= 1
            # Add a log for task completion
            if job_rc[job_id] == 0:
                total_job_time = (datetime.now() - job_rc['{}_time'.format(job_id)]).total_seconds()
                # Add task finished log
                logging.info("Finished all the reducers of Job ID: {}".format(job_id))
                logging.info("FINISHED: All tasks of Job ID: {} in {}".format(job_id, total_job_time))

        # Cleanup task on next scheduler run
        slots[slot_index][1] = True
        worker_id = slots[slot_index][2]

        # Change the reference count to show the
        # newly available slots
        logstring = "PLOT: {}\t".format(datetime.now().strftime("%m/%d/%Y,%H:%M:%S"))
        async with slot_rc_lock:
            slot_rc['Total'] += 1
            slot_rc[worker_id] += 1
            for j in range(len(self.workers)):
                logstring += "{}\t".format(self.workers[j]['slots'] - slot_rc[j])
        logging.info(logstring)


    # Scheduler Section
    # All the functions related to scheduling
    # go here

    # Whatever Worker First Scheduling
    # The scheduler that listens to the scheduler queue
    # and schedules task on the workers
    async def _scheduler(self, scheduler_queue, job_rc):
        # This is a reference count for slots
        # This helps us determine the total capacity
        # and per worker capacity to schedule tasks on
        slot_rc = {'Total': self.slot_size}
        worker_count = len(self.workers)
        for i in range(worker_count):
            slot_rc[i] = self.workers[i]['slots']

        # Initialize task tracker
        slots = [[None, True, 0] for i in range(self.slot_size)]
        current_worker = 0
        change_limit = self.workers[0]['slots']

        # Set the index of worker for which the slot belongs to
        for i in range(self.slot_size):
            if (i == change_limit):
                current_worker += 1
                change_limit += self.workers[current_worker]['slots']
            slots[i][2] = current_worker
            
        # A lock for the shared slot_rc
        # Because there are increment and decrement operations on these
        # slots, we need to lock them to avoid using stale slate between
        # these updates
        slot_rc_lock = asyncio.Lock()

        # Scheduler runs indefinitely
        while True:
            # Get the payload from the queue
            task_queue_payload = await scheduler_queue.get()

            # task_queue_payload id is a tuple of (priority, data)
            # A lower priority task will we scheduled over a higher
            # priority task - hence the reducers of completed mappers
            # will be put in front off the pending mapper tasks
            task_payload = task_queue_payload[2]

            job_id = task_payload['job_id']

            # Schedule all the tasks for the job
            for i in task_payload['tasks']:
                # Find the free slot to schedule the task on
                empty_slot = -1
                while empty_slot == -1:
                    # Traverse the slot list looking for
                    # a free slot
                    for j in range(self.slot_size):
                        # If a free slot is found use it
                        # NOTE: The task sharing slot list will
                        #   set a False as True and hence this
                        #   part might not cause any race
                        if slots[j][1]:
                            # The first field stores the
                            # previous task. If this field is
                            # not None, we must cancel the task
                            # that ran previously but now is idle
                            # to free memory on queue
                            if slots[j][0] != None:
                                slots[j][0].cancel()
                                slots[j][0] = None
                            empty_slot = j
                            break

                    # If no empty slot is found defer search
                    # for one second by which time slots might
                    # be available
                    if empty_slot == -1:
                        await asyncio.sleep(1)
                
                # Changet the refcount for the worker
                async with lock:
                    slot_rc['Total'] -= 1
                    slot_rc[slots[empty_slot][2]] -= 1
                    
                slots[empty_slot][1] = False

                # Schedule the task on the Asyncio queue
                slots[free_slot][0] = asyncio.create_task(self._connect_to_worker(job_id, i, empty_slot, slots, scheduler_queue, job_rc, slot_rc, slot_rc_lock))


    # Random Scheduler
    # Random Scheduler randomly selects a worker, checks if the
    # worker has capacity to operate more tasks. If yes, then
    # the task is scheduled on the worker else we search for
    # another worker with free slots
    async def _random_scheduler(self, scheduler_queue, job_rc):
        # This is a reference count for slots
        # This helps us determine the total capacity
        # and per worker capacity to schedule tasks on
        slot_rc = {'Total': self.slot_size}
        worker_count = len(self.workers)
        for i in range(worker_count):
            slot_rc[i] = self.workers[i]['slots']

        # This is the slots list. This list holds all the tasks
        # and relevent details about it. When a tak is completed, we use
        # this list for cleanup
        slots = [[None, True, 0] for i in range(self.slot_size)]

        # A lock for the shared slot_rc
        # Because there are increment and decrement operations on these
        # slots, we need to lock them to avoid using stale slate between
        # these updates
        slot_rc_lock = asyncio.Lock()

        while True:
            # Get the payload from the queue
            task_queue_payload = await scheduler_queue.get()

            # task_queue_payload id is a tuple of (priority, data)
            # A lower priority task will we scheduled over a higher
            # priority task - hence the reducers of completed mappers
            # will be put in front off the pending mapper tasks
            task_payload = task_queue_payload[2]
            # print(task_payload)

            job_id = task_payload['job_id']
            task_type = task_payload['type']
            logging.info("{} tasks of Job ID: {} are being scheduled".format(task_type, job_id))

            # Schedule all the tasks for the job
            for i in task_payload['tasks']:
                total_capacity  = 0
                # Check if capacity exists
                while not total_capacity:
                    # Use lock to read
                    # There is a race condition otherwise because
                    # of the way we increment these rc when tasks
                    # are done
                    async with slot_rc_lock:
                        total_capacity = slot_rc['Total']
                    # Sleep for 1 second if there are no empty
                    # slots as that is the minimum amount of time
                    # for a slot to free up next
                    if not total_capacity:
                        await asyncio.sleep(1)

                random_worker = -1
                # Find a slot in slots to add our task
                # Because we maintain a refcount for tasks, we can be sure
                # that is capacity is greater than 1, there is at least one
                # task slot here that is free.
                # Because we are depending on random, we may get a worker
                # who is busy so we have to loop to find a free worker
                while True:
                    random_worker = random.randint(1, worker_count) - 1
                    current_capacity = 0
                    async with slot_rc_lock:
                        current_capacity = slot_rc[random_worker]

                    # Break if current capacity for this worker is not 0
                    if current_capacity:
                        break

                free_slot = -1
                # Find a free slot within the slot list to put our task in
                # Because we have a refcount, we can be sure if the program
                # has reached this stage there is at least one task slot free
                for j in range(self.slot_size):
                    if slots[j][1]:
                        # If the slot has previous task which is now finished
                        # clean up the task and make the task slot to None
                        if slots[j][0] != None:
                            slots[j][0].cancel()
                            slots[j][0] = None
                        free_slot = j
                        break

                # Update ref count before we
                # start the task
                logstring = "PLOT: {}\t".format(datetime.now().strftime("%m/%d/%Y,%H:%M:%S"))
                async with slot_rc_lock:
                    slot_rc['Total'] -= 1
                    slot_rc[random_worker] -= 1
                    for j in range(len(self.workers)):
                        logstring += "{}\t".format(self.workers[j]['slots'] - slot_rc[j])
                logging.info(logstring)

                # Set all the metadata for the task slot
                slots[free_slot][1] = False
                slots[free_slot][2] = random_worker

                # Schedule the task on the Asyncio queue
                slots[free_slot][0] = asyncio.create_task(self._connect_to_worker(job_id, i, free_slot, slots, scheduler_queue, job_rc, slot_rc, slot_rc_lock))
                
            logging.info("{} tasks of Job ID: {} have been scheduled".format(task_type, job_id))


    # Least Loaded Scheduler
    # Least loaded scheduler takes the least loaded worker
    # and schedules task on the particular worker
    async def _least_loaded_scheduler(self, scheduler_queue, job_rc):
        # This is a reference count for slots
        # This helps us determine the total capacity
        # and per worker capacity to schedule tasks on
        slot_rc = {'Total': self.slot_size}
        worker_count = len(self.workers)
        for i in range(worker_count):
            slot_rc[i] = self.workers[i]['slots']

        # This is the slots list. This list holds all the tasks
        # and relevent details about it. When a tak is completed, we use
        # this list for cleanup
        slots = [[None, True, 0] for i in range(self.slot_size)]

        # A lock for the shared slot_rc
        # Because there are increment and decrement operations on these
        # slots, we need to lock them to avoid using stale slate between
        # these updates
        slot_rc_lock = asyncio.Lock()

        while True:
            # Get the payload from the queue
            task_queue_payload = await scheduler_queue.get()

            # task_queue_payload id is a tuple of (priority, data)
            # A lower priority task will we scheduled over a higher
            # priority task - hence the reducers of completed mappers
            # will be put in front off the pending mapper tasks
            task_payload = task_queue_payload[2]
            # print(task_payload)

            job_id = task_payload['job_id']
            task_type = task_payload['type']
            logging.info("{} tasks of Job ID: {} are being scheduled".format(task_type, job_id))

            # Schedule all the tasks for the job
            for i in task_payload['tasks']:
                total_capacity  = 0
                # Check if capacity exists
                while not total_capacity:
                    # Use lock to read
                    # There is a race condition otherwise because
                    # of the way we increment these rc when tasks
                    # are done
                    async with slot_rc_lock:
                        total_capacity = slot_rc['Total']
                    # Sleep for 1 second if there are no empty
                    # slots as that is the minimum amount of time
                    # for a slot to free up next
                    if not total_capacity:
                        await asyncio.sleep(1)

                # Find the worker with lowest number of tasks scheduled on it
                # out of all the worker whose current capacity is greater than
                # zero
                least_loaded_worker = -1
                
                # Max job running on a worker can be slot_size
                # Hence we set the threshold the max + 1
                min_job_running = self.slot_size + 1
                
                # Iterate over the workers to find the capacity
                for j in range(worker_count):
                    current_spare_capacity = -1
                    # Get the curret capacity behind a lock
                    # There is a race condition for read if
                    # we don't use the lock from the way we
                    # increment the ref count when a task finishes
                    # on the worker
                    async with slot_rc_lock:
                        current_spare_capacity = slot_rc[j]
                    
                    # If the capacity is 0, we can just move to the next worker
                    if current_spare_capacity:
                        # Find the number of jobs running on the worker using
                        # Total capacity and refcount
                        jobs_running = self.workers[j]['slots'] - current_spare_capacity
                        
                        # Update the least_loaded worker in case
                        # this worker has the least job running
                        # on it out of all the workers we hav
                        # seen till here
                        if jobs_running < min_job_running:
                            min_job_running = jobs_running
                            least_loaded_worker = j

                # Note - We cause we total refcount for total free slots and hence
                # there will always be a worker with spare capacity when we try to
                # find a worker which is least loaded

                free_slot = -1
                # Find a free slot within the slot list to put our task in
                # Because we have a refcount, we can be sure if the program
                # has reached this stage there is at least one task slot free
                for j in range(self.slot_size):
                    if slots[j][1]:
                        # If the slot has previous task which is now finished
                        # clean up the task and make the task slot to None
                        if slots[j][0] != None:
                            slots[j][0].cancel()
                            slots[j][0] = None
                        free_slot = j
                        break

                # Update ref count before we
                # start the task
                logstring = "PLOT: {}\t".format(datetime.now().strftime("%m/%d/%Y,%H:%M:%S"))
                async with slot_rc_lock:
                    slot_rc['Total'] -= 1
                    slot_rc[least_loaded_worker] -= 1
                    for j in range(len(self.workers)):
                        logstring += "{}\t".format(self.workers[j]['slots'] - slot_rc[j])
                logging.info(logstring)

                # Set all the metadata for the task slot
                slots[free_slot][1] = False
                slots[free_slot][2] = least_loaded_worker

                # Schedule the task on the Asyncio queue
                slots[free_slot][0] = asyncio.create_task(self._connect_to_worker(job_id, i, free_slot, slots, scheduler_queue, job_rc, slot_rc, slot_rc_lock))
                
            logging.info("{} tasks of Job ID: {} have been scheduled".format(task_type, job_id))


    # Round Robin Scheduler
    # The scheduler goes around the free server list in a Round Robin
    # manner to schedule task on the workers with capacity
    async def _round_robin_scheduler(self, scheduler_queue, job_rc):
        # This is a reference count for slots
        # This helps us determine the total capacity
        # and per worker capacity to schedule tasks on
        slot_rc = {'Total': self.slot_size}
        worker_count = len(self.workers)
        for i in range(worker_count):
            slot_rc[i] = self.workers[i]['slots']

        # This is the slots list. This list holds all the tasks
        # and relevent details about it. When a tak is completed, we use
        # this list for cleanup
        slots = [[None, True, 0] for i in range(self.slot_size)]

        # A lock for the shared slot_rc
        # Because there are increment and decrement operations on these
        # slots, we need to lock them to avoid using stale slate between
        # these updates
        slot_rc_lock = asyncio.Lock()
        
        # The start point
        current_worker = 0

        while True:
            # Get the payload from the queue
            task_queue_payload = await scheduler_queue.get()

            # task_queue_payload id is a tuple of (priority, data)
            # A lower priority task will we scheduled over a higher
            # priority task - hence the reducers of completed mappers
            # will be put in front off the pending mapper tasks
            task_payload = task_queue_payload[2]
            # print(task_payload)

            job_id = task_payload['job_id']
            task_type = task_payload['type']
            logging.info("{} tasks of Job ID: {} are being scheduled".format(task_type, job_id))

            # Schedule all the tasks for the job
            for i in task_payload['tasks']:
                total_capacity  = 0
                # Check if capacity exists
                while not total_capacity:
                    # Use lock to read
                    # There is a race condition otherwise because
                    # of the way we increment these rc when tasks
                    # are done
                    async with slot_rc_lock:
                        total_capacity = slot_rc['Total']
                    # Sleep for 1 second if there are no empty
                    # slots as that is the minimum amount of time
                    # for a slot to free up next
                    if not total_capacity:
                        await asyncio.sleep(1)

                current_worker_capacity = 0
                async with slot_rc_lock:
                    current_worker_capacity = slot_rc[current_worker]
                    
                # # Approach 1 - No Go Ahead
                # # When a worker in the Round Robin traversal
                # # is busy at full capacity, wait for the worker
                # # to be free
                # # Wait for the current worker to be free
                # # to schedule on the particular worker
                # while not current_worker_capacity:
                #     await asyncio.sleep(1)
                #     async with slot_rc_lock:
                #         current_worker_capacity = slot_rc[current_worker]
                
                # Approach 2 - Go Ahead 
                # If the current worker is loaded at the full capacity, go ahead
                # with the traversal in round robin fashion
                while not current_worker_capacity:
                    # Go to next worker in search of the free worker in a
                    # round robin fashion
                    current_worker = (current_worker + 1) % worker_count
                    async with slot_rc_lock:
                        current_worker_capacity = slot_rc[current_worker]
                    

                # Note - We cause we total refcount for total free slots and hence
                # there will always be a worker with spare capacity when we try to
                # find a worker which is least loaded

                free_slot = -1
                # Find a free slot within the slot list to put our task in
                # Because we have a refcount, we can be sure if the program
                # has reached this stage there is at least one task slot free
                for j in range(self.slot_size):
                    if slots[j][1]:
                        # If the slot has previous task which is now finished
                        # clean up the task and make the task slot to None
                        if slots[j][0] != None:
                            slots[j][0].cancel()
                            slots[j][0] = None
                        free_slot = j
                        break

                # Update ref count before we
                # start the task
                logstring = "PLOT: {}\t".format(datetime.now().strftime("%m/%d/%Y,%H:%M:%S"))
                async with slot_rc_lock:
                    slot_rc['Total'] -= 1
                    slot_rc[current_worker] -= 1
                    for j in range(len(self.workers)):
                        logstring += "{}\t".format(self.workers[j]['slots'] - slot_rc[j])
                logging.info(logstring)

                # Set all the metadata for the task slot
                slots[free_slot][1] = False
                slots[free_slot][2] = current_worker

                # Schedule the task on the Asyncio queue
                slots[free_slot][0] = asyncio.create_task(self._connect_to_worker(job_id, i, free_slot, slots, scheduler_queue, job_rc, slot_rc, slot_rc_lock))

            logging.info("{} tasks of Job ID: {} have been scheduled".format(task_type, job_id))


   # Middleware
   # Takes payload from wait queue and put it on the scheduler queue
   # We maintain two queues for the sake of encapsulation of the
   # scheduler from server and moreover it allows us to to implement
   # Round Robin Scheduling better
    async def _add_to_scheduler_queue(self, wait_queue, scheduler_queue, job_rc):
        while True:
            task_payload = await wait_queue.get()
            job_id = task_payload['job_id']
            job_rc['{}_time'.format(job_id)] = datetime.now()
            logging.info("ARRIVED: Job ID: {} arrived at scheduler queue".format(job_id))

            # Separate out map tasks and reduce tasks
            # We know a reduce task cannot be scheduled until all the mappers are done
            # So we sort the mapper by their run duration and tag the reducer task list
            # to the last mapper task. When this mapper task finishes, the final map task
            # will schedule the reduce tasks on the scheduling queue
            map_tasks = sorted(task_payload['map_tasks'], key = lambda x: x['duration'])
            reduce_tasks = sorted(task_payload['reduce_tasks'], key = lambda x: x['duration'])
            map_tasks[-1]['schedule_forward'] = reduce_tasks

            # job_rc need not be locked as there won't be any mutations that will cause a
            # data race. The only time job_rc fields are modified is when the reduce tasks
            # are scheduled and no two process will try to write to the same field in the
            # job_rc dict at the same time
            job_rc[job_id] = len(map_tasks)

            # WE cannot compare two dictionaries and priority queue compares elements to find the location
            # in the queue and hence we need to avoid collision of second spot and hence we use random
            await scheduler_queue.put((1, sum([random.random() for i in range(5)]), {'job_id': job_id, 'type': 'Mapper', 'tasks': map_tasks}))


    # Creates the scheduler tasks and returns
    # the handlers to the main method
    def _start_scheduler(self, wait_queue):
        # This is the job reference count dictionary
        # This is used to keep a reference count of the tasks in the
        # particular job. When reference count hits 0, we can start logging
        job_rc = dict()
        scheduler = self.scheduler_map[self.algorithm]

        scheduler_queue = asyncio.PriorityQueue()
        scheduler_task = asyncio.create_task(scheduler(scheduler_queue, job_rc))
        logging.info('Started scheduler')

        queue_task = asyncio.create_task(self._add_to_scheduler_queue(wait_queue, scheduler_queue, job_rc))
        logging.info('Starting server middleware')

        return [scheduler_task, queue_task]


    # Server Section
    # All functions server related
    # can be found here


    # Handle request coming from socket
    # The function reads the data from the socket
    # and sends the payload to be parsed and
    # queued
    async def _handle_request(self, reader, writer):
        logging.info("New request received")
        # Read the payload
        # In case of an incomplete JSON error
        # increse the number in parameter and
        # try again - the message string might
        # be larger than X bytes
        data = await reader.read(1000)

        # Close connection after reading as the
        # request.py client isn't waiting for any
        # answers from the master
        writer.close()

        # Get the data as a string
        message = data.decode()
        await self._add_to_wait_queue(message)


    # Parse the payload forwarded by _handle_request
    # Creates a dictionary from the JSON string
    # and adds it to wait queue
    async def _add_to_wait_queue(self, message):
        # Parse payload from client as a
        # Python dictionary to ease scheduling
        payload_dictionary = json.loads(message)
        await self._wait_queue.put(payload_dictionary)


    # Start the server and listen for request
    # Once connection is established, the _handle_request method
    # steps in to handle the payload received
    async def _start_server(self, queue):
        # Start a server on localhost on port 5000
        self._wait_queue = queue
        server = await asyncio.start_server(self._handle_request, self.host, self.port)

        # Get and display the socket address
        # on the console
        addr = server.sockets[0].getsockname()
        # print('Master started on: {}:{} using the {} scheduler.'.format(addr[0], addr[1], self.algorithm))
        logging.info('Master started on: {}:{} using the {} scheduler.'.format(addr[0], addr[1], self.algorithm))

        # Run server forever
        async with server:
            await server.serve_forever()


    # Creates all the concurrent tasks needed
    # for master to function
    async def _main(self):
        wait_queue = asyncio.Queue()
        scheduler_tasks = self._start_scheduler(wait_queue)
        server_task = asyncio.create_task(self._start_server(wait_queue))
        await asyncio.gather(*scheduler_tasks, server_task)


    # Entry point to start the master
    def entrypoint(self):
        asyncio.run(self._main())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-sa", "--scheduling-algorithm", choices=["random", "least-loaded", "round-robin"], default="random", help="Select the scheduling algorithms for the app master")
    args = parser.parse_args()
    
    logging.basicConfig(format='%(asctime)s  ::  %(message)s', filename='log/master.log', level=logging.INFO)
    logging.info('Starting master')

    # We need some amount off randomness everywhere
    # This seed is used t preserve uniformity in
    # randomness to get reproducible results
    random.seed(1729)
    master = Master(args.scheduling_algorithm.upper())
    master.entrypoint()
