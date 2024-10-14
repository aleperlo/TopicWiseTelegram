from datetime import datetime, timedelta, timezone
from worker import MonitoringWorker
import multiprocessing
import pymongo
import queue


class MonitoringMaster:
    def __init__(self, workers, connection_string: str, threshold_check=12, dbname='GroupMonitoring_on_Telegram', can_join=[]):

        self.n_processes = len(workers)
        self.workers = workers
        self.processes = []
        self.busy = [False for _ in range(self.n_processes)]

        try:
            print(f"{datetime.now()} - [MASTER]: Opening mongodb connection...")
            myclient = pymongo.MongoClient(connection_string)
            self.db = myclient[dbname]
        except Exception as e:
            print(f"{datetime.now()} - [!] Error in opening database connection: {e}")
            exit(-1)
        print(f"{datetime.now()} - [MASTER]: Creating leave queues, wait queues, " +
              "processes queue, task list")
        self.result_queue = multiprocessing.Queue()
        self.process_queue = multiprocessing.Queue()
        # self.tasks = multiprocessing.Queue()
        self.tasks = []
        self.process_queue_timeout = 3
        self.threshold_check = threshold_check
        if len(can_join) == 0:
            self.can_join = [True for _ in range(self.n_processes)]
        else:
            self.can_join = can_join


    def run_workers(self):
        # Create and start worker process
        print(f"{datetime.now()} - [MASTER]: Launching workers... ")
        for i, worker in enumerate(self.workers):
            # start new process
            self.process_queue.put(i)
            self.tasks.append(multiprocessing.Queue())
            worker.bind(i, self.tasks[i], self.result_queue, self.process_queue)
            process = multiprocessing.Process(target=worker.launch_client)
            process.start()
            self.processes.append(process)


    def crawl(self, assign_task='both', tdelta=24):
        """Assign tasks to the worker according to the command specified in ```assign_task```:
        - ```join``` only perform ```TRY_JOIN```;
        - ```check``` only perform ```CHECK_UPDATES```, ```TRY_JOIN``` but just for groups you sent a request;
        - ```both``` (default) assign TRY_JOIN and ```CHECK_UPDATES```, giving priority to the latter.
        """
        if assign_task == 'check':
            update_until = datetime.now(tz=timezone.utc) + timedelta(hours=tdelta)
        message = 'join and check' if assign_task == 'both' else assign_task
        print(f"{datetime.now()} - [MASTER] Starting to {message} groups")
        x = self.db['tbp'].find_one()
        just_checked_results = False
        while x is not None or assign_task == 'check':
            if assign_task == 'check' and datetime.now(tz=timezone.utc) > update_until:
                break
            if just_checked_results or not self.process_queue.empty():
                try:
                    worker_id = self.process_queue.get(timeout=self.process_queue_timeout)
                except queue.Empty:
                    just_checked_results = False
                    continue

                # try to assign task CHECK_WAIT
                task = {
                    'name': 'CHECK_WAIT',
                    'data': []
                }
                # check if any groups for which a request was sent can be checked
                date_threshold = datetime.now(tz=timezone.utc) - timedelta(hours=self.threshold_check)
                group = self.db['groups'].find_one({'last_update': {'$lt': date_threshold}, 'state': 'waiting', 'worker_id': worker_id}, sort={'last_update': 1})
                # if possible, assign the CHECK_WAIT task
                if group is not None:
                    task['data'] = {
                        'username': group['username'],
                        'id': group['id']
                    }
                    self.db['groups'].update_one(
                        {'username': group['username']},
                        {'$set': {'state': 'joining'}}
                    )
                    self.tasks[worker_id].put(task)
                    self.busy[worker_id] = True
                    print(f"{datetime.now()} - [MASTER] Assigning task {task['name']} for a group you already sent a request to")
                    # after assigning a task, get results
                    just_checked_results = True
                    self.get_results()
                    continue

                # try to assign task CHECK_UPDATES
                task = {
                    'name': 'CHECK_UPDATES',
                    'data': []
                }
                # check if any groups can be checked
                date_threshold = datetime.now(tz=timezone.utc) - timedelta(hours=self.threshold_check)
                group = self.db['groups'].find_one({'last_update': {'$lt': date_threshold}, 'state': 'inside', 'worker_id': worker_id}, sort={'last_update': 1})
                # if possible, assign the CHECK_UPDATES task
                if group is not None and (assign_task != 'join' or not self.can_join[worker_id]):
                    task['data'] = {
                        'id': group['id'],
                        'username': group['username'],
                        'offset_date': group['last_update']
                    }
                    self.db['groups'].update_one(
                        {'id': group['id']},
                        {'$set': {'state': 'checking'}})
                    self.tasks[worker_id].put(task)
                    self.busy[worker_id] = True
                    print(f"{datetime.now()} - [MASTER] Assigning task {task['name']}")
                    # after assigning a task, get results
                    just_checked_results = True
                    self.get_results()
                    continue

                # assign task TRY_JOIN for groups you have not asked to be accepted in yet
                if assign_task != 'check' and self.can_join[worker_id]:
                    task = {
                        'name': 'TRY_JOIN',
                        'data': []
                    }
                    x = self.db['tbp'].find_one({})
                    if x is not None:
                        if self.db['groups'].find_one({'username': x['username']}) is None:
                            # assign task
                            task['data'] = {
                                'username': x['username']
                            }
                            self.tasks[worker_id].put(task)
                            self.busy[worker_id] = True
                            # update x
                            x['state'] = 'joining'
                            x['last_update'] = datetime.now(tz=timezone.utc)
                            x['worker_id'] = worker_id
                            # update db
                            x.pop('_id', None)
                            self.db['groups'].insert_one(x)
                            print(f"{datetime.now()} - [MASTER] Assigning task {task['name']}")
                            # after assigning a task, get results
                            just_checked_results = True
                            self.get_results()
                        self.db['tbp'].delete_many({'username': x['username']})
                        continue
                    else:
                        self.process_queue.put(worker_id)
                        # no groups need to be joined: exit
                        break
                # no task assigned: put process back in process queue and check results
                self.process_queue.put(worker_id)
                just_checked_results = True
                self.get_results()
            else:
                just_checked_results = True
                self.get_results()
        print(f"{datetime.now()} - [MASTER]: tbp collection is empty or check interval has expired, stopping crawling")
    

    def update_usernames(self, topic='all'):
        if topic == 'all':
            query = {'state': 'inside'}
        else:
            query = {'state': 'inside', 'topic': topic}
        cursor = self.db['groups'].find(query)

        finished = [False for _ in range(self.n_processes)]
        worker_groups = [[] for _ in range(self.n_processes)]
        for group in cursor:
            worker_id = group['worker_id']
            worker_groups[worker_id].append(group)
        
        while not all(finished):
            worker_id = self.process_queue.get()
            if len(worker_groups[worker_id]) == 0:
                finished[worker_id] = True
                continue
            group = worker_groups[worker_id].pop(0)
            task = {
                'name': 'CHECK_USERNAME',
                'data': {}
            }
            task['data'] = {
                'username': group['username'],
                'id': group['id']
            }
            print(f"{datetime.now()} - [MASTER] Assigning task {task['name']}")
            self.tasks[worker_id].put(task)
            self.get_results()
        self.get_results()
        for i in range(self.n_processes):
            self.process_queue.put(i)


    def get_results(self, block=False):
        while not self.result_queue.empty() or block:
            result = self.result_queue.get()
            print(f"{datetime.now()} [MASTER] Getting result for the username {result['username']} with code {result['code']}")

            if result['code'] == "JOIN_SUCCESS":
                n_messages = result['messages']
                print(f"{datetime.now()} - [MASTER] New messages found in group {result['username']}: {n_messages}")
                # change state to inside
                # WARNING: collection may not exists if no messages were found in the group
                # unlikely if the mau criteria is used
                self.db['groups'].update_one(
                    {'username': result['username']},
                    {
                        '$set': {'messages': {'timestamp': result['timestamp'], 'n_messages': result['messages'], 'first': True},
                                 'state': 'inside', 'id': result['id'],
                                 'last_update': result['timestamp'],
                                 'first_message_date': result['first_message'],
                                 'collection_name': f"messages_{result['id']}"}
                    })
                if 'full_entity' in result:
                    self.db['groups'].update_one(
                        {'username': result['username']},
                        {'$set': {'full_entity': result['full_entity']}}
                    )

            elif result['code'] == "UPDATE_SUCCESS":
                n_messages = result['messages']
                # change state to inside, add record to update_date
                self.db['groups'].update_one(
                    {'id': result['id']},
                    {
                        '$set': {'last_update': result['timestamp'], 'state': 'inside'},
                        '$push': {'update_date': {'timestamp': result['timestamp'], 'n_messages': n_messages}}
                    })
                print(f"{datetime.now()} - [MASTER] New messages found in group {result['username']}: {n_messages}")
            
            elif result['code'] == "REQUEST_SENT":
                # change state to waiting, add record to update_date
                self.db['groups'].update_one(
                    {'username': result['username']},
                    {
                        '$set': {'state': 'waiting', 'last_update': result['timestamp'], 'id': result['id']},
                        '$push': {'error_messages': {'timestamp': result['timestamp'], 'message': result['error_messages']}}
                    }
                )
                if 'full_entity' in result:
                    self.db['groups'].update_one(
                        {'username': result['username']},
                        {'$set': {'full_entity': result['full_entity']}}
                    )
                print(f"{datetime.now()} - [MASTER] Waiting to be approved in group {result['username']}")
            
            elif result['code'] == "ENTITY_FOUND":
                new_entity = result['new_entity']
                new_username = result['new_entity']['chats'][0]['username']
                old_username = result['username']
                if new_username != old_username:
                    self.db['groups'].update_one(
                        {'username': result['username']},
                        {
                            '$push': {'old_usernames': {'date_updated': result['timestamp'], 'username': old_username}},
                            '$set': {'username': new_username}
                        }
                    )
                    print(f"{datetime.now()} - [MASTER] Username {old_username} updated to {new_username}")
                old_entity = self.db['groups'].find_one({'username': new_username})['full_entity']
                old_bots = {entity['id'] for entity in old_entity['users']}
                new_bots = {entity['id'] for entity in new_entity['users']}
                bot_diff = new_bots.difference(old_bots)
                for bot in new_entity['users']:
                    if bot['id'] in bot_diff:
                        self.db['groups'].update_one({'username': new_username}, {'$push': {'full_entity.users': bot}})

            elif result['code'] == "FAILURE":
                print(f"{datetime.now()} - [MASTER] Task failed for the username {result['username']}: {result['error_messages']}")
                # change state to failed
                self.db['groups'].update_one(
                    {'username': result['username']},
                    {
                        '$set': {'state': 'failed', 'last_update': result['timestamp']},
                        '$push': {'error_messages': {'timestamp': result['timestamp'], 'message': result['error_messages']}}
                    }
                )
                # log error messages
                file = open('log.txt', 'a')
                file.write(f"{datetime.now()} - {result['error_messages']}\n")
                file.close()
            
            self.busy[result['worker_id']] = False
            # return if get_result is blocking
            if block:
                return