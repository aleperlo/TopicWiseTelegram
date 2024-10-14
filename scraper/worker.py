import asyncio
from datetime import datetime, timedelta, timezone
import random
import pymongo
import telethon
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from util import *


class MonitoringWorker:
    def __init__(self, api_id, api_hash, connection_string, messages_limit=None, messages_limit_days=30, starting_date=None, dbname='GroupMonitoring_on_Telegram'):
        self.api_id = api_id
        self.api_hash = api_hash
        # set limits for message scraping
        self.messages_limit = messages_limit
        self.messages_limit_days = messages_limit_days
        self.starting_date = starting_date
        # set lower bound and upper bound of the wait interval before joining a channel
        self.join_wait_lb = 55
        self.join_wait_ub = 65
        # set lower bound and upper bound of the wait interval before leaving a channel
        self.leave_wait_lb = 0
        self.leave_wait_ub = 5
        # MongoDB connection string
        self.connection_string = connection_string
        self.dbname = dbname


    def bind(self, pid, tasks, result_queue, process_queue):
        # set id and bind queues to the slave
        self.pid = pid
        self.task_queue = tasks
        self.result_queue = result_queue
        self.process_queue = process_queue
        # initialize TelegramClient object with the given API id and hash
        self.client = telethon.TelegramClient(
            f"session_{str(self.pid)}", self.api_id, self.api_hash)


    def launch_client(self):
        self.db = pymongo.MongoClient(self.connection_string)[self.dbname]
        with self.client:
            self.client.loop.run_until_complete(self.__work())
            self.client.disconnect()
    

    async def __work(self):
        me = await self.client.get_me()
        print(f"{datetime.now()} - [WORKER n.{self.pid}]: Worker {me.username} launched")
        await self.__crawl_worker()
    

    def get_offset_date(self):
        if self.starting_date is None:
            offset_date = datetime.now(tz=timezone.utc) - timedelta(self.messages_limit_days)
        else:
            offset_date = self.starting_date
        return offset_date


    async def join_public_group(self, username, result):
        """Join the group with the given ```username```."""
        print(f"{datetime.now()} - [WORKER n.{self.pid}] Joining public group '{result['username']}'")
        try:
            # join the group with the given username
            full_entity = await self.client(GetFullChannelRequest(username))
            if full_entity.full_chat.ttl_period is not None:
                result['code'] = "FAILURE"
                result['error_messages'] = "Group has a TTL period"
                print(f"{datetime.now()} - [WORKER n.{self.pid}] {result['error_messages']}")
                return None
            await self.client(JoinChannelRequest(username))
            print(f"{datetime.now()} - [WORKER n.{self.pid}] Joined '{username}'")
        except telethon.errors.FloodWaitError as e:
            wait = wait_time(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] Flood Error: Waiting for {wait} seconds. ({e})")
            await asyncio.sleep(wait + 10)
            await self.join_public_group(username, result)
        except telethon.errors.InviteRequestSentError as e:
            result['code'] = "REQUEST_SENT"
            result['id'] = full_entity.full_chat.id
            result['full_entity'] = full_entity.to_dict()
            result['error_messages'] = str(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] {e}")
            return None
        # except telethon.errors.UserAlreadyParticipantError:
        #     print(f"{datetime.now()} - [WORKER n.{self.pid}] Joined '{username}', user already participant")
        except Exception as e:
            result['code'] = "FAILURE"
            result['error_messages'] = str(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] {e}")
            return None
        return full_entity
    

    async def check_dialog(self, entity_id, result):
        async for dialog in self.client.iter_dialogs():
            if dialog.entity.id == entity_id:
                result['code'] = "JOIN_SUCCESS"
                return entity_id
        # No entity with matching id has been found, you are still waiting
        result['code'] = "REQUEST_SENT"
        result['error_messages'] = "Your request has not been approved yet"
        print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] {result['error_messages']}")
        return None


    async def check_username(self, entity_id, result):
        try:
            # entity = await self.client.get_entity(entity_id)
            entity = await self.client(GetFullChannelRequest(entity_id))
            new_username = entity.chats[0].username
        except Exception as e:
            result['code'] = "FAILURE"
            result['error_messages'] = str(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] {e}")
            return None
        return entity.to_dict()


    async def collect_messages(self, entity_id, result, offset_date):
        """Collect at most ```self.limit``` messages starting from ```offset_date``` from entity with id ```entity_id```."""
        first = True
        found = False
        # store offset_date to start once again from offset_date in case of FloodWaitError
        last_date = offset_date
        print(f"{datetime.now()} - [WORKER n.{self.pid}] Collecting messages from '{result['username']}' from date {offset_date}")
        try:
            async for dialog in self.client.iter_dialogs():
                if dialog.entity.id == entity_id:
                    found = True
                    async for m in self.client.iter_messages(dialog.id, limit=self.messages_limit, reverse=True, offset_date=offset_date, wait_time=60):
                        if first:
                            # store date of the first message
                            first = False
                            result['first_message'] = m.date
                        # update db and result dictionary
                        self.db[f'messages_{entity_id}'].insert_one(m.to_dict())
                        result['messages'] += 1
                        # update last_date to restart in case of FloodWaitError
                        last_date = m.date
        except telethon.errors.FloodWaitError as e:
            wait = wait_time(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] Flood Error: Waiting for {wait} seconds. ({e})")
            await asyncio.sleep(wait + 10)
            result['messages'] = []
            await self.collect_messages(entity_id, result, last_date)
        except Exception as e:
            result['code'] = "FAILURE"
            result['error_messages'] = str(e)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] [!] {e}")
        if not found:
            result['code'] = "FAILURE"
            result['error_messages'] = "Group not found"


    async def __crawl_worker(self):
        print(f"{datetime.now()} - [WORKER n.{self.pid}] Crawling...")
        while True:
            # wait for a task to be dispatched to the worker
            task = self.task_queue.get()
            lb = self.join_wait_lb
            ub = self.join_wait_ub

            # TRY_JOIN: join the group and collect messages since:
            # - starting_date, if starting date is specified
            # - datetime.now() - timedelta(limit_days) otherwise
            if task['name'] == "TRY_JOIN":
                username = task['data']['username']
                result = {
                    'code': "JOIN_SUCCESS",
                    'username': username,
                    'id': "",
                    'messages': 0,
                    'timestamp': None,
                    'error_messages': "",
                    'first_message': None,
                    'worker_id': self.pid
                }
                full_entity = await self.join_public_group(username, result)
                if full_entity is not None:
                    result['id'] = full_entity.full_chat.id
                    result['full_entity'] = full_entity.to_dict()
                    # set offset_date according to the given parametres
                    offset_date = self.get_offset_date()
                    await self.collect_messages(result['id'], result, offset_date)

            # CHECK_UPDATES: collect messages since the given offset_date
            elif task['name'] == "CHECK_UPDATES":
                data = task['data']
                result = {
                    'code': "UPDATE_SUCCESS",
                    'username': data['username'],
                    'id': data['id'],
                    'messages': 0,
                    'timestamp': None,
                    'error_messages': "",
                    'first_message': None,
                    'worker_id': self.pid
                }
                await self.collect_messages(data['id'], result, data['offset_date'])
            
            # CHECK_WAIT: check a group you are waiting to be accepted in
            elif task['name'] == "CHECK_WAIT":
                data = task['data']
                result = {
                    'code': "JOIN_SUCCESS",
                    'username': data['username'],
                    'id': data['id'],
                    'messages': 0,
                    'timestamp': None,
                    'error_messages': "",
                    'first_message': None,
                    'worker_id': self.pid
                }
                entity_id = await self.check_dialog(data['id'], result)
                if entity_id is not None:
                    offset_date = self.get_offset_date()
                    await self.collect_messages(entity_id, result, offset_date)
            
            elif task['name'] == "CHECK_USERNAME":
                lb = self.leave_wait_lb
                ub = self.leave_wait_ub
                data = task['data']
                result = {
                    'code': "ENTITY_FOUND",
                    'username': data['username'],
                    'id': data['id'],
                    'messages': 0,
                    'timestamp': None,
                    'error_messages': "",
                    'first_message': None,
                    'new_username': '',
                    'worker_id': self.pid
                }
                new_entity = await self.check_username(data['id'], result)
                if new_entity is not None:
                    result['new_entity'] = new_entity
                
            # put result in result_queue
            result['timestamp'] = datetime.now(tz=timezone.utc)
            self.result_queue.put(result)
            # wait for join_wait_lb < n < join_wait_ub sec before joining a group
            wait_time = random.randint(lb, ub)
            print(f"{datetime.now()} - [WORKER n.{self.pid}] waiting {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            # put pid back in process_queue
            print(f"{datetime.now()} - [WORKER n.{self.pid}]: Finished waiting")
            self.process_queue.put(self.pid)