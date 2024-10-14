from tgstat import TGStatScraper
from master import MonitoringMaster
from worker import MonitoringWorker
from datetime import datetime

# read data from data.txt
data = open('data.txt', 'r').readlines()
connection_string = data[0].rstrip()

topics = ['Politics', 'Cryptocurrencies', 'Technologies', 'Economics', 'Darknet', 'Education', 'Linguistics', 'Courses and guides', 'Software & Applications', 'Video and films', 'Bookmaking', 'Erotic']

# Initialize worker and master
starting_date = datetime(year=2024, month=3, day=1)
workers = []
for i in range(1, len(data), 2):
    api_id = data[i].rstrip()
    api_hash = data[i+1].rstrip()
    worker = MonitoringWorker(api_id, api_hash, connection_string, starting_date=starting_date)
    workers.append(worker)
master = MonitoringMaster(workers, connection_string)
master.run_workers()

# Initalize scraper for TGStat
translations = {'Английский', 'Ingliz', 'Inglizcha'}
tg_ranking = TGStatScraper('https://tgstat.com/ratings/chats', connection_string, translation_set=translations)

# During the loop, ask for groups and join them
for topic in topics:
    tg_ranking.get_group_ranking(topic, sort='mau')
    # tg_ranking.enrich_topic_with_language(topic)
    tg_ranking.send_to_processing(topic)
    master.crawl('join')

# Check the groups every 12 hours (default value)
while True:
    master.crawl('check')
    for topic in topics:
        master.update_usernames(topic)
        tg_ranking.get_group_ranking(topic, sort='mau')
        # tg_ranking.enrich_topic_with_language(topic)
        tg_ranking.send_to_processing(topic)
        master.crawl('join')