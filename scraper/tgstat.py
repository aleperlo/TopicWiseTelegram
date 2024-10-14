import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from time import sleep
import pymongo
import re


class TGStatScraper:
    def __init__(self, url, connection_string, max_requests=3, language_threshold=5, language="English", limit=100, connect=True, translation_set=None, dbname='GroupMonitoring_on_Telegram'):
        self.url = url
        self.headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        self.topic = {}
        self.groups = {}
        self.max_requests = max_requests
        self.language_threshold = language_threshold
        self.language = language
        self.limit = limit
        self.translation_set = {} if translation_set is None else translation_set
        # Connect to MongoDB
        try:
            print(f"{datetime.now()} - [TGStat]: Opening mongodb connection...")
            myclient = pymongo.MongoClient(connection_string)
            self.db = myclient[dbname]
        except Exception as e:
            print(f"{datetime.now()} - [!] Error in opening database connection: {e}")
            exit(-1)
        # Request topics
        if connect:
            self.response = self.request(url)
            self.soup = BeautifulSoup(self.response.content, 'html.parser')
            self.get_topic_list()


    def get_topic_list(self):
        buttons = self.soup.find_all('div', {'class':"dropdown-menu max-height-320px overflow-y-scroll"})[1]
        # Iterate over each button and extract name and href attributes
        for button in buttons.find_all('a', {'class':"dropdown-item"}):
            href = button['href']
            button_name= button.text.strip()
            self.topic[button_name] = href
            if self.db['topics'].find_one({'name': button_name}) is None:
                topic = {
                    'name': button_name,
                    'href': href,
                    'countries': {},
                    'languages': {},
                    'en_count': 0,
                    'joined_count': 0,
                    'groups_gathered_dates': []
                }
                self.db['topics'].insert_one(topic)


    def request(self, url):
        repeat = True
        counter = 0
        while repeat and counter < self.max_requests:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('title')
            if "429" not in title.contents[0]:
                repeat = False
            else:
                print(f"{datetime.now()} - [TGStat] Encountered 429 Error")
                sleep(5)
                counter += 1
        return None if repeat else response


    def __get_group_ranking(self, url, topic, public=True, sort='members'):
        url = url.split('?sort=members')[0]
        if public:
            url = url+'/public'
        elif public == True:
            url = url + '/private'
        url = url + '?sort=' + sort
        
        # Send a GET request to the URL with custom headers
        response = self.request(url)
        date_gathered = datetime.now(tz=timezone.utc)
        self.db['topics'].update_one({'name': topic}, {'$push': {'groups_gathered_dates': date_gathered}})
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all the chat elements
            chat_elements = soup.find_all('div', {'class': "card peer-item-row mb-2 ribbon-box border"})

            groups=[]
            for chat in chat_elements:
                chat_name = chat.find('div', class_="text-truncate font-16 text-dark mt-n1").text.strip()
                topic=chat.find('div', class_="text-truncate font-12 text-dark").text.strip()
                number_partecipants = chat.find('div', class_="text-truncate font-14 text-dark mt-n1").text.strip()
                number_messages=chat.find('div', {'class':"text-center",'data-html':"true", 'data-original-title':"Number of messages in the group in the last 7 days", 'data-placement':"top", 'data-toggle':"tooltip", 'data-trigger':"click", 'title':""}).text.strip().split('\n')[0].strip()
                number_active_users=chat.find('h4', {'class':"text-dark font-weight-normal mb-1 font-16 font-sm-18"}).text.strip()#, 'data-html':"true",'data-placement':"top", 'data-toggle':"tooltip", 'data-trigger':"click", 'title':""})


                # Find the <div> tag with the specified class
                div_tag = chat.find('div', class_='col col-12 col-sm-5 col-md-5 col-lg-4')
                # Find the <a> tag inside the <div>
                link_tag = div_tag.find('a')
                # Get the value of the 'href' attribute
                link = link_tag.get('href')
                # Define a regular expression pattern to match the username
                pattern = r'/@([a-zA-Z0-9_]+)'
                # Search for the username in the URL path using the regular expression
                username = re.search(pattern, link).group(1)

                group = {
                    "chat_name": chat_name,
                    "username": username,
                    "topic": topic,
                    "number_of_messages": int(float(number_messages.strip('k'))*1000) if 'k'in number_messages else ( int(float(number_messages.strip('m'))*1000000) if  'm' in number_messages else int(number_messages)),
                    "number_active_users": int(''.join(re.findall(r'\d+', number_active_users))),
                    "tg_link": link,
                    "country": "",
                    "language": "",
                    "date_gathered": date_gathered
                }
                groups.append(group)
                if self.db['seed'].find_one({'tg_link': link}) is None:
                    self.db['seed'].insert_one(group)
            return groups
        else:
            print(url,"Failed to retrieve page.")
            return  None

        
    def get_group_ranking(self, topic, public=True, sort='members'):
        print(f"{datetime.now()} - [TGStat] Getting ranking for topic '{topic}'")
        groups = self.__get_group_ranking('https://tgstat.com/'+self.topic[topic], topic, public=public, sort=sort)
        limit = self.limit if self.limit < len(groups) else len(groups)
        self.groups[topic] = groups
    

    def enrich_topic_with_language(self, topic):
        print(f"{datetime.now()} - [TGStat] Getting language and country for topic '{topic}'")
        country_dict = {}
        language_dict = {}
        for i, group in enumerate(self.groups[topic]):
            url = group['tg_link']
            check = self.db['seed'].find_one({'tg_link': url, 'country': {'$ne': ""}, 'language': {'$ne': ""}})
            if check is not None:
                continue
            try:
                response = self.request(url)
                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the HTML content of the page
                    soup = BeautifulSoup(response.content, 'html.parser')
                    country, language = soup.find('div', {'class': "mt-4"}).text.split('\n')[-2:]
                    country = country[:-1].strip().replace(' ', '_')
                    language = language.strip().replace(' ', '_')
                    if language in self.translation_set:
                        language = self.language

                    self.groups[topic][i]['country'] = country
                    self.groups[topic][i]['language'] = language
                    self.db['seed'].update_one({'tg_link': url}, {'$set': {'country': country, 'language': language}})
                    if language == self.language:
                        self.db['topics'].update_one({'name': topic}, {'$inc': {'en_count': 1}})
                    if language in language_dict:
                        language_dict[language] += 1
                    else:
                        language_dict[language] = 1
                    if country in country_dict:
                        country_dict[country] += 1
                    else:
                        country_dict[country] = 1
            except Exception as e:
                print(url, e)
        topic_analytics = self.db['topics'].find_one({'name': topic})
        for country in country_dict:
            if country in topic_analytics['countries']:
                command = '$inc'
            else:
                command = '$set'
            self.db['topics'].update_one({'name': topic}, {command: {f'countries.{country}': country_dict[country]}})
        for language in language_dict:
            if language in topic_analytics['languages']:
                command = '$inc'
            else:
                command = '$set'
            self.db['topics'].update_one({'name': topic}, {command: {f'languages.{language}': language_dict[language]}})


    def send_to_processing(self, topic):
        print(f"{datetime.now()} - [TGStat] Groups were found for topic '{topic}', sent to processing")
        cursor = self.db['seed'].find({'topic': topic})
        for entry in cursor:
            entry['last_update'] = datetime.now(tz=timezone.utc)
            entry['messages'] = []
            entry.pop('_id', None)
            check_groups = self.db['groups'].find_one({'username': entry['username']})
            if check_groups is None:
                print(f"{datetime.now()} - [TGStat] Group {entry['username']} sent to processing")
                self.db['tbp'].insert_one(entry)
            else:
                print(f"{datetime.now()} - [TGStat] Group {entry['username']} was not inserted in tpb since it already is in groups")