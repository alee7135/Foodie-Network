import sys
import json
import oauth2
import requests
import urllib
from bs4 import BeautifulSoup
import re
import time
import pprint
from pymongo import MongoClient
from pymongo import errors
import numpy as np
import logging
from subprocess import call
from export_s3 import export_s3

class scraper(object):
    FLAG = 0

    def __init__(self, collection, bus_id, bus_url):
    # Need a queue variable
    # Restaurant of Interest (ROI)
        self.collection = collection
        self.bus_id = bus_id
        self.bus_homepage = bus_url
        # self.sleep = sleep
        # self.user_ids = set()
        self.number_users = 0

    def try_request(self, url):
        '''
        catch url request errors and re-try 3 times with 5 second pause
        '''
        counter = 0

        header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.22 Safari/537.36'  # noqa
        }
        # proxymesh_keys = json.load(open("proxy_mesh_keys.json"), encoding='UTF-8')

        # username = str(proxymesh_keys['username'])
        # password = str(proxymesh_keys['password'])
        # auth = requests.auth.HTTPProxyAuth(username, password)
        # proxies = {'http': 'http://us-ca.proxymesh.com:31280'}

        try:
            time.sleep(np.random.randint(1,10))
            r = requests.get(str(url), headers=header)
            if r.status_code == 200:
                return r.text
            else:
                while counter < 3:
                    counter += 1
                    time.sleep(np.random.randint(1,10))
                    r = requests.get(str(url), headers=header)
                    if r.status_code == 200:
                        return r.text
                logging.warning('Could not access link %s' % url)
                FLAG = 1
                return ""
        except:
            logging.warning('Could not access link %s' % url)
            FLAG = 1
            return ""

    def scrape_single_page(self, page):
        '''
        INPUT: None
        OUTPUT: json object of all reviewer's reviews
            business_id, user_id, stars, text, date, rating (useful, funny, cool)
        '''
        html = self.try_request(page)
        soup = BeautifulSoup(html, "html.parser")

        reviews = soup.find_all('ul', attrs={'class':'ylist ylist-bordered reviews'})[0].find_all('li')

        count = 0
        for i in reviews:
            try:
                if i.find_all('ul', attrs={'user-passport-stats'}):
                    # only if review of review has > 3 reviews, we continue
                    if int(i.find_all('b')[-1].text) > 3:
                        # User id
                        if i.find('li', attrs={'class':"user-name"}):
                            # reviewer = i.find('li', attrs={'class':"user-name"})
                            reviewer = i.find('a', attrs={'class':"user-display-name"})
                            user_id = reviewer.get('href')
                            # insert reviewer data
                            self.add_reviewer_stats(user_id)
                            # self.user_ids.add(user_id)

                        # stars review
                        if i.find('div', attrs={'class':"rating-very-large"}):
                            stars = i.find('div', attrs={'class':"rating-very-large"})
                            stars = stars.find('i').get('class')[1]
                            rating = int(re.findall(r'\d+$', stars)[0])

                        # texts
                        if i.find('p'):
                            text = i.find('p').text

                        # date
                        if i.find('span', attrs={'class', 'rating-qualifier'}):
                            date = i.find('span', attrs={'class', 'rating-qualifier'}).text
                            date = re.findall(r"(\d+\/\d+\/\d+)", date)[0]

                        # votes
                        try:
                            votes = {'useful':0, 'funny':0, 'cool':0}
                            if i.find_all('li', attrs={'class':'vote-item inline-block'}):
                                emotion = ['useful', 'funny', 'cool']
                                for vot in xrange(3): # useful, funny, cool
                                    vote = i.find_all('li', attrs={'class':'vote-item inline-block'})[vot]
                                    vote = vote.find('span', attrs={'class':'count'}).text
                                    if not vote:
                                        votes[emotion[vot]] = 0
                                    else:
                                        votes[emotion[vot]] = int(vote)
                        except:
                            votes = {'useful':0, 'funny':0, 'cool':0}

                        # Check-In
                        check_in = 0  # need to re-initiate check-in because not all reviewers checked in for each restaurant
                        if i.find('div', attrs={'class':'review_tags'}):
                            check_in = i.find('div', attrs={'class':'review_tags'})
                            check_in = check_in.text
                            # print 'CHECK_IN', check_in
                            if re.findall(r'check-in', check_in):
                                check_in = int(re.findall(r'\d+', check_in)[0])
                            else:
                                check_in = 0

                        if i.find('li', attrs={'class':"user-name"}):
                            update_reviews = {
                                    'type': 'review',
                                    'user_id': user_id,
                                    'business_id': self.bus_id,
                                    'rating': rating,
                                    'text': text,
                                    'date': date,
                                    'votes': votes,
                                    'check_in': check_in
                                    }

                            if not self.collection.find_one({'user_id': user_id, 'business_id': self.bus_id}):
                                self.collection.insert(update_reviews)
                                print 'REVIEW for %s by %s INSERTED: ' %(self.bus_id, user_id)
                            else:
                                print 'REVIEW for %s by %s ALREADY IN DATABASE: ' %(self.bus_id, user_id)

                            count += 1

                            print "+++++++++++++++++++++ NEXT REVIEW +++++++++++++++++++++++++"

            except Exception as e:
                logging.warning('Error %s for review #(%d) for business_id: %s' %(str(e), count, str(self.bus_id)) )
                # continue
                break

    def scrape_get_successivepagelinks(self):
        '''
        INPUT: None
        OUTPUT: list of restaurant pages to be scraped one by one
        '''
        # s = r'^.+%s' %(self.bus_id)
        # prefix = re.findall(s, self.bus_homepage)[0]
        s = "http://www.yelp.com/biz/" + self.bus_id.encode('utf-8') + '?sort_by=elites_desc'
        s0 = "http://www.yelp.com/biz/" + self.bus_id.encode('utf-8')
        prefix = str(s)
        prefix0 = str(s0)

        html = self.try_request(prefix)
        soup = BeautifulSoup(html, "html.parser")
        rc = soup.find('div',attrs={'pagination-block'}).text
        # print rc
        rc = int(re.findall(r'of (\d+)', rc)[0])
        print rc
        # review_count = re.findall(r'\d+', soup.find("span", attrs={'class':'review-count rating-qualifier'}).text)
        # num_pages = rc / 20

        page_ids = []

        for page in xrange(rc + 1):  # need to include +1 extra page because python goes up to but not inclusive
            if page == 0:
                page_ids.append(prefix)
            else:
                page_ids.append(prefix0 + '?start=' + str(page*20) + '&sort_by=elites_desc') # only returns 20 results per page

        return page_ids

    def scrape_all_reviews(self):
        '''
        INPUT: None
        OUTPUT: updates all_reviewer_ids - list of all reviewers in our database
        '''
        pages = self.scrape_get_successivepagelinks()
        # time.sleep(np.random.randint(1,10))
        count = 1
        for page in pages:
            print page
            try:
                self.scrape_single_page(page)
            except Exception as e:
                logging.warning('Error %s for scrape_single_page for business_id: %s on Page: %d' %(str(e), str(self.bus_id), count) )
                break
            # time.sleep(np.random.randint(1,10))

            print "--------------------- NEXT PAGE (%d) -------------------------" %(count)
            count += 1
            # break

    def page_link_generator(self, user_id):
        '''
        INPUT: reviewer id
        OUTPUT: actual link to reviewer's page(s)
        '''
        # Can replace this later as a constant
        domain = 'http://www.yelp.com'
        # Pull out only user id
        userid = re.findall(r'userid=(.+$)', user_id)[0]
        link = domain + '/user_details?userid=' + userid
        return link

    def add_reviewer_stats(self, user_id):

        if not self.collection.find_one({'$and':[{'type': 'user'}, {'user_id':user_id}]}):
            # Make request to user's homepage
            # time.sleep(np.random.randint(1,10))
            link = self.page_link_generator(user_id)
            html = self.try_request(link)
            if len(html) > 0:
                homepage = BeautifulSoup(html, "html.parser")
                # # Check if user has > 3 reviews, if not, we drop this reviewer
                # profile = homepage.find('div', attrs={'class': 'user-profile_info arrange_unit'})
                # review_count = re.findall(r'\d+', profile.find('li', attrs={'class':'review-count'}).text)[0]

                # if int(review_count) > 3:
                try:
                    self.scrape_reviewer_stats(user_id, homepage)
                    self.number_users += 1
                    print 'USER_ID: %s INSERTED' %user_id
                except Exception as e:
                    logging.warning('Error %s for scrape_reviewer_stats for user_id: %s' %(str(e), str(user_id)) )
                    # If there was an error with this user, we skip to next user
                # else:
                #     print 'NOT ENOUGH REVIEWS: %s' %user_id
        else:
            print 'USER_ID: %s ALREADY IN DATABASE' %user_id
            # IF user already in database, no point in scraping their reviews again unless they've updated. Not implemented at this time.

        # break

    def scrape_reviewer_stats(self, user_id, homepage):
        '''
        INPUT: reviewer id and soup for reviewer's homepage
        OUTPUT: updates all user information
        '''
        ## Need to be on USER MAIN PROFILE OVERVIEW
        # Basic user stats
        profile = homepage.find('div', attrs={'class': 'user-profile_info arrange_unit'})
        name = profile.find('h1').text
        hometown = profile.find('h3').text

        friend_count = re.findall(r'\d+', profile.find('li', attrs={'class':'friend-count'}).text)[0]
        review_count = re.findall(r'\d+', profile.find('li', attrs={'class':'review-count'}).text)[0]
        photo_count = re.findall(r'\d+', profile.find('li', attrs={'class':'photo-count'}).text)[0]
        if profile.find('span', attrs={'class':'elite-badge'}):
            elite_badge = 1
        else:
            elite_badge = 0

        # Histogram of ratings and votes
        sidebar = homepage.find('div', attrs={'class':'user-details-overview_sidebar'})

        try:
            stars = ['5', '4', '3', '2', '1']
            ratings = {'5':0, '4':0, '3':0, '2':0, '1':0}
            for i,j in enumerate(sidebar.find_all('td', attrs={'class':'histogram_count'})):
                ratings[stars[i]] = (int(j.text))
        except:
            ratings = {'5':0, '4':0, '3':0, '2':0, '1':0}

        try:
            emotion = ['useful', 'funny', 'cool']
            votes = {'useful':0, 'funny':0, 'cool':0}
            for i,j in enumerate(sidebar.find('ul', attrs={'class':'ylist ylist--condensed'}).find_all('li')):
                votes[emotion[i]] = int(re.findall(r'\d+', j.text)[0])
        except:
            votes = {'useful':0, 'funny':0, 'cool':0}

        # Acount_begin date
        try:
            account_date = sidebar.find_all('div', attrs={'class':'ysection'})[-1]
            account_date = account_date.find_all('li')[-1]
            account_date = account_date.find('p').text
        except:
            account_date = None

        update_users = {
                'type': 'user',
                'user_id': user_id,
                'name': name,
                'hometown': hometown,
                'friend_count': friend_count,
                'review_count': review_count,
                'photo_count': photo_count,
                'elite_badge': elite_badge,
                'ratings': ratings,
                'votes': votes,
                'account_date': account_date
        }

        self.collection.insert(update_users)

    def run(self):

        # Get all users for restaurant
        print "Begin scraping all business reviews: %s ..." %(self.bus_id)

        self.scrape_all_reviews()

        print 'USERS WITH >3 REVIEWS INSERTED: ', self.number_users


def main():
    # Initiate MongoDB Database & Collection
    client = MongoClient()
    db = client['yelp']
    coll = db['businesses']

    business_list = list(coll.find({'type':'business', 'review_count': {'$gte': 500 }}))
    print "Scraping %d businesses" %(len(business_list))
    start = int(sys.argv[1])
    end = int(sys.argv[2])

    # Initiate error log
    name = "../logs/log_%d_%d.txt" % (start, end)
    logging.basicConfig(filename=name, level=logging.WARNING)
    count = start
    # Extract only interested restaurant indexes
    sublst = business_list[start:end]
    # print business_list
    for bus in sublst:
        # if bus['id'] == 'old-jerusalem-restaurant-san-francisco':
            start_time = time.time()
            logging.warning('business: %d, %s' % (count, bus['id']))
            print "************************** NEW BUSINESS #(%d) **********************" %count
            print 'BUSINESS ID: %s' %(bus['id'])
            # if FLAG is up, service is down, we need to stop
            if scraper.FLAG == 1:
                print "FLAG RAISED %s, COUNT: %d" %(bus['id'], count)
                break
            scrape = scraper(collection = coll, bus_id = bus['id'], bus_url = bus['url'])
            scrape.run()
            end_time = time.time()
            logging.warning("took %d seconds" % (end-start))

            count += 1
            # break
            print '\n'
    logging.warning("DONE")

    # Export results as JSON to S3
    file_name = "results_%d_%d" %(start, end)
    export_s3(file_name)

if __name__ == '__main__':
    main()
