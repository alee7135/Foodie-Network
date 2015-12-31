# import yelp_api
import json
import oauth2
import urllib
from urllib2 import urlopen
from bs4 import BeautifulSoup
import re
import time
import pprint
from pymongo import MongoClient
from pymongo import errors


class scraper(object):
    def __init__(self, collection, bus_id, bus_url, sleep=1.0):
    # Need a queue variable
    # Restaurant of Interest (ROI)
        self.collection = collection
        self.bus_id = bus_id
        self.bus_homepage = bus_url
        self.sleep = sleep
        self.user_ids = set()
        self.number_users = 0

### Functions to get all users (restaurant reviewers)
    def scrape_single_page(self, page):
        '''
        INPUT: None
        OUTPUT: updates all_reviewer_ids - list of people who reviewed the restaurant
        '''
        html = urlopen(page).read()
        soup = BeautifulSoup(html, "lxml")
        reviewers = soup.find_all('ul', attrs={'class':'ylist ylist-bordered reviews'})[0].find_all('a', attrs={'class':"user-display-name"})

        for reviewer in reviewers:
            self.user_ids.add(reviewer.get('href'))
            # print reviewer.get('href')
        # print self.user_ids

    def scrape_get_successivepagelinks(self):
        '''
        INPUT: None
        OUTPUT: list of restaurant pages to be scraped one by one
        '''
        html = urlopen(self.bus_homepage).read()
        soup = BeautifulSoup(html, "lxml")
        review_count = re.findall(r'\d+', soup.find("span", attrs={'class':'review-count rating-qualifier'}).text)
        review_count = int(review_count[0])
        num_pages = review_count / 20

        s = r'^.+%s' %(self.bus_id)
        prefix = re.findall(s, self.bus_homepage)[0]
        page_ids = []
        try:
            for page in xrange(num_pages + 1):  # need to include +1 extra page because python goes up to but not inclusive
                page_ids.append(prefix + '?start=' + str(page*20)) # only returns 20 results per page
        except Exception as e:
            print e
        return page_ids

    def scrape_reviewer_ids(self):
        '''
        INPUT: None
        OUTPUT: updates all_reviewer_ids - list of all reviewers in our database
        '''
        pages = self.scrape_get_successivepagelinks()
        time.sleep(self.sleep)
        for page in pages:
            self.scrape_single_page(page)
            time.sleep(self.sleep)

### Functions to begin scraping each user's profile
    def page_link_generator(self, user_id, homepage=False, page_number=0):
        '''
        INPUT: reviewer id, whether it is the first page of reviewer, current page to scrape
        OUTPUT: actual link to reviewer's page(s)
        '''
        # Can replace this later as a constant
        domain = 'http://www.yelp.com'
        # Pull out only user id
        userid = re.findall(r'userid=(.+$)', user_id)[0]
        # Create new link which pre-filters restaurants only
        if homepage == True:
            link = domain + '/user_details?userid=' + userid
        else:
            link = domain + '/user_details_reviews_self?userid=' + userid + '&review_filter=category&category_filter=restaurants&rec_pagestart=%s' %(str(page_number))
        return link

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

    def scrape_reviewer_reviews(self, user_id, page_number=None):
        '''
        INPUT: reviewer id
        OUTPUT: json object of all reviewer's reviews
            business_id, user_id, stars, text, date, rating (useful, funny, cool)
        '''
        # if we are scraping the homepage, we use that, otherwise we need to create a new soup for each next page
        link = self.page_link_generator(user_id, page_number=page_number*10)
        html = urlopen(link).read()
        soup = BeautifulSoup(html, "lxml")
        print 'LINK: ', link
        reviews = soup.find_all('ul', attrs={'class':'ytype ylist ylist-bordered reviews'})[0].find_all('li')

        for i in reviews:
            # business_id
            if i.find('a', attrs={'class':'biz-name'}):
                business_id = i.find('a', attrs={'class':'biz-name'})
                business_id = business_id.get('href')
                business_id = re.findall(r'^/biz/(.+$)', business_id)[0]
                # update list of businesses we have reviewed
                if not self.collection.find_one({'type':'add_business','id': business_id}):
                    self.collection.insert({'type':'add_business', 'id': business_id})
                    print 'ADDED BUSINESS %s' %(business_id)

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

            update_reviews = {
                    'type': 'review',
                    'user_id': user_id,
                    'business_id': business_id,
                    'rating': rating,
                    'text': text,
                    'date': date,
                    'votes': votes,
                    'check_in': check_in
                    }

            if i.find('a', attrs={'class':'biz-name'}):
                if not self.collection.find_one({'user_id': user_id, 'business_id': business_id}):
                    self.collection.insert(update_reviews)
                    print 'REVIEW for %s by %s INSERTED: ' %(business_id, user_id)
                else:
                    print 'REVIEW for %s by %s ALREADY IN DATABASE: ' %(business_id, user_id)

                print "+++++++++++++++++++++ NEXT REVIEW +++++++++++++++++++++++++"
                # break

    def scrape_reviewers(self, user_id, homepage):
        '''
        INPUT: reviewer id
        OUTPUT: ***
        '''
        # FIRST: Scrape reviewer stats which can only be found on homepage
        self.scrape_reviewer_stats(user_id, homepage)
        self.number_users += 1

        # Retrieve number of pages of RESTAURANT REVIEWS only
        try:
            link = self.page_link_generator(user_id)
            html = urlopen(link).read()
            review_page = BeautifulSoup(html, "lxml")
            pages = review_page.find('div', attrs={'class':'page-of-pages arrange_unit arrange_unit--fill'})
            pages = int(re.findall(r'([\d]+)\s+$', pages.text)[0])
        except:
            pages = 0
            # print 'reviewer only has one page'

        # SECOND: Iterate each user page and retrieve all restaurant reviews
        print 'PAGES: ', pages
        for page in xrange(pages):
            self.scrape_reviewer_reviews(user_id, page_number=page)
            time.sleep(self.sleep)
            # break
            print "-----------------------NEXT PAGE---------------------------"

    def run(self):

        # Get all users for restaurant
        print "Getting user ids for %s ..." %(self.bus_id)
        self.scrape_reviewer_ids()
        # Update information about users and reviews if necessary
        print 'Begin scraping users ...'
        for user_id in self.user_ids:
            if not self.collection.find_one({'$and':[{'type': 'user'}, {'user_id':user_id}]}):
                # Make request to user's homepage
                link = self.page_link_generator(user_id, homepage=True)
                html = urlopen(link).read()
                homepage = BeautifulSoup(html, "lxml")
                # Check if user has > 3 reviews, if not, we drop this reviewer
                profile = homepage.find('div', attrs={'class': 'user-profile_info arrange_unit'})
                review_count = re.findall(r'\d+', profile.find('li', attrs={'class':'review-count'}).text)[0]

                if int(review_count) > 3:
                    print "---------------------- NEW USER --------------------------"
                    print 'USER_ID: %s INSERTED' %user_id
                    self.scrape_reviewers(user_id, homepage)
                else:
                    print 'NOT ENOUGH REVIEWS: %s' %user_id
            else:
                print 'USER_ID: %s ALREADY IN DATABASE' %user_id
                # IF user already in database, no point in scraping their reviews again unless they've updated. Not implemented at this time.
            # break
        print 'TOTAL USERS: ', len(self.user_ids)
        print 'USERS WITH >3 REVIEWS INSERTED: ', self.number_users


def main():
    # Initiate MongoDB Database & Collection
    client = MongoClient()
    db = client['yelp']
    coll = db['businesses']

    for bus in coll.find({'type':'business'}):
        # if bus['id'] == 'afghan-oasis-berkeley':
            print "************************** NEW BUSINESS **********************"
            print 'BUSINESS ID: %s' %(bus['id'])
            scrape = scraper(collection = coll, bus_id = bus['id'], bus_url = bus['url'])
            scrape.run()
            break
            print '\n'


if __name__ == '__main__':
    main()
