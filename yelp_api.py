import json
import oauth2
import urllib
import urllib2
import pprint

'''
The purpose of this function is to be the first interaction with the user.
The user inputs a restaurant search term along with a location they are intersted.
This function returns the business url which will be passed into
yelp_scraper.py function
'''

yelp_keys = json.load(open("/Users/alee/Downloads/yelp_key.json"), encoding='UTF-8')

API_HOST = 'api.yelp.com'
SEARCH_LIMIT = 20
KEY = yelp_keys['Consumer Key']
SECRET_KEY = yelp_keys['Consumer Secret']
TOKEN = yelp_keys['Token']
SECRET_TOKEN = yelp_keys['Token Secret']


def request(path, url_params=None):
    """Prepares OAuth authentication and sends the request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        urllib2.HTTPError: An error occurs from the HTTP request.
    """
    # create endpoint url
    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(API_HOST,urllib.quote(path.encode('utf8')))
    # create consumer access
    consumer = oauth2.Consumer(KEY, SECRET_KEY)
    # GET Request
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)
    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': KEY
        }
    )
    token = oauth2.Token(TOKEN, SECRET_TOKEN)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()

    print u'Querying {0} ...'.format(signed_url)

    conn = urllib2.urlopen(signed_url, None)
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()
    return response

def search(term, location):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """
    SEARCH_PATH = '/v2/search/'
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'offset': 0,
        'limit': SEARCH_LIMIT
    }
    return request(SEARCH_PATH, url_params=url_params)

def get_business(business_id):
    """Query the Business API by a business ID.
    Args:
        business_id (str): The ID of the business to query.
    Returns:
        dict: The JSON response from the request.
    """
    BUSINESS_PATH = '/v2/business/'
    business_path = BUSINESS_PATH + business_id
    print business_path
    return request(business_path)

def query_restaurant_url(term, location, print_info=False):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    # first create a search
    response = search(term, location)
    # check if it actually is a business, if not we dont care
    businesses = response.get('businesses')

    if not businesses:
        print u'No businesses for {0} in {1} found.'.format(term, location)
        return
    # talk only the first business id
    business_id = businesses[0]['id']

    print u'{0} businesses found, querying business info '         'for the top result "{1}" ...'.format(
            len(businesses), business_id)
    response = get_business(business_id)

    print u'Result for business "{0}" found:'.format(business_id)
    if print_info == True:
        pprint.pprint(response, indent=2)
    return response['url']
