import sys
sys.path.append("/Users/alee/Documents/Zipfian/FINAL/yelp/code")
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash, jsonify
from yelp_api import search
import pickle
# from flask.ext.pymongo import PyMongo

# Initiate MongoDB Database & Collection
app = Flask(__name__)
# mongo = PyMongo(app)

@app.route('/')
def home_page():
    # businesses = mongo.db.businesses.find({'type': 'business'}).count()
    return render_template('index.html')#,
        # businesses=businesses)

# Post request, once user submits a restaurant, it calls this view
@app.route('/request')
def yelp_search():
    # user_request = str(request.form['restaurant_id'])
    term = request.args.get('user_search_term')
    print term
    yelp_call = search(term=term, location='San Francisco')
    restaurants  = yelp_call['businesses']#[0]['id']
    name_id = [res['name'] for res in restaurants if res['review_count']>=500]
    img = [res['image_url'] for res in restaurants if res['review_count']>=500]
    url = [res['url'] for res in restaurants if res['review_count']>=500]
    rating = [res['rating'] for res in restaurants if res['review_count']>=500]
    lat = [res['location']['coordinate']['latitude'] for res in restaurants if res['review_count']>=500]
    log = [res['location']['coordinate']['longitude'] for res in restaurants if res['review_count']>=500]
    print name_id
    return jsonify(name_id=name_id, img=img, url=url, rating=rating, lat=lat, log=log)


# Get user data and plot on map
@app.route('/model')
def run_model():
    # Generate the list of restaurants in the community and return a list
    term = request.args.get('user_search_term')
    yelp_call = search(term=term, location='San Francisco')
    restaurants  = yelp_call['businesses']#[0]['id']
    name_id = [res['id'] for res in restaurants]
    business_ids = name_id

    # Generate basic restaurant statistics
    ratings = business_ratings[name_id[0]]
    votes = business_votes[name_id[0]]

    # Generate the network graph somehow...
    return jsonify(business_ids=business_ids, ratings=ratings, votes=votes)





if __name__ == '__main__':
    # Pickle the Yelp_Instance,
    # pickle.dump(user_edge_weight, )
    business_ratings = pickle.load(open( "../data/business_ratings.p", "rb"  ))
    business_votes = pickle.load(open( "../data/business_votes.p", "rb"  ))

    app.run(host='0.0.0.0', port=8081, debug=True)
