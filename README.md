# FOODIE NETWORK
### Your Guide to More Authentic Yelp Reviews 
![alt tag](https://github.com/alee7135/yelp/blob/master/imgs/foodie_network.png)

##What does Authenticity Mean to you?
Thousands of restaurant reviews on Yelp but unfortunately users have little direct information about which reviews are legitimate? Although Yelp has an automated filtering algorithm which filters out reviews based on the reputation of each reviewer, it still doesn’t necessarily distinguish between reviews which are more authentic than others. I define authenticity as a user (reviewer) who reviews a single cuisine type with high frequency within its immediate network (I'll elaborate more on this). 

Theoretically, every user on Yelp has a different level of qualification to rate a certain restaurant based on the type of cuisine they are most familiar with. This level of qualification depends on their review history and can be empirically estimated by the network of restaurants that they most commonly frequent. A user who frequently visits Chinese restaurants will be more qualified to to rate a Chinese restaurant than an Ethiopian restaurant. It is important to realize that this is a big assumption that I am making for my model and while it is not always true (new users could have just as much knowledge about on cuisine as the most elite reviewers on Yelp), my assumption is that this phenomenon holds true in large and especially when dealing with large social networks such as Yelp. My app, Foodie Network aims to enlighten app users about different restaurants before they dine. 

##Data Source 
Yelp business data is available through the Yelp API. I downloaded ~ 7,000 restaurants in San Francisco. However, unfortunately the API does not provide user profile data or review data. For this, I scraped over 100,000 reviews and over 15,000 user profile data. Since scraping for all 7,000+ restaurants would have taken more time than I had especially after dealing with multiple bans from Yelp, I decided to filter my dataset to only include restaurants with greater than 500 reviews and users with a elite badge. The scraping was an involved process which included iterating each restaurant, page, user, and review each user did. I deployed 10 EC2 instances by sending a custom bash script to each machine to install the necessary updates and packages and to run my python script to scrape Yelp. I wrote another script to send data from each instance to Amazon S3 for storage. I scraped reviews for the user id, business id, rating, votes, review text, date and whether it was check-in. I scraped user profiles for the user id, friend count, votes, photo count, name, elite, rating summary, and hometown. 

##Repo Structure
```
├── code
|   ├── yelp_api.py (module to access/connect to the Yelp API and make requests)
|   ├── get_restaurants.py (module to connect to Yelp API and retrieve 7000+ restaurants in San Francisco)
|   ├── yelp_scraper.py (module which actually does the scraping of all users and reviews)
|   ├── package_install.sh (shell script to install necessary packages on Amazon EC2 instances)
|   ├── export_s3.py (function to export all results to an S3 bucket)
|   ├── get_s3_data.py (function to import all results and consolidate results into single json file)
|   ├── make_edges.py (functions to take single json file and prepare network edges for input into network module)
|   ├── network_igraph.py (main module which builds the model and runs community detection algorithms)
|   ├── yelp_statistics.py (functions calculate some simple descriptive statistics)
|   └──
|
├── web_app
|   ├── static (images, CSS and JavaScript files)
|   ├── templates (webpage templates)
|   └── flaskapp.py (the Flask application - run this file to launch the app)
|
├── presentations
|   ├── Hiring Day Draft presentation
|   └── Hiring Day Final presentation
|
├── imgs
|   └── images for readme.md
```

##Data Pipeline

![alt tag](https://github.com/alee7135/yelp/blob/master/imgs/pipeline.jpg)

##Network Graph Building
I refer to reviewers as users and restaurant as businesses. I began with the easiest Python package, NetworkX. I inserted vertices for businesses and users and edges for reviews. I began modeling the network with the simplest network configuration which hold only user-business connections. With just over a 100,000 edges, I spend a significant time exploring different network packages in Python. After writing a custom algorithm which would find communities and compute modularity using NetworkX, I realized it was too slow. 
I switched to GraphTools which was significantly faster because it is fully implemented in C language. However, their algorithm for community detection requires the user to pre-specify an iteration count and community count. I used Scipy’s minimize optimization function to try to find the optimal parameters for my dataset using different algorithms. I found that COBYLA worked the most efficiently but I was not convinced it was the right algorithm as I plotted the number of communities against the average modularity. I tested the other algorithm offered in GraphTools, including block partition model but it provided similar results. 
I made a decision to go with the third and final package, iGraph because it had better community detection algorithms despite being slightly slower than GraphTools. I revised my network script to accommodate the changes and rebuilt the model. While I did most of my local testing locally, I used a single EC2 instance with 120 GB of RAM and 300 GB of memory to run my models. After I was success in completing one model run, I tested other features such as adding weights between user-business with ratings. This did not have a significant effect. I then tried larger models such as including user-user and cuisine-cuisine connections but unfortunately the network quickly became intractable with over 10 million network edges. Even after 2 days, the network had not even finished building, let alone do any computations, I decided to I abort mission. Going forward, all results are computed using only the user-business model. 

Using the network, I tried several algorithms but I settled with the fast-greedy community algorithm in iGraph because it was designed for large networks and produced sensible results. Of the 100,000+ edges and 18,000+ vertices inside my network, the model found roughly 10 communities based on optimizing modularity. Networks with high modularity have denser connections between the nodes with communities but sparse connections between nodes in different communities. Not surprisingly, communities are rather unbalanced with the largest community have ~4,000 nodes and the smallest having just ~34 nodes. This lack of balanced community is a bad fact of a poor network which is not surprising considering I am only using user-business connections when I should actually be using user-user connections. The makeup of the communities yield some interesting results. If I look at the distribution of cuisine types within communities, I see that they are unified by similar tastes. For example, asian food makes up one community while italian and pizza another and mediterranean another and american another, etc. 

##Model Evaluation
Considering the difficulty in validating unsupervised clustering-type algorithms, this was no different. Initially, I figured that I could establish a baseline using average restaurant ratings. My hypothesis is that communities of users will rate similarly when compared to just the average of all users. First, I compute the mean-square-error between the average rating and actual rating for each review. Second, I compute the mean-square-error for average community ratings and actual ratings. Again, my hypothesis was that communities of users tend to rate more similarly than just the average user which means the MSE for communities should be lower. Turns out, the average still does better. This indicates that communities of a restaurant are not always heterogeneous because just because two people are in same community, it doesn’t mean they will rate the same. This was quite a powerful insight because it tells me that I can not predict a user’s rating simply based on his similarity to other users. That is, communities of users may eat the same food but they could have vastly different opinions about the same food. Without this evaluation metric and no time to perform a proper A/B test, I decided to simply use human evaluation of the results. 

## Compute Authenticity Reweighting
Now that I have my network of reviews separated into communities, I can leverage the community structure to reweight reviews based on the authenticity of each user. Suppose I was interested in the authenticity of Chinese cuisine for restaurant X. For each user, I compute a weighted average of proportion of Chinese restaurants that the user has rated for each type of review: 1) User belongs to the same community as Restaurant X and he/she has reviewed restaurant within Restaurant X's community, give highest weight of say 3; 2) User belongs to the same community as Restaurant X and he/she reviewed restaurant outside of the community of Restaurant X, give medium weight of 2; 3) User belongs to a different community from Restaurant X and has either reviewed a restaurant within or outside of Restaurant X's community, give lowest weight of 1. The value of the weight is arbitrary as long as it is constant because I will normalize by dividing by the sum of the weights for each user. Now, this gives a weighted average weight for each review. I ultimately weight value for each user to recompute a new Yelp rating for Restaurant X, taking into account authenticity. 



##Python Packages Used
•	NumPy
•	pandas
•	SciPy
•	scikit-learn
•	urllib
•	BeautifulSoup
•	PyMongo
•	threading
•	Flask
•	NetworkX
•	iGraph
•	GraphTools

## Other Technologies
•	Amazon S3, EC2
•	CartoDB
•	Javascript, HTML, CSS
•	MongoDB


