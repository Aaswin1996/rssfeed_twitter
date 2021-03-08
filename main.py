import feedparser,time
import pymongo
import tweepy
import hashlib
from datetime import datetime 

access_token = "1039585356931375117-yH4bsuc0QwSMiygKjCTYd6AbIvFsaO"
access_secret = "lQpTf3CMe9HweJdD5IufbxJu9MpIVznFf88sWNO9e5EEH"
api_key = "CYqwlNkOJDbdfMjbaGoPRM2lT"
api_secret = "clGp6aQ1DIyOiV5U2R3rR11HHEs2Twj85Rzr426SIevS95cgrz"

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["rssfeeds"]
custom_url = "https://cointelegraph.com/rss"

mycol = mydb[custom_url]
mycol.delete_many({})

auth = tweepy.OAuthHandler(api_key,api_secret)
auth.set_access_token(access_token,access_secret)

api = tweepy.API(auth)
try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")
    
def username_to_userids(usernames,api):
    userids=[]
    for username in usernames:
        x = api.get_user(username)
        userids.append(int(x.id_str))
    return userids

userids = username_to_userids(["SinhaAaswin","mi8ltd"],api)
while True:
    time.sleep(30)
    if len(list(mycol.find())) == 0:
        to_be_hash_st = ""
        feed = feedparser.parse(custom_url)
        for i in range(len(feed["entries"])):         
            to_be_hash_st+= feed["entries"][i]["title"]+"@@"+feed["entries"][i]["links"][0]["href"]+"@@//" 
        print(to_be_hash_st)
        hash_val = hashlib.md5(to_be_hash_st.encode()).hexdigest()
        mycol.insert_one({"Hash":hash_val,"URL":custom_url})
        print("Initial Hash inserted for ",custom_url)
        print(datetime.now())
#         print(list(mycol.find())[0]["Hash"])
    else :
        to_be_hash_str = ""
        feed = feedparser.parse(custom_url)
        for i in range(len(feed["entries"])):         
            to_be_hash_str+= feed["entries"][i]["title"]+"@@"+feed["entries"][i]["links"][0]["href"]+"@@//"
        
        hash_val = hashlib.md5(to_be_hash_str.encode()).hexdigest()
#         print(hash_val,list(mycol.find())[0]["Hash"])
        if list(mycol.find())[0]["Hash"]!=hash_val:
            print(to_be_hash_str,to_be_hash_st)
            print("Update in Rss feed hence message needs to be sent")
            for userid in userids:
                time.sleep(2)
                api.send_direct_message(userid,"Title: "+ to_be_hash_str.split("@@//")[0:1][0].split("@@")[0] +"\n"
                                                +"Link: "+ to_be_hash_str.split("@@//")[0:1][0].split("@@")[1])
                
            mycol.update_one({
              'URL': custom_url
            },{
              '$set': {
                "Hash":hash_val
              }
            }, upsert=False)

            print(datetime.now())
            print()
        else:
            print("No Update in rss feed")
            print(datetime.now())
            print()
    
    
    
    