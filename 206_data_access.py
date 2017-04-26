import unittest
import itertools
import collections
import tweepy
import twitter_info # same deal as always...
import json
import sqlite3
import requests

## Your name: Dong-Lien Lin
## The names of anyone you worked with on this project:



##### CACHE SETUP CODE:
CACHE_FNAME = "SI206_FP_CACHE.json"
try:
	cache_file= open(CACHE_FNAME, 'r')
	cache_contents= cache_file.read()
	CACHE_DICTION= json.loads(cache_contents)
	cache_file.close()
except:
	CACHE_DICTION= {}

##### TWEEPY SETUP CODE:
# Authentication information should be in a twitter_info file...
consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# Set up library to grab stuff from twitter with your authentication, and return it in a JSON format 
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

##### DB SETUP CODE:
conn = sqlite3.connect('si206fp.db')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS Company')
cur.execute("CREATE TABLE Company(CompanyName TEXT PRIMARY KEY, Movielist  TEXT, Actors TEXT, numMovies INTEGER, numActors INTEGER)")

cur.execute('DROP TABLE IF EXISTS Movies')
cur.execute("CREATE TABLE Movies(Title TEXT PRIMARY KEY, actors TEXT, year TEXT, language TEXT, ratings INTEGER, score INTEGER)")

cur.execute('DROP TABLE IF EXISTS ActorsTweet')
cur.execute("CREATE TABLE ActorsTweet(user_id INT , screen_name TEXT, num_favs INTEGER, description TEXT)")

##### CLASS, FUNCTION SETUP CODE:

class ProductionCompany(): 
	def __init__(self, omdbresponse):
		self.CompanyName= omdbresponse["Production"]
		self.Movies= {omdbresponse["Title"]: {"Year":omdbresponse["Year"], "Actors": omdbresponse["Actors"].split(",")}}  # If got time, would make Movies a class itself
		self.Actorslist= [omdbresponse["Actors"].split(",")]
		self.Websiteurl= [omdbresponse["Website"]]
	def NumActorsCooperated(self):
		return len(self.Actorslist)
	def NumMovies(self):
		return len(self.Movies.keys())
	def AddtoActorslist(self, newactorlist):
		for newactor in newactorlist:
			if newactor not in self.Actorslist:
				self.Actorslist.extend(newactor)
	def AddtoMovies(self, newmovie): # every item in the newmovieslist should be a tuple (year, title, actors)
		if newmovie[1] not in self.Movies:
			self.Movies[newmovie[1]]= {"Year": newmovie[0], "Actors": newmovie[2]}
class MoviesInstance():  #Though the class might be somewhat confusing, it helps clearing my mind when coding.
	def __init__(self, omdbresponse):
		self.title= omdbresponse["Title"]
		self.year= omdbresponse["Year"]
		self.actors= omdbresponse["Actors"].split(",")
		self.mrs= omdbresponse["Rated"]
		self.runtime= int(omdbresponse["Runtime"].split()[0])
		self.genre= omdbresponse["Genre"].split()
		self.director= omdbresponse["Director"]
		self.language= omdbresponse["Language"]
		self.country= omdbresponse["Country"]
		try:
			self.score= int(omdbresponse["Metascore"]) 
		except: 
			self.score= 0
		try:
			self.imdbRating= float(omdbresponse["imdbRating"]) 
		except: 
			self.imdbRating= 0

class CompanyEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, ProductionCompany):
			return [obj.Movies, obj.Actorslist]
		#Let the base class default method raise the TypeError
		return json.JSONEncoder.default(self, obj)
class MoviesEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, MoviesInstance):
			return [obj.title, obj.year, obj.actors, obj.mrs, obj.runtime, obj.genre, obj.director, obj.language, obj.country, obj.score, obj.imdbRating]
		#Let the base class default method raise the TypeError
		return json.JSONEncoder.default(self, obj)


#####
def lsttostr(lst):
	"""
		change a list into a string that concatenates all elements # prob useful in db load functions
	"""
	string= ""
	for every in lst:
		string+=every
	return string

def makevalidtweetname(x):
	return x.replace(" ", "")

def replacespacewithplus(x): 
	''' transform a space-connected string into a '+' connected string just to make easy searching movies. Assume string is valid'''
	splitted= x.split()
	newstr= splitted[0]
	index= 1
	while (index< len(splitted)):
		newstr+= "+"
		newstr+= splitted[index]
		index+=1
	return newstr

def cacheomdbresponse(companylist, movielist, diction):  # diction should be a cache dictionary
	''' extract useful data from a omdb response, the output being aggregate into the diction (key is the productioncompany)
	    should check if company is searched before and customize upon that.'''
	for every_movie in movielist:
		if every_movie.title not in diction:
			diction[every_movie.title]= json.dumps(every_movie, cls= MoviesEncoder)	
	for every_company in companylist:
		diction[every_company.CompanyName]= json.dumps(every_company, cls= CompanyEncoder)
	return 
#cur.execute("INSERT OR IGNORE INTO Tweets (tweet_id, text, user_id, time_posted, retweets) VALUES (?, ?, ?, ?, ?)", (every_tweet['id'], every_tweet["text"], every_tweet["user"]["id"], every_tweet["created_at"], every_tweet["retweet_count"]))

def dbloadcompany(companylist, dbcursor): #later change the company into the companylist
	for comps in companylist:
		statement= "DELETE FROM Company WHERE CompanyName = ?"
		dbcursor.execute(statement, (comps.CompanyName,))
		productionlist= list(comps.Movies.keys())
		statement= "INSERT INTO Company(CompanyName , Movielist , Actors, numMovies, numActors) VALUES(?, ?, ?, ?, ?)"
		dbcursor.execute(statement, (comps.CompanyName, str(productionlist), str(comps.Actorslist), comps.NumMovies(), comps.NumActorsCooperated()))

def dbloadmovies(movielist, dbcursor):
	for movs in movielist:
		statement= "INSERT INTO Movies(Title, actors, year, language, ratings, score) VALUES(?, ?, ?, ?, ?, ?)"
		dbcursor.execute(statement, (movs.title, str(movs.actors), movs.year, movs.language, movs.imdbRating, movs.score))

def dbloadactortweetresponse(complist, movlist, dbcursor, diction):
	for every_movie in movlist:
		for every_actor in every_movie.actors:
			#print (every_actor.replace(" ",""))
			if every_actor.replace(" ","") not in diction:
				# try:
				result= api.get_user(every_actor.replace(" ",""))
				statement= "INSERT INTO ActorsTweet(user_id, screen_name, num_favs , description) VALUES(?, ?, ?, ?)"
				dbcursor.execute(statement, (result['id'], result["screen_name"], result['favourites_count'], result['description']))
				diction[every_actor.replace(" ","")]= [result["id"], result["screen_name"], result['favourites_count'], result['description']]
				# print (every_actor)
				# print ("yep")
				# except:
				# 	something= "a" # just dont want to use pass
	for every_comp in complist:
		try:
			result= api.get_user(every_comp.CompanyName.replace(" ",""))
			statement= "INSERT INTO ActorsTweet(user_id, screen_name, num_favs , description) VALUES(?, ?, ?, ?)"
			dbcursor.execute(statement, (result['id'], result["screen_name"], result['favourites_count'], result['description']))
			diction[every_actor.replace(" ","")]= [result["id"], result["screen_name"], result['favourites_count'], result['description']]
		except :
			pass 
		
				


def interactive_data_access(complist, movlist, diction):
	"""
		The function would first access data from all the movies that the user want to search, and then use the data to contruct the db
		And we go from there
	"""
	print ("Please enter a VALID and NON-REPETITIVE movie title (space is acceptable, but no leading and trailing space please)")
	print ("The program does NOT equip with an error handling mechanism, so please reopen it when crashes")
	print ("Enter 'quit' to quit the program")
	
	base_url= "http://www.omdbapi.com/?t="
	userinput_movie= input()
	while(userinput_movie!= "quit"):
		userinput_movie= replacespacewithplus(userinput_movie)
		url= base_url+ userinput_movie
		response= requests.get(url)
		kk= json.loads(response.text)
		movlist.append(MoviesInstance(kk))
		for x in complist:
			if x.CompanyName== kk["Production"]:
				#print ("asdfasdf")
				x.AddtoMovies([kk["Year"], kk["Title"], kk["Actors"].split(",")])
		else:
			complist.append(ProductionCompany(kk))
		cacheomdbresponse(complist, movlist, diction)
		print ("Done processing data, please enter a VALID and NON-REPETITIVE movie title to continue scraping data")
		print ("The program does NOT equip with an error handling mechanism, so please reopen it when crashes")
		print ("Enter 'quit' to quit the program, as usual :D")
		userinput_movie= input()


################ END OF SETUP CODE
################ BEGIN IMPL CODE
companysearched= []
moviessearched= []
actorssearched= []
interactive_data_access(companysearched, moviessearched, CACHE_DICTION)
dbloadactortweetresponse(companysearched, moviessearched, cur, CACHE_DICTION)
dbloadcompany(companysearched, cur)
dbloadmovies(moviessearched, cur)

OUTPUT_FILE= "SI206FP_out.txt"
hasoldmovie=""
b= cur.execute("SELECT Company.CompanyName, Movies.Title FROM Company INNER JOIN Movies WHERE Movies.year < 2000")
hasoldmovie+=b.fetchone()[0]
hasoldstatement= "Production Company: "+ hasoldmovie+ "has produced a movie before year 2000"
output= open(OUTPUT_FILE, 'w')
output.write(str(hasoldstatement))
output.write("\n")
high_rating_movies=[x for x in cur.execute("SELECT  Movies.Title FROM Movies  WHERE ratings > 7")]
high_rating_statement= str(high_rating_movies), " has high ratings"
output.write(str(high_rating_statement))
output.write("\n")
b= cur.execute("SELECT * FROM Movies WHERE Movies.score > 60").fetchone()
moviedict= {b[0]: b[1:]}
output.write("Movies that have a ratings higher thatn 60: ")
output.write(json.dumps(moviedict))
output.write("\n")
b= cur.execute("SELECT * FROM Movies").fetchall()
moviedictha= {x[0]: x[1:] for x in b}
output.write("Movies that have been searched: ")
output.write(json.dumps(moviedictha))
output.write("\n")







#dbloaddata(companysearched, moviessearched, cur)

# for x in list(CACHE_DICTION.values()): # every x should be in a ProductionCompany instance
# 	dbcompanyloadresponse(x, cur)

print (CACHE_DICTION)


##### CACHE AND DB CLOSING
output.close()
cache_file= open(CACHE_FNAME, 'w')
cache_file.write(json.dumps(CACHE_DICTION))
cache_file.close()
conn.commit()
cur.close()
##### END CACHING AND DISCONNECTIN DB

###################################### 
###################################### TEST CASE STARTS HERE
class CompanyTest(unittest.TestCase):
	def test_name(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		self.assertEqual(type(a.CompanyName),type("string"))
	def test_movies(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		self.assertEqual(type(list(a.Movies.values())[0]),type({}))
	# def test_movie_instance_1(self):
	# 	self.assertEqual(type(a.Movies[0]),type(Movie()))
	def test_movie_instance_2(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		self.assertEqual(type(list(a.Movies.keys())[0]),type("string"))
	def test_actor_list(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		self.assertEqual(type(a.Actorslist),type([1,2,3]))
	def test_actor_instance(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		self.assertEqual(type(a.Actorslist[0][0]), type("string"))
	def test_num_actors(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		the_num= a.NumActorsCooperated()
		self.assertEqual(type(thenum), int)
	def test_num_actors(self):
		x= "star wars"
		x= replacespacewithplus(x)
		base_url= "http://www.omdbapi.com/?t="
		url= base_url+ x
		response= requests.get(url)
		kk= json.loads(response.text)
		a= ProductionCompany(kk)
		the_num= a.NumMovies()
		self.assertEqual(type(the_num), int)


## Remember to invoke all your tests...
if __name__ == "__main__":
	unittest.main(verbosity =2)