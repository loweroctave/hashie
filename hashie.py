# hashie.py
# @author Terry Winters (terry@leapservices.com)
# @internal @created 11-8-2012

class hash(object):
	
	def __init__(self, redis, name=None):
		if redis: self.db = redis
		if name: self.get(name)
		
	def create(self, key, score=None, data=None):
		self.key = key
		if score: self.score = score
		if data: self.data = data
		
	# commit a hash to db
	def commit(self):
		self.db.hmset(self.key, self.data)
	
	# get a hash from the db
	def get(self, key):
		self.key = key
		self.data = self.db.hgetall(key)
		return self.data
	
	# get value for a key
	def val(self, key):
		return self.data.get(key) if self.data.get(key) else None
		
	def delete(self):
		self.db.delete(self.key)
	
class hashset(object):
	
	def __init__(self, redis, name=None):
		if redis: self.db = redis
		if name: self.new(name)
		self.hashes = []
	
	# load a set with range
	def getRange(self, r1, r2):
		self.setData = self.db.zrangebyscore(self.set, r1, r2, withscores=True)
		self.loadHashes()
		return self.setData
	
	# remove range from redis
	def removeRange(self, r1, r2):
		self.removeHashes()
		self.db.zremrangebyscore(self.set, r1, r2)
	
	# getRange and delete from redis
	def clearRange(self, r1, r2):
		self.getRange(r1, r2)
		self.removeRange(r1, r2)
		return self.setData
	
	# add a hash to set/redis
	def add(self, hash):
		self.hashes.append(hash)
		hash.commit()
		self.db.zadd(self.set, hash.key, hash.score)
	
	def removeHashes(self):
		for h in self.hashes:
			h.delete()
		
	def loadHashes(self):
		for i in self.setData:
			h = hash(self.db, i[0])
			self.hashes.append(h)
		return len(self.hashes)
	
	# start a new set
	def new(self, setName):
		if setName: self.set = setName
	
	# return the hashes
	def getHashes(self):
		return self.hashes if self.hashes else []
		
	def getHash(self, index):
		return self.hashes[index] if self.hashes[index] else False
		
	def getScore(self, name):
		return self.db.zscore(self.set, name)
