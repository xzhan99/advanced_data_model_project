import pymongo

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.assignment1

# Tags改成数组
# all_post = db.posts.find()
# for post in all_post:
#     condition = {'_id': post['_id']}
#     tags = post['Tags']
#     tags = [] if tags == '' else tags.replace('\"', '').split(',')
#     post['Tags'] = tags
#     db.posts.update(condition, post)

# posts里每个question加一个属性记录所有answer的id
# all_post = db.posts.aggregate([
#     {'$lookup':{
#         'from': "posts",
#         'localField': "Id",
#         'foreignField': "ParentId",
#         'as': "answers"}},
#     ])
# for post in all_post:
#     ids = [i['Id'] for i in post['answers']]
#     if len(ids) > 0:
#         condition = {'_id': post['_id']}
#         post['AnswerId'] = [i['Id'] for i in post['answers']]
#         del post['answers']
#         db.posts.update(condition, post)

# 给PostTypeId=2的post加上Tags
# answers = db.posts.aggregate([
#     {'$match': {'PostTypeId': 2}},
#     {'$lookup': {
#         'from': "posts",
#         'localField': "ParentId",
#         'foreignField': "Id",
#         'as': "Parent"}}
# ])
# for post in answers:
#     condition = {'_id': post['_id']}
#     post['Tags'] = post['Parent'][0]['Tags']
#     del post['Parent']
#     db.posts.update(condition, post)

# 给PostTypeId=2的post加上Title
# answers = db.posts.aggregate([
#     {'$match': {'PostTypeId': 2}},
#     {'$lookup': {
#         'from': "posts",
#         'localField': "ParentId",
#         'foreignField': "Id",
#         'as': "Parent"}}
# ])
# for post in answers:
#     condition = {'_id': post['_id']}
#     post['Title'] = post['Parent'][0]['Title']
#     del post['Parent']
#     db.posts.update(condition, post)

# 删除所有AcceptedAnswerId=""
posts = db.posts.find({'AcceptedAnswerId': ""})
for post in posts:
    condition = {'_id': post['_id']}
    del post['AcceptedAnswerId']
    db.posts.update(condition, post)


client.close()
