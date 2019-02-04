from collections import Counter

def get_categories_tags(c):
    '''
    get tags from categories
    '''
    c = yelp_business_f.select("categories").collect()
    tags = []
    for i in range(len(c)):
        try:
            t = c[i][0].split(",")
        except:
            continue
        for x in t:
            tags.append(x.encode('utf-8'))
    return tags

def top_common_tags(tags):
    '''
    get top 5 common tags in catogories tags
    '''
    tags = get_categories_tags(c)
    common = Counter(tags).most_common(1)
    if len(tags) < 5:
        common = Counter(tags).most_common(len(tags))
    else:
        common = Counter(tags).most_common(5)
    common_tag = []
    for x in common:
        common_tag.append(x[0])
    return common_tag

c = [['u'Tours', u' Breweries', u' Pizza', u' Restaurants', u' Food', u' Hotels & Travel'',0],None]
print(get_categories_tags(c))
