from flask import Flask, request
from flask import render_template
from flask_sqlalchemy import SQLAlchemy
import json

def get_config():
    with open("../config.json", "r") as f:
        jsonstr = f.read()
        conf = json.loads(jsonstr)
    return conf

conf = get_config()
env = "development"

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = conf["postgres"][env]["url"]
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

db = SQLAlchemy(app)

class Abb(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    price = db.Column(db.Integer)
    number_of_reviews = db.Column(db.Integer)
    name = db.Column(db.String(1023))
    city = db.Column(db.String(63))
    restaurants = db.relationship('AbbRestaurants', backref='abb', lazy=True)
    shopping = db.relationship('AbbShopping', backref='abb', lazy=True)
    pubs = db.relationship('AbbPubs', backref='abb', lazy=True)
    chinese = db.relationship('AbbChinese', backref='abb', lazy=True)
    italian = db.relationship('AbbItalian', backref='abb', lazy=True)


class AbbRestaurants(db.Model):
    abb_id = db.Column(db.Integer,
                       db.ForeignKey('abb.id'),
                       nullable=False,
                       primary_key=True)

    score = db.Column(db.Integer, nullable=False)

class AbbShopping(db.Model):
    abb_id = db.Column(db.Integer,
                       db.ForeignKey('abb.id'),
                       nullable=False,
                       primary_key=True)

    score = db.Column(db.Integer, nullable=False)

class AbbPubs(db.Model):
    abb_id = db.Column(db.Integer,
                       db.ForeignKey('abb.id'),
                       nullable=False,
                       primary_key=True)

    score = db.Column(db.Integer, nullable=False)

class AbbChinese(db.Model):
    abb_id = db.Column(db.Integer,
                       db.ForeignKey('abb.id'),
                       nullable=False,
                       primary_key=True)

    score = db.Column(db.Integer, nullable=False)

class AbbItalian(db.Model):
    abb_id = db.Column(db.Integer,
                       db.ForeignKey('abb.id'),
                       nullable=False,
                       primary_key=True)

    score = db.Column(db.Integer, nullable=False)

cities = ["Toronto", "Montreal", "Austin", "Oakland", "Boston", "Sevilla"]
categories = ["Restaurants", "Shopping", "Pubs", "Chinese", "Italian"]

@app.route('/')
def home():
    return render_template('home.html', cities=cities, categories=categories)

category2table = {
    "Restaurants": AbbRestaurants,
    "Shopping": AbbShopping,
    "Pubs": AbbPubs,
    "Chinese": AbbChinese,
    "Italian": AbbItalian,
}

@app.route('/search')
def search():
    city = request.args.get("city")
    category = request.args.get("category")
    score_table = category2table[category]
    listings = db.session.query(score_table)\
                         .join(Abb)\
                         .order_by(score_table.score.desc())\
                         .filter(Abb.city == city)\
                         .limit(5)\
                         .all()
    return render_template('search.html', listings=listings, category=category)
