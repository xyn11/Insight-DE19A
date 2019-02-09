from flask import Flask, request
from flask import render_template
from flask_sqlalchemy import SQLAlchemy
import json

app = Flask(__name__)
conf = get_config()
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://'+conf["postgres"]["user"]+':' \
    +conf["postgres"]["password"] + '@' + conf["flask"]["ip"] + ':5432/' + conf["postgres"]["user"]

db = SQLAlchemy(app)

class Fake(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    addr = db.Column(db.String(255), nullable=False)
    
    def __repr__(self):
        return f'<Fake name: {self.name} addr: {self.addr}>'

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/search', methods=['POST'])
def search():
    city = request.form["city"]
    fakes = Fake.query.all()
    return render_template('search.html', city=city, fakes=fakes)