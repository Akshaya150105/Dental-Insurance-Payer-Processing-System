from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class PayerGroup(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)
    payers = db.relationship('Payer', backref='payer_group', lazy=True)

class Payer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    pretty_name = db.Column(db.String(255))
    payer_group_id = db.Column(db.Integer, db.ForeignKey('payer_group.id'), nullable=False)
    payer_details = db.relationship('PayerDetail', backref='payer', lazy=True)

class PayerDetail(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    payer_id = db.Column(db.Integer, db.ForeignKey('payer.id'), nullable=False)
    payer_name_raw = db.Column(db.String(255), nullable=False)
    payer_number = db.Column(db.String(50))