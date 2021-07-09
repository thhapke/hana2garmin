

from flask import Flask, render_template, flash, redirect
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, FileField, SelectMultipleField
from wtforms.validators import DataRequired
from flask_wtf.file import FileField, FileRequired, FileAllowed
from wtforms.widgets import PasswordInput
import yaml

from parsefit import fitfile


with open('config.yaml') as yamls:
    params = yaml.safe_load(yamls)

db = {'host': params['HDB_HOST'],
      'user': params['HDB_USER'],
      'pwd': params['HDB_PWD'],
      'port': params['HDB_PORT'],
      'schema' : params['SCHEMA'] }

athlete = {'user': params['appuser'],'pwd': params['apppwd']}


app = Flask(__name__)
bootstrap = Bootstrap(app)
app.config['SECRET_KEY'] = 'Di4DdAthlete2021'

class uploadForm(FlaskForm):
    user = StringField('User: ', validators=[DataRequired()])
    pwd = StringField('Password: ', widget=PasswordInput(hide_value=False), validators=[DataRequired()])
    sport = SelectMultipleField('Sport',choices=[('cycling_indoor','Cycling Indoor'),
                                                 ('cycling_outdoor','Cycling Outdoor'),
                                                 ('running','Running'),
                                                 ('swimming_pool', 'Swimming Pool'),
                                                 ('swimming_open_water','Swimming Open Water')],validators=[DataRequired()])
    fitfile = FileField('Document', validators=[FileRequired(),FileAllowed(['zip','fit','gz'], 'zip and fit only!')])
    submit = SubmitField('Submit')

@app.route('/', methods = ['GET','POST'])
def index():

    user = 'JanFrodeno'
    pwd = 'ddAthlete4Di'

    uploadform = uploadForm(user = user, pwd = pwd)
    validate_value = uploadform.validate_on_submit()
    if uploadform.validate_on_submit() :
        user = uploadform.user.data
        pwd = uploadform.pwd.data

        if not (athlete['user'] == user and athlete['pwd'] == pwd ) :
            flash('Wrong user credentials!','warning')
            render_template('upload.html', form=uploadform)
        else :
            sports = uploadform.sport.data
            fitfile(uploadform.fitfile.data,sports,db)
            #except Exception as e :
            #    flash('Exception: {}'.format(e),'warning')
             #   render_template('upload.html', form=uploadform)

            flash('Data uploaded successfully!','success')
            render_template('upload.html', form=uploadform)

    return render_template('upload.html', form=uploadform)

if __name__ == '__main__':
    app.run('0.0.0.0',port=8080)
