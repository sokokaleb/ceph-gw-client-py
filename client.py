from flask import Flask
from flask_dotenv import DotEnv
app = Flask(__name__)
env = DotEnv()
env.init_app(app)
env.eval(keys={
    'DEBUG': int
});

# Anthony please continue here below
# Cek konten dari .env, cara pakenya tinggal app.config['KEY']

# Supaya ngga usah pake command `flask run`, tinggal `python client.py` aja.
if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'])
