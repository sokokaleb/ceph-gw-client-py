from flask import Flask
from flask import jsonify
from flask_dotenv import DotEnv
import rados, sys

app = Flask(__name__)
env = DotEnv()
env.init_app(app)
env.eval(keys={
    'DEBUG': int
});

# Anthony please continue here below
# Cek konten dari .env, cara pakenya tinggal app.config['KEY']

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf', conf=dict(keyring='/etc/ceph/ceph.client.admin.keyring'))

# get list of buckets
@app.route('/', methods=['GET'])
def bucket_get():
    pools = cluster.list_pools()
    results = []
    for pool in pools:
        results.append(pool)
    return jsonify({'result': results, 'meta': {'status_code': 200, 'message': 'OK'}})

# create a new bucket
@app.route('/<bucket_name>', methods=['PUT'])
def bucket_create(bucket_name):
    cluster.create_pool(app.config['BUCKET_PREFIX'] + bucket_name)
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

cluster.connect()

# Supaya ngga usah pake command `flask run`, tinggal `python client.py` aja.
if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'])
