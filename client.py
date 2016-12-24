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

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf', conf=dict(keyring='/etc/ceph/ceph.client.admin.keyring'))

# get list of buckets
@app.route('/', methods=['GET'])
def bucket_get():
    pools = cluster.list_pools()
    results = []
    for pool in pools:
        if app.config['BUCKET_PREFIX'] in pool:
            results.append(pool)
    return jsonify({'result': results, 'meta': {'status_code': 200, 'message': 'OK'}})

# create a new bucket
@app.route('/<bucket_name>', methods=['PUT'])
def bucket_create(bucket_name):
    cluster.create_pool(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# delete a specific bucket
@app.route('/<bucket_name>', methods=['DELETE'])
def bucket_delete(bucket_name):
    # TODO: add an exception when there is no such bucket_name
    cluster.delete_pool(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# list all objects in a specific bucket
@app.route('/<bucket_name>', methods=['GET'])
def bucket_list(bucket_name):
    ioctx = cluster.open_ioctx(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    results = []
    # TODO: add an exception when there is no such bucket_name
    while True:
        try:
            rados_object = object_iterator.next()
            results.append(rados_object)
        except StopIteration:
            break
    return jsonify({'result': results, 'meta': {'status_code': 200, 'message': 'OK'}})

cluster.connect()

# Supaya ngga usah pake command `flask run`, tinggal `python client.py` aja.
if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'])
