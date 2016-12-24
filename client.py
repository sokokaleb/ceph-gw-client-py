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

def append_cluster(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        cluster = rados.Rados(conffile=app.config['CONF_FILE'], conf=dict(keyring=app.config['KEYRING']))
        cluster.connect()
        return f(cluster=cluster, *args, **kwargs)
    return decorated_function

# get list of buckets
@app.route('/', methods=['GET'])
@append_cluster
def bucket_get():
    pools = cluster.list_pools()
    cluster.shutdown()
    results = []
    for pool in pools:
        if app.config['BUCKET_PREFIX'] in pool:
            results.append(pool)
    return jsonify({'result': results, 'meta': {'status_code': 200, 'message': 'OK'}})

# create a new bucket
@app.route('/<bucket_name>', methods=['PUT'])
@append_cluster
def bucket_create(bucket_name):
    cluster.create_pool(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    cluster.shutdown()
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# delete a specific bucket
@app.route('/<bucket_name>', methods=['DELETE'])
@append_cluster
def bucket_delete(bucket_name):
    # TODO: add an exception when there is no such bucket_name
    cluster.delete_pool(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    cluster.shutdown()
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# add a new object to an existing cluster
@app.route('/<bucket_name>/<object_name>', methods=['PUT'])
@append_cluster
def object_put(bucket_name, object_name):
    # TODO: add an exception when there is no such bucket_name
    ioctx = cluster.open_ioctx(bucket_name)
    # TODO: change to value in request payload
    ioctx.write(object_name, 'temporary value')
    ioctx.close()
    cluster.shutdown()
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# list all objects in a specific bucket
@app.route('/<bucket_name>', methods=['GET'])
@append_cluster
def bucket_list(bucket_name):
    # TODO: add an exception when there is no such bucket_name
    ioctx = cluster.open_ioctx(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    object_iterator = ioctx.list_objects()
    results = []
    while True:
        try:
            rados_object = object_iterator.next()
            results.append(rados_object)
        except StopIteration:
            break
    ioctx.close()
    cluster.shutdown()
    return jsonify({'result': results, 'meta': {'status_code': 200, 'message': 'OK'}})

# Supaya ngga usah pake command `flask run`, tinggal `python client.py` aja.
if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'])
