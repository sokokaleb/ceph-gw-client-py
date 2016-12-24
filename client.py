from flask import Flask
from flask import jsonify
from flask import request
#from flask.ext.api import status
from flask_dotenv import DotEnv
from functools import wraps
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
        cluster = rados.Rados(conffile=app.config['CONFFILE_PATH'], conf=dict(keyring=app.config['KEYRING_PATH']))
        cluster.connect()
        return f(cluster=cluster, *args, **kwargs)
    return decorated_function

# get list of buckets
@app.route('/', methods=['GET'])
@append_cluster
def bucket_get(cluster):
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
def bucket_create(cluster, bucket_name):
    cluster.create_pool(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    cluster.shutdown()
    return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# delete a specific bucket
@app.route('/<bucket_name>', methods=['DELETE'])
@append_cluster
def bucket_delete(cluster, bucket_name):
    bucket_name = app.config['BUCKET_PREFIX'] + '-' + bucket_name
    if cluster.pool_exists(bucket_name):
        cluster.delete_pool(bucket_name)
        cluster.shutdown()
        return jsonify({'status': 'OK'})
    else:
        cluster.shutdown()
        return jsonify({'status': 'file not found'})

#def get_object_ref(cluster, bucket_name, object_name):
#    if cluster.pool_exists(bucket_name):
#        ioctx = cluster.open_ioctx(bucket_name)
#        try:
#            return ioctx.read(object_name)
#        except Error:
#            return None
#    else:
#        return None

# add a new object to an existing cluster
@app.route('/<bucket_name>/<object_name>', methods=['PUT'])
@append_cluster
def object_put(cluster, bucket_name, object_name):
    bucket_name = app.config['BUCKET_PREFIX'] + '-' + bucket_name
    if cluster.pool_exists(bucket_name):
    try:
            ioctx = cluster.open_ioctx(bucket_name)
            ioctx.write_full(object_name, str(request.form['content'])) #'test here') #request.form['content'])
            ioctx.close()
            cluster.shutdown()
            return jsonify({'meta': {'status_code': 200, 'message': 'OK', 'file': request.form['content']}})
        except Error:
            cluster.shutdown()
            return jsonify({'test': request.form['content']})
    else:
        cluster.shutdown()
        return jsonify({'meta': {'status_code': 200, 'message': 'OK'}})

# get the content of an object
@app.route('/<bucket_name>/<object_name>', methods=['GET'])
@append_cluster
def object_get(cluster, bucket_name, object_name):
    bucket_name = app.config['BUCKET_PREFIX'] + '-' + bucket_name
    if cluster.pool_exists(bucket_name):
        ioctx = cluster.open_ioctx(bucket_name)
        result = ioctx.read(object_name)
        ioctx.close()
        cluster.shutdown()
        return jsonify(result)
    else:
        cluster.shutdown()
        return "Resource not available", status.HTTP_404_NOT_FOUND

# delete an object from a bucket
@app.route('/<bucket_name>/<object_name>', methods=['DELETE'])
@append_cluster
def object_delete(cluster, bucket_name, object_name):
    bucket_name = app.config['BUCKET_PREFIX'] + '-' + bucket_name
    if cluster.pool_exists(bucket_name):
        ioctx = cluster.open_ioctx(bucket_name)
        try:
           # return jsonify({'file': object_name})
            ioctx.remove_object(object_name)
            ioctx.close()
            return jsonify({'status': 'OK'})
        except Error:
            ioctx.close()
            return jsonify({'status': 'not available'})
    else:
        return jsonify({'not available (bucket)'})

# list all objects in a specific bucket
@app.route('/<bucket_name>', methods=['GET'])
@append_cluster
def bucket_list(cluster, bucket_name):
    # TODO: add an exception when there is no such bucket_name
    ioctx = cluster.open_ioctx(app.config['BUCKET_PREFIX'] + '-' + bucket_name)
    object_iterator = ioctx.list_objects()
    results = []
    while True:
        try:
            rados_object = object_iterator.next()
            results.append(rados_object.key)
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
