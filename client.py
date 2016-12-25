from flask import Flask
from flask import jsonify
from flask import make_response
from flask import request
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
        res = f(cluster=cluster, *args, **kwargs)
        cluster.shutdown()
        return res
    return decorated_function

# helper to get prefixed bucket name
def get_bucket_name(bucket_name):
    return '{}-{}'.format(app.config['BUCKET_PREFIX'], bucket_name)

# get list of buckets
@app.route('/', methods=['GET'])
@append_cluster
def bucket_get(cluster):
    pools = cluster.list_pools()
    cluster.shutdown()
    results = []
    for pool in pools:
        if pool.startswith(app.config['BUCKET_PREFIX']):
            results.append(pool[len(app.config['BUCKET_PREFIX']):])
    return jsonify(results), 200

# create a new bucket
@app.route('/<bucket_name>', methods=['PUT'])
@append_cluster
def bucket_create(cluster, bucket_name):
    bucket_name = get_bucket_name(bucket_name)
    if cluster.pool_exists(bucket_name):
        return 'bucket {} exists'.format(bucket_name), 309
    else:
        cluster.create_pool(bucket_name)
        return '', 200

# delete a specific bucket
@app.route('/<bucket_name>', methods=['DELETE'])
@append_cluster
def bucket_delete(cluster, bucket_name):
    bucket_name = get_bucket_name(bucket_name)
    if cluster.pool_exists(bucket_name):
        cluster.delete_pool(bucket_name)
        return 'bucket {} deleted'.format(bucket_name), 200
    else:
        return 'bucket {} not found'.format(bucket_name), 404

# helper function to get an object
def get_object_content(cluster, bucket_name, object_name):
    if cluster.pool_exists(bucket_name):
        try:
            ioctx = cluster.open_ioctx(bucket_name)
            size, _ = ioctx.stat(object_name)
            return ioctx.read(object_name, size)
        except Exception:
            return None
    else:
        return None

# add a new object to an existing cluster
@app.route('/<bucket_name>/<object_name>', methods=['PUT'])
@append_cluster
def object_put(cluster, bucket_name, object_name):
    bucket_name = get_bucket_name(bucket_name)
    if cluster.pool_exists(bucket_name):
        try:
            ioctx = cluster.open_ioctx(bucket_name)
            ioctx.write_full(object_name, request.data)
            ioctx.close()
            return 'created new object {}/{}'.format(bucket_name,  object_name), 200
        except Exception:
            return 'error writing object', 406
    else:
        return 'bucket {} not found'.format(bucket_name), 404

# get the content of an object
@app.route('/<bucket_name>/<object_name>', methods=['GET'])
@append_cluster
def object_get(cluster, bucket_name, object_name):
    bucket_name = get_bucket_name(bucket_name)
    if cluster.pool_exists(bucket_name):
        ioctx = cluster.open_ioctx(bucket_name)
        size, _ = ioctx.stat(object_name)
        result = ioctx.read(object_name, size)
        ioctx.close()
        return result, 200, {'Content-Disposition': 'attachment; filename=%s' % object_name}
    else:
        return 'bucket {} not found'.format(bucket_name), 404

# delete an object from a bucket
@app.route('/<bucket_name>/<object_name>', methods=['DELETE'])
@append_cluster
def object_delete(cluster, bucket_name, object_name):
    bucket_name = get_bucket_name(bucket_name)
    if cluster.pool_exists(bucket_name):
        ioctx = cluster.open_ioctx(bucket_name)
        try:
            ioctx.remove_object(object_name)
            return '', 200
        except Exception:
            return 'object {} not found'.format(object_name), 404
        finally:
            ioctx.close()
    else:
        return 'bucket {} not found'.format(bucket_name), 404

# copy an object
@app.route('/<source_bucket>/<source_object>/<dest_bucket>/<dest_object>', methods=['COPY'])
@append_cluster
def object_copy(cluster, source_bucket, source_object, dest_bucket, dest_object):
    source_bucket = get_bucket_name(source_bucket)
    dest_bucket = get_bucket_name(dest_bucket)
    source_object_ref = get_object_content(cluster, source_bucket, source_object)
    dest_object_ref = get_object_content(cluster, dest_bucket, dest_object)
    if source_object_ref is None:
        return 'source object not found', 404
    if dest_object_ref is not None:
        return 'target exist', 409
    if not cluster.pool_exists(dest_bucket):
        return 'destination bucket not found', 404
    ioctx = cluster.open_ioctx(dest_bucket)
    ioctx.write_full(dest_object, source_object_ref)
    ioctx.close()
    return 'copied', 200

# list all objects in a specific bucket
@app.route('/<bucket_name>', methods=['GET'])
@append_cluster
def bucket_list(cluster, bucket_name):
    bucket_name = get_bucket_name(bucket_name)
    if not cluster.pool_exists(bucket_name):
        return 'bucket {} not found'.format(bucket_name), 404
    ioctx = cluster.open_ioctx(bucket_name)
    object_iterator = ioctx.list_objects()
    results = []
    while True:
        try:
            rados_object = object_iterator.next()
            results.append(rados_object.key)
        except StopIteration:
            break
    ioctx.close()
    return jsonify(results), 200

# Supaya ngga usah pake command `flask run`, tinggal `python client.py` aja.
if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config['PORT'],
            debug=app.config['DEBUG'])
