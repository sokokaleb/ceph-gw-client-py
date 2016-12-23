from flask import Flask
import rados, sys

app = Flask(__name__)
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf', conf=dict(keyring='/etc/ceph/ceph.client.admin.keyring'))

# get list of buckets
@app.route('/', methods=['GET'])
def bucket_get():
    pools = cluster.list_pools()
    results = ''
    for pool in pools:
        results += pool
    return jsonify('{result: pool, meta: {status_code: 200, message: "OK"}}')

# create a new bucket
@app.route('/<bucket_name>', methods=['PUT'])
def bucket_create(bucket_name):
    cluster.create_pool('CUSTOM_REST_API_BUCKET-' + bucket_name)
    return jsonify('{meta: {status_code: 200, message: "OK"}}')

