import aerospike
from aerospike import exception as ex
from aerospike import predicates as p
import sys

config = {
    'hosts': [ ('172.17.0.2', 3000) ]
}
 
NAMESPACES = 'test'
SETS = 'custumer1'

_CLIENT = None

def connect_db():
    if _CLIENT is not None and _CLIENT.is_connected():
        return _CLIENT
    
    try:
        CLIENT = aerospike.client(config).connect()
        return CLIENT
    
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

def disconnect_db():
      if _CLIENT is not None and _CLIENT.is_connected():
         _CLIENT.close()


#1 add records 
def add_customer(customer_id, phone, ltv):
    key = (NAMESPACES, SETS, str(customer_id))
    bins = {
        'phone': phone,
        'ltv': ltv,
    }
    try:
        client = connect_db()
        client.put(key, bins, meta={'ttl':60})
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        client.close()
        sys.exit(1)
#2 read id
def get_ltv_by_id(customer_id):
    key = (NAMESPACES, SETS, str(customer_id))

    try:
        client = connect_db()
        (k, m, bins) = client.get(key)
        return bins.get('ltv')
        
    except ex.RecordNotFound:
        return f"Not value 'ltv' for key {customer_id}"

    except Exception as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        client.close()
        sys.exit(1)


#3 read phone 
def get_ltv_by_phone(phone):
    
    try:
        bins = 'phone';
        client = connect_db()
        q = client.query(NAMESPACES, SETS)
        q.select('ltv')
        q.where(p.equals(bins, phone))
        records = q.results( {'total_timeout':2000} )
        if len(records) > 0:
            (k, m, bins) = records[0]
            return bins.get('ltv')
        else:
            return f"not value 'ltv' for {phone}"

    except ex.IndexNotFound:
        # create index for phone
        client.index_string_create(NAMESPACES, SETS, bins, 'index_'+bins)
        return get_ltv_by_phone(phone)

    except Exception as e:
        print(f"Error: {e}")
        client.close()
        sys.exit(1)

#check ALL
_CLIENT = connect_db()

# add records
for id in range(0, 1001):
    add_customer(id, f'+{5000000000 + id}', 100 + id)    
# read to id 
for id in range(0, 1001, 50):
    print(f"ltv value = {get_ltv_by_id(id)} for id = {id}")
# read to phone_number
for id in range(0, 1001, 100):
    phone_number = "+"+str(5000000000 + id + 100)
    print(f"ltv value = {get_ltv_by_phone(phone_number)} for phone = {phone_number}")
# close DB
disconnect_db()
