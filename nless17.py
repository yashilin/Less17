import logging
import aerospike
import sys
from aerospike import exception as ex
from aerospike import predicates as p


PHONE_NUM = 'phone_number'
LIFETIME_VAL = 'lifetime_value'

# Configure the client
config = {
  'hosts': [ ('127.0.0.1', 3000) ]
}

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

try:
    client.index_string_create("test", "customer_new", "phone", "idx_phone2")
except ex.IndexFoundError as e:
    logging.error('Error: {0} [{1}]'.format(e.msg, e.code))
except ex.AerospikeError as e:
    logging.error('Failed to connect to the cluster with', config['hosts'])
    sys.exit(1)

def add_customer(customer_id, phone_number, lifetime_value):
   key = ("test", "customer_new3", customer_id)

  # Records are addressable via a tuple of (namespace, set, key)
  
   client.put(key,{'phone': str(phone_number), 'ltv': lifetime_value})
  

def get_ltv_by_id(customer_id):
    key = ("test", "customer_new3", customer_id)
    try:
       (key, metadata, item) = client.get(key)
       return item.get('ltv')

    except ex.RecordNotFound:
        return logging.error('Requested non-existent customer ' + str(customer_id))
      
    except Exception as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        client.close()
        sys.exit(1)


def get_ltv_by_phone(phone_number):
    try:
        bins = 'phone_number';
        query = client.query("test", "customer_new3")
        query.select(PHONE_NUM, LIFETIME_VAL)
        query.where(p.equals(PHONE_NUM, phone_number))
        records = query.results({'total_timeout': 2000})
        if len(records) > 0:
                (key, metadata, bins) = records[0]
                return bins.get('lifetime_value')
        else:
                return f"Not value 'lifetime_value' for fild {phone_number}"
    except ex.IndexNotFound:

        client.index_string_create("test", "customer_new3", bins, 'inx'+bins)
        return get_ltv_by_phone(phone_number)


    except Exception as e:
        print(f"Error: {e}")
        client.close()
        sys.exit(1)
# Run for check
for i in range(0, 10):
    add_customer(i, i, i + 1)

for i in range(0, 10):
    assert (i + 1 == get_ltv_by_id(i)), 'No LTV by ID ' + str(i)
    assert (i + 1 == get_ltv_by_phone(i)), 'No LTV by phone ' + str(i)

if client is not None:
    client.close()
# Close the connection to the Aerospike cluster

        