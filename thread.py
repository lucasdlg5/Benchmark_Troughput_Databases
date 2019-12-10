import cx_Oracle
import threading
from datetime import datetime
import os
import pandas


update_amount = 500 ############################################################################## Quantidade de Updates

choose = 1 ############################################################################## Tipo de Commit
thread_amount = [1,5,10,20]
folder_name = ['read_commited', 'serializable']
exec_type = ['SET TRANSACTION ISOLATION LEVEL  READ COMMITTED','SET TRANSACTION ISOLATION LEVEL SERIALIZABLE']
thread_list_index = 0

def set_log(thread_index):
  now = datetime.now()
  # file = open('thread_'+str(actual_index)+'\\thread_'+str(actual_index)+'log_'+str(thread_index)+'.txt', 'a')
  # file.write('thread '+str(thread_index)+' | '+now.strftime('%d/%m/%Y %H:%M:%S') + '\n')
  file = open(folder_name[choose]+'\\thread_'+str(atual_amount)+'\\thread_'+str(atual_amount)+'.csv', 'a')
  file.write(''+now.strftime('%H:%M:%S') + ';\n')
  
  file.close()

def update_cust_credit(thread_list_index):
  i = 0
  while (i < update_amount):
    
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orclfatec')
    conn = cx_Oracle.connect(user=r'system', password='fatec', dsn=dsn_tns)
    cur = conn.cursor()
    cur.execute(exec_type[choose])
    # cur.execute('set transaction isolation level serializable')
    stt = 'update sh.customers set cust_credit_limit = cust_credit_limit - :1 where customers.cust_id between :2 and :3 and cust_credit_limit > 100'
    cur.execute(stt, (100, 1, 10))
    conn.commit()
    cur.close()
    conn.close()
    set_log(thread_list_index)
    print(str(i) + ' row updated')
    i=i+1

# os.remove('thread_'+str(thread_amount)+'\\thread_'+str(thread_amount)+'.txt')
# print('thread_'+str(thread_amount)+'.txt removed.')


atual_amount = thread_amount[3] ############################################################################## Quantidade de Threads

thread_list = [None] * atual_amount
for i in range(atual_amount):
  thread_list[thread_list_index] = threading.Thread(target=update_cust_credit,args=(thread_list_index,))
  thread_list[thread_list_index].start()
  print('thread '+str(thread_list_index)+' is running.')
  thread_list_index = thread_list_index + 1