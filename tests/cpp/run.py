"""Local emulator only: synthetic fixtures; SDK CRC stream / Arrow IPC contract probe.
Not a complete SDK HTTP client or ClickHouse E2E replacement.
"""
import json, subprocess, sys, tempfile, urllib.request, urllib.error
from urllib.parse import urlencode
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
endpoint = sys.argv[1].rstrip('/')
def call(path, method='GET', body=None, encoding='identity'):
    req=urllib.request.Request(endpoint+path, data=body, method=method, headers={'Accept-Encoding':encoding})
    with urllib.request.urlopen(req) as r: return r.read()
def sql(text):
    body=f'<Instance><Job><Tasks><SQL><Name>probe</Name><Query>{escape(text)}</Query></SQL></Tasks></Job></Instance>'.encode()
    req=urllib.request.Request(endpoint+'/projects/test_project/instances', data=body, method='POST')
    with urllib.request.urlopen(req) as response:
        location=response.headers['Location']
    result=ET.fromstring(call(location))
    if result.findtext('./Tasks/Task/Status')!='Success':
        raise RuntimeError('fixture SQL failed: '+str(result.findtext('./Tasks/Task/Result')))
for count in (0,8,10007):
    name=f'cpp_contract_{count}'
    values=','.join(f"({i},'value{i}')" for i in range(count))
    sql(f'drop table if exists {name}; create table {name}(id bigint,s string);'+(f"insert into {name} values {values}" if count else ''))
    base=f'/projects/test_project/tables/{name}'
    sess=json.loads(call(base+'?downloads','POST'))
    assert sess['RecordCount']==count, sess
    try:
        for encoding in ('identity','zstd','x-lz4-frame'):
            data=call(base+'?'+urlencode({'downloadid':sess['DownloadID'],'data':'','arrow':'','rowrange':f'(0,{count})'}),encoding=encoding)
            with tempfile.NamedTemporaryFile() as f:
                f.write(data); f.flush()
                result=subprocess.run(['/probe/reader',f.name,str(count)],capture_output=True,text=True)
                if result.returncode: raise RuntimeError(f'{encoding} count={count}: {result.stderr} exit={result.returncode}')
                print(encoding,result.stdout.strip(),flush=True)
    finally:
        call(base+'?'+urlencode({'downloadid':sess['DownloadID']}),'POST')
for path,method,expected in [('/projects/missing/tunnel','GET','NoSuchProject'),('/projects/missing/tables/t?downloads','POST','NoSuchProject'),('/projects/test_project/tables/missing?downloads','POST','NoSuchTable')]:
    try: call(path,method)
    except urllib.error.HTTPError as e:
        assert e.code==404 and expected.encode() in e.read() and e.headers['x-odps-request-id']
        print(expected,'404 request_id=present',flush=True)
    else: raise AssertionError(expected)

sql("drop table if exists cpp_contract_partition; create table cpp_contract_partition(id bigint,s string) partitioned by (ds string); alter table cpp_contract_partition add partition(ds='empty')")
base='/projects/test_project/tables/cpp_contract_partition'
try: call(base+'?'+urlencode({'downloads':'','partition':'ds=missing'}),'POST')
except urllib.error.HTTPError as e:
    assert e.code==404 and b'NoSuchPartition' in e.read() and e.headers['x-odps-request-id']
    print('NoSuchPartition 404 request_id=present',flush=True)
else: raise AssertionError('missing partition succeeded')
sess=json.loads(call(base+'?'+urlencode({'downloads':'','partition':'ds=empty'}),'POST'))
assert sess['RecordCount']==0
call(base+'?'+urlencode({'downloadid':sess['DownloadID'],'partition':'ds=empty'}),'POST')
print('existing empty partition: success rows=0',flush=True)
