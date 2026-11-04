import ctypes,glob,os,select,signal,struct,sys,time
root='/home/mo/jxm/logservice-chaos-20261103-d61a7206'
node=3
parent=glob.glob(f'{root}/node-{node}/logservice-data/*/host-*/*/exported-snapshot/shard-1/replica-*')[0]
pid=int(open(f'{root}/node-{node}/pid').read())
if os.path.realpath(f'/proc/{pid}/exe')!=f'{root}/mo-service': raise RuntimeError('not the dedicated test process')
libc=ctypes.CDLL(None,use_errno=True)
fd=libc.inotify_init1(os.O_NONBLOCK)
if fd<0:raise OSError(ctypes.get_errno(),'inotify_init1')
if libc.inotify_add_watch(fd,os.fsencode(parent),0x100|0x80)<0:raise OSError(ctypes.get_errno(),'inotify_add_watch')
print(f'watching={parent} pid={pid}',flush=True)
until=time.monotonic()+90
while time.monotonic()<until:
    if not select.select([fd],[],[],1)[0]:continue
    data=os.read(fd,65536);off=0
    while off<len(data):
        wd,mask,cookie,name_len=struct.unpack_from('iIII',data,off)
        name=data[off+16:off+16+name_len].split(b'\0',1)[0].decode()
        off+=16+name_len
        if name.startswith('snapshot-') and name.endswith('.generating') and mask&0x100:
            print(f'event={name} at={time.time()}',flush=True)
            try:
                os.kill(pid,signal.SIGSTOP)
                print(f'paused={pid} at={time.time()}',flush=True)
                time.sleep(5)
            finally:
                os.kill(pid,signal.SIGCONT)
                print(f'resumed={pid} at={time.time()}',flush=True)
            sys.exit(0)
print('watch_timeout',flush=True)
sys.exit(1)
